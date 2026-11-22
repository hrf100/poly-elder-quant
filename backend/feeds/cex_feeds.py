import asyncio
import time
from collections import deque
from statistics import median
from typing import Callable
import websockets

from backend.fast_json import loads as fast_loads


def _fmt_feed_error(exc: Exception) -> str:
    """Convert raw WebSocket/network exceptions into concise Chinese fault diagnostics."""
    if isinstance(exc, asyncio.TimeoutError):
        return "15s无新数据超时，重连中"
    msg = str(exc) or ""
    low = msg.lower()
    if "451" in msg:
        return "HTTP 451 地区限制"
    if "403" in msg:
        return "HTTP 403 拒绝访问"
    if "404" in msg:
        return "HTTP 404 接口路径失效"
    if "429" in msg:
        return "HTTP 429 请求限流"
    if isinstance(exc, websockets.exceptions.ConnectionClosed):
        code = getattr(exc, "code", None)
        return f"连接断开({code})" if code else "连接已断开"
    if "gaierror" in low or "name or service not known" in low or "nodename" in low or "getaddrinfo" in low:
        return "DNS解析失败"
    if "ssl" in low or "certificate" in low:
        return "TLS握手失败"
    if "timed out" in low or "timeout" in low:
        return "连接握手超时"
    if "refused" in low or "reset" in low:
        return "连接被重置/拒绝"
    brief = msg.strip().replace("\n", " ")[:48]
    return f"{type(exc).__name__}: {brief}" if brief else type(exc).__name__


class CexPriceFeed:
    """
    v29 Low-CPU High-Precision CEX Feed:
    1. Binance Spot: 100ms-paced `btcusdt@depth5@100ms` (bid-ask-bounce-free mid price at <=10 Hz)
       combined with `btcusdt@miniTicker` (1 Hz exact latency timestamp) -> ~98% less WS/TLS/JSON CPU than raw aggTrade!
    2. Binance USDT-M Perpetual Futures: 2026 `/market/ws/btcusdt@aggTrade` accumulated into an O(1)
       128-slot 100ms ring buffer (zero per-trade tuple/dict allocations, zero pruning loops).
    3. Both streams use an external 3s watchdog task instead of per-frame `asyncio.wait_for()`,
       eliminating thousands of asyncio timer handles per minute.
    """

    WS_IDLE_TIMEOUT_SEC = 15.0
    _RING_SIZE = 128
    _RING_MASK = 127

    def __init__(self, on_price: Callable[[str, float, float], None]):
        self.on_price = on_price
        self.status = {
            "binance": {"connected": False, "last_price": 0.0, "latency_ms": 0.0, "last_msg_ts": 0.0, "errors": 0, "last_error": ""},
            "binance_perp": {
                "connected": False,
                "last_price": 0.0,
                "latency_ms": 0.0,
                "last_msg_ts": 0.0,
                "errors": 0,
                "last_error": "",
                "perp_stale": True,
                "cvd_3s_imbalance": 0.0,
                "cvd_10s_imbalance": 0.0,
                "cvd_3s_usd": 0.0,
                "perp_basis_jump_bps": 0.0,
                "vol_3s_baseline_usd": 60000.0,
                "adaptive_cvd_3s_min_usd": 65000.0,
                "vol_3s_rel": 1.0,
            },
        }
        # Hot scalar fields (updated in-place without dict allocations)
        self._spot_price: float = 0.0
        self._spot_msg_ts: float = 0.0
        self._spot_latency_ms: float = 0.0
        self._perp_price: float = 0.0
        self._perp_msg_ts: float = 0.0
        self._perp_latency_ms: float = 0.0

        # O(1) 100ms ring buffer covering 12.8s of perp aggTrade volume
        self._ring_bkt: list[int] = [0] * self._RING_SIZE
        self._ring_signed: list[float] = [0.0] * self._RING_SIZE
        self._ring_vol: list[float] = [0.0] * self._RING_SIZE

        # Rolling 10-minute (600 x 1s) volume baselines with 5s cached median
        self._vol_3s_hist: deque[float] = deque(maxlen=600)
        self._vol_10s_hist: deque[float] = deque(maxlen=600)
        self._last_vol_sample_ts: float = 0.0
        self._last_base_calc_ts: float = 0.0
        self._cached_vol_3s_base: float = 60000.0
        self._cached_vol_10s_base: float = 180000.0
        self._perp_basis_ema_bps: float = 0.0
        self._perp_basis_primed: bool = False
        self._last_basis_upd_ts: float = 0.0
        self._stop = False

    def stop(self):
        self._stop = True

    def get_best_spot(self) -> float:
        return self._spot_price if self._spot_price > 0.0 else float(self.status["binance"]["last_price"] or 0.0)

    def record_perp_trade(self, price: float, qty: float, is_buyer_maker: bool, now: float):
        """O(1) perpetual trade accumulation into 100ms ring bucket (zero per-trade allocations)."""
        self._perp_price = price
        self._perp_msg_ts = now
        notional = price * qty
        signed = -notional if is_buyer_maker else notional

        bkt = int(now * 10.0)
        idx = bkt & self._RING_MASK
        if self._ring_bkt[idx] != bkt:
            self._ring_bkt[idx] = bkt
            self._ring_signed[idx] = signed
            self._ring_vol[idx] = notional
        else:
            self._ring_signed[idx] += signed
            self._ring_vol[idx] += notional

        # Throttle basis EMA update to 4 Hz (250ms)
        spot = self._spot_price
        if spot > 0.0 and price > 0.0 and (not self._perp_basis_primed or (now - self._last_basis_upd_ts) >= 0.25):
            self._last_basis_upd_ts = now
            spread_bps = (price - spot) / spot * 10000.0
            if not self._perp_basis_primed:
                self._perp_basis_ema_bps = spread_bps
                self._perp_basis_primed = True
            else:
                self._perp_basis_ema_bps = 0.96 * self._perp_basis_ema_bps + 0.04 * spread_bps

    def _robust_baseline(self, hist: deque[float], default_val: float, floor_val: float, ceil_val: float) -> float:
        n = len(hist)
        if n == 0:
            return default_val
        med = float(median(hist))
        avg = sum(hist) / n
        est = 0.65 * med + 0.35 * avg
        if n < 30:
            w = n / 30.0
            est = w * est + (1.0 - w) * default_val
        return max(floor_val, min(ceil_val, est))

    def get_orderflow_metrics(self, now: float | None = None) -> dict:
        t = now or time.time()

        # Sync spot status scalars cleanly at 1 Hz
        spot_st = self.status["binance"]
        if self._spot_price > 0.0:
            spot_st["last_price"] = self._spot_price
            spot_st["last_msg_ts"] = self._spot_msg_ts
            spot_st["latency_ms"] = round(self._spot_latency_ms, 1)

        perp_st = self.status["binance_perp"]
        last_msg_ts = self._perp_msg_ts or float(perp_st.get("last_msg_ts") or 0.0)
        msg_age = (t - last_msg_ts) if last_msg_ts > 0.0 else 999.0
        if perp_st["connected"] and last_msg_ts > 0.0 and msg_age > self.WS_IDLE_TIMEOUT_SEC:
            perp_st["connected"] = False
            perp_st["last_error"] = "15s无新成交超时，重连中"
        perp_fresh = bool(perp_st["connected"] and msg_age <= 6.0)

        # Scan O(1) fixed 128-slot ring buffer (~10us)
        cur_bkt = int(t * 10.0)
        min_bkt_3 = cur_bkt - 30
        min_bkt_10 = cur_bkt - 100
        s3 = v3 = s10 = v10 = 0.0
        rb = self._ring_bkt
        rs = self._ring_signed
        rv = self._ring_vol
        for i in range(self._RING_SIZE):
            b = rb[i]
            if b >= min_bkt_10 and b <= cur_bkt:
                sv = rs[i]
                vv = rv[i]
                s10 += sv
                v10 += vv
                if b >= min_bkt_3:
                    s3 += sv
                    v3 += vv

        # Sample rolling 10m baseline once per second ONLY while perp stream is genuinely fresh (<=6s)
        if (t - self._last_vol_sample_ts) >= 0.95 and perp_fresh:
            self._last_vol_sample_ts = t
            self._vol_3s_hist.append(v3)
            self._vol_10s_hist.append(v10)

        # Recompute 600-element medians every 5s (or during initial 30s warm-up)
        if (t - self._last_base_calc_ts) >= 5.0 or len(self._vol_3s_hist) < 30:
            self._last_base_calc_ts = t
            self._cached_vol_3s_base = self._robust_baseline(self._vol_3s_hist, 60000.0, 12000.0, 600000.0)
            self._cached_vol_10s_base = self._robust_baseline(self._vol_10s_hist, 180000.0, 35000.0, 1500000.0)

        vol_3s_base = self._cached_vol_3s_base
        vol_10s_base = self._cached_vol_10s_base
        vol_3s_rel = v3 / max(12000.0, vol_3s_base)
        adaptive_cvd_3s_min = max(25000.0, min(160000.0, 1.35 * vol_3s_base))

        imb3 = (s3 / v3) if v3 > 1.0 else 0.0
        imb10 = (s10 / v10) if v10 > 1.0 else 0.0

        perp_p = self._perp_price if self._perp_price > 0.0 else float(perp_st["last_price"] or 0.0)
        spot_p = self.get_best_spot()
        basis_jump_bps = 0.0
        if perp_fresh and perp_p > 0.0 and spot_p > 0.0 and self._perp_basis_primed:
            cur_spread = (perp_p - spot_p) / spot_p * 10000.0
            basis_jump_bps = cur_spread - self._perp_basis_ema_bps

        perp_st.update({
            "last_price": perp_p,
            "last_msg_ts": last_msg_ts,
            "latency_ms": round(self._perp_latency_ms, 1),
            "perp_stale": not perp_fresh,
            "cvd_3s_imbalance": round(imb3, 3),
            "cvd_10s_imbalance": round(imb10, 3),
            "cvd_3s_usd": round(s3, 0),
            "perp_basis_jump_bps": round(basis_jump_bps, 2),
            "vol_3s_baseline_usd": round(vol_3s_base, 0),
            "adaptive_cvd_3s_min_usd": round(adaptive_cvd_3s_min, 0),
            "vol_3s_rel": round(vol_3s_rel, 2),
        })

        return {
            "perp_connected": bool(perp_st["connected"]),
            "perp_stale": not perp_fresh,
            "perp_price": perp_p,
            "cvd_3s_imbalance": round(imb3, 4),
            "cvd_10s_imbalance": round(imb10, 4),
            "cvd_3s_usd": round(s3, 1),
            "vol_3s_usd": round(v3, 1),
            "vol_10s_usd": round(v10, 1),
            "vol_3s_baseline_usd": round(vol_3s_base, 1),
            "vol_10s_baseline_usd": round(vol_10s_base, 1),
            "vol_3s_rel": round(vol_3s_rel, 3),
            "adaptive_cvd_3s_min_usd": round(adaptive_cvd_3s_min, 1),
            "perp_basis_jump_bps": round(basis_jump_bps, 3),
        }

    async def _run_idle_watchdog(self, ws, get_last_mono: Callable[[], float], timed_out_flag: list[bool]):
        """Checks once every 3s whether the stream has been silent for > WS_IDLE_TIMEOUT_SEC."""
        check_interval = min(3.0, max(0.05, self.WS_IDLE_TIMEOUT_SEC / 3.0))
        try:
            while not self._stop:
                await asyncio.sleep(check_interval)
                if (time.monotonic() - get_last_mono()) > self.WS_IDLE_TIMEOUT_SEC:
                    timed_out_flag[0] = True
                    try:
                        await ws.close()
                    except Exception:
                        pass
                    return
        except asyncio.CancelledError:
            pass

    async def run_binance(self):
        """
        Binance Spot BTC-USDT reference stream.
        Primary: 100ms-paced `depth5@100ms` (mid price) + `miniTicker` (1s server latency) -> <=11 msg/s!
        Fallback: `btcusdt@aggTrade` if combined stream returns 404.
        """
        primary_url = "wss://stream.binance.com:9443/stream?streams=btcusdt@depth5@100ms/btcusdt@miniTicker"
        fallback_url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
        use_fallback = False
        while not self._stop:
            url = fallback_url if use_fallback else primary_url
            timed_out = [False]
            last_rx_mono = time.monotonic()
            watchdog = None
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10, close_timeout=5) as ws:
                    self.status["binance"]["connected"] = True
                    self.status["binance"]["last_error"] = ""
                    last_rx_mono = time.monotonic()
                    watchdog = asyncio.create_task(self._run_idle_watchdog(ws, lambda: last_rx_mono, timed_out))
                    async for raw in ws:
                        if self._stop:
                            break
                        last_rx_mono = time.monotonic()
                        msg = fast_loads(raw)
                        now = time.time()
                        if "stream" in msg:
                            sname = msg["stream"]
                            data = msg["data"]
                            if "depth5" in sname:
                                bids = data.get("bids")
                                asks = data.get("asks")
                                if bids and asks:
                                    price = 0.5 * (float(bids[0][0]) + float(asks[0][0]))
                                    self._spot_price = price
                                    self._spot_msg_ts = now
                                    self.on_price("binance", price, now)
                            else:
                                c_px = float(data.get("c") or 0.0)
                                ev_ms = float(data.get("E") or (now * 1000.0))
                                self._spot_latency_ms = max(0.0, now * 1000.0 - ev_ms)
                                if c_px > 0.0 and self._spot_price <= 0.0:
                                    self._spot_price = c_px
                                self._spot_msg_ts = now
                        else:
                            p_raw = msg.get("p")
                            if p_raw is None:
                                continue
                            price = float(p_raw)
                            ev_ms = float(msg.get("E") or (now * 1000.0))
                            self._spot_price = price
                            self._spot_msg_ts = now
                            self._spot_latency_ms = max(0.0, now * 1000.0 - ev_ms)
                            self.on_price("binance", price, now)
                    if timed_out[0]:
                        raise asyncio.TimeoutError("spot idle timeout")
            except Exception as e:
                err_txt = "15s无新数据超时，重连中" if timed_out[0] else _fmt_feed_error(e)
                if (not use_fallback) and ("404" in err_txt):
                    use_fallback = True
                else:
                    use_fallback = False
                self.status["binance"]["connected"] = False
                self.status["binance"]["errors"] += 1
                self.status["binance"]["last_error"] = err_txt
                await asyncio.sleep(2.0 if "451" not in err_txt else 10.0)
            finally:
                if watchdog:
                    watchdog.cancel()

    async def run_binance_perp(self):
        """
        Leading orderflow stream: Binance USDT-M Perpetual Futures aggTrade.
        Uses Binance 2026 `/market/ws/btcusdt@aggTrade` + external 3s watchdog (zero per-frame wait_for overhead).
        """
        primary_url = "wss://fstream.binance.com/market/ws/btcusdt@aggTrade"
        fallback_url = "wss://fstream.binance.com/ws/btcusdt@aggTrade"
        use_fallback = False
        while not self._stop:
            url = fallback_url if use_fallback else primary_url
            timed_out = [False]
            last_rx_mono = time.monotonic()
            watchdog = None
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10, close_timeout=5) as ws:
                    self.status["binance_perp"]["connected"] = True
                    self.status["binance_perp"]["last_error"] = ""
                    last_rx_mono = time.monotonic()
                    watchdog = asyncio.create_task(self._run_idle_watchdog(ws, lambda: last_rx_mono, timed_out))
                    async for raw in ws:
                        if self._stop:
                            break
                        last_rx_mono = time.monotonic()
                        data = fast_loads(raw)
                        p_raw = data.get("p")
                        q_raw = data.get("q")
                        if p_raw is None or q_raw is None:
                            continue
                        price = float(p_raw)
                        qty = float(q_raw)
                        is_buyer_maker = bool(data.get("m", False))
                        now = time.time()
                        ev_ms = float(data.get("E") or (now * 1000.0))
                        self._perp_latency_ms = max(0.0, now * 1000.0 - ev_ms)
                        self.status["binance_perp"]["perp_stale"] = False
                        self.status["binance_perp"]["last_error"] = ""
                        self.record_perp_trade(price, qty, is_buyer_maker, now)
                    if timed_out[0]:
                        raise asyncio.TimeoutError("perp idle timeout")
            except Exception as e:
                err_txt = "15s无新成交超时，重连中" if timed_out[0] else _fmt_feed_error(e)
                if (not use_fallback) and ("404" in err_txt):
                    use_fallback = True
                else:
                    use_fallback = False
                self.status["binance_perp"]["connected"] = False
                self.status["binance_perp"]["perp_stale"] = True
                self.status["binance_perp"]["errors"] += 1
                self.status["binance_perp"]["last_error"] = err_txt
                await asyncio.sleep(2.5 if "451" not in err_txt else 10.0)
            finally:
                if watchdog:
                    watchdog.cancel()
