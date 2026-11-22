import asyncio
import json
import time
from collections import deque
from statistics import median
from typing import Callable
import websockets


def _fmt_feed_error(exc: Exception) -> str:
    """Convert raw WebSocket/network exceptions into concise Chinese fault diagnostics."""
    if isinstance(exc, asyncio.TimeoutError):
        return "15s无新成交超时，重连中"
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
    Connects to:
    1. Binance Spot BTC-USDT aggTrade (spot price reference)
    2. Binance USDT-M Perpetual Futures BTC-USDT aggTrade (LEADING orderflow: 3s/10s CVD + Perp-Spot basis jump
       + 10-minute rolling adaptive volume baseline so quiet & busy sessions both trigger accurately)

    v24/v26 Hardening:
    - Both Binance WebSocket loops enforce a strict 15s recv() idle timeout so half-open silent stalls
      immediately trigger reconnect instead of hanging forever with a green dot.
    - Rolling volume baseline ONLY samples while perp messages are actively arriving (<=6s fresh),
      preventing disconnected zeros from collapsing the adaptive CVD floor to $25k.
    """

    WS_IDLE_TIMEOUT_SEC = 15.0

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
        # Rolling 12s perp aggTrades: (ts, signed_usd, abs_usd)
        self._perp_trades: deque[tuple[float, float, float]] = deque(maxlen=4000)
        # Rolling 10-minute (600 x 1s) volume baselines for session-adaptive orderflow thresholds
        self._vol_3s_hist: deque[float] = deque(maxlen=600)
        self._vol_10s_hist: deque[float] = deque(maxlen=600)
        self._last_vol_sample_ts: float = 0.0
        self._perp_basis_ema_bps: float = 0.0
        self._perp_basis_primed: bool = False
        self._stop = False

    def stop(self):
        self._stop = True

    def get_best_spot(self) -> float:
        b = self.status["binance"]
        return float(b["last_price"] or 0.0)

    def _prune_perp_trades(self, now: float):
        cutoff = now - 12.0
        while self._perp_trades and self._perp_trades[0][0] < cutoff:
            self._perp_trades.popleft()

    def record_perp_trade(self, price: float, qty: float, is_buyer_maker: bool, now: float):
        """Record a Binance Perpetual Futures aggTrade & update rolling CVD / Perp-Spot basis jump."""
        notional = price * qty
        # m == False -> aggressive market BUY; m == True -> aggressive market SELL
        signed = -notional if is_buyer_maker else notional
        self._perp_trades.append((now, signed, notional))
        self._prune_perp_trades(now)

        spot = self.get_best_spot()
        if spot > 0 and price > 0:
            spread_bps = (price - spot) / spot * 10000.0
            if not self._perp_basis_primed:
                self._perp_basis_ema_bps = spread_bps
                self._perp_basis_primed = True
            else:
                self._perp_basis_ema_bps = 0.985 * self._perp_basis_ema_bps + 0.015 * spread_bps

    def _robust_baseline(self, hist: deque[float], default_val: float, floor_val: float, ceil_val: float) -> float:
        n = len(hist)
        if n == 0:
            return default_val
        med = float(median(hist))
        avg = sum(hist) / n
        est = 0.65 * med + 0.35 * avg
        # Smooth warm-up blend over first 30 seconds so cold-start never misfires on a single tick
        if n < 30:
            w = n / 30.0
            est = w * est + (1.0 - w) * default_val
        return max(floor_val, min(ceil_val, est))

    def get_orderflow_metrics(self, now: float | None = None) -> dict:
        t = now or time.time()
        self._prune_perp_trades(t)

        perp_st = self.status["binance_perp"]
        last_msg_ts = float(perp_st.get("last_msg_ts") or 0.0)
        msg_age = (t - last_msg_ts) if last_msg_ts > 0 else 999.0
        # Auto-surface silent stall on status indicator if >15s without any trade frame
        if perp_st["connected"] and last_msg_ts > 0 and msg_age > self.WS_IDLE_TIMEOUT_SEC:
            perp_st["connected"] = False
            perp_st["last_error"] = "15s无新成交超时，重连中"
        perp_fresh = bool(perp_st["connected"] and msg_age <= 6.0)

        s3, v3 = 0.0, 0.0
        s10, v10 = 0.0, 0.0
        cut3 = t - 3.0
        cut10 = t - 10.0
        for ts, signed, total in self._perp_trades:
            if ts >= cut10:
                s10 += signed
                v10 += total
                if ts >= cut3:
                    s3 += signed
                    v3 += total

        # CRITICAL v24 fix: ONLY sample rolling 10m baseline while perp stream is genuinely fresh (<=6s)!
        # Never pollute baseline with 0.0 during WebSocket stalls or disconnects.
        if (t - self._last_vol_sample_ts) >= 0.95 and perp_fresh:
            self._last_vol_sample_ts = t
            self._vol_3s_hist.append(v3)
            self._vol_10s_hist.append(v10)

        vol_3s_base = self._robust_baseline(self._vol_3s_hist, default_val=60000.0, floor_val=12000.0, ceil_val=600000.0)
        vol_10s_base = self._robust_baseline(self._vol_10s_hist, default_val=180000.0, floor_val=35000.0, ceil_val=1500000.0)
        vol_3s_rel = v3 / max(12000.0, vol_3s_base)
        # Adaptive net 3s CVD USD floor: scales smoothly from $25k (calm sessions) up to $160k (peak US sessions)
        adaptive_cvd_3s_min = max(25000.0, min(160000.0, 1.35 * vol_3s_base))

        imb3 = (s3 / v3) if v3 > 1.0 else 0.0
        imb10 = (s10 / v10) if v10 > 1.0 else 0.0

        perp_p = float(perp_st["last_price"] or 0.0)
        spot_p = self.get_best_spot()
        basis_jump_bps = 0.0
        if perp_fresh and perp_p > 0 and spot_p > 0 and self._perp_basis_primed:
            cur_spread = (perp_p - spot_p) / spot_p * 10000.0
            basis_jump_bps = cur_spread - self._perp_basis_ema_bps

        perp_st.update({
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

    async def run_binance(self):
        url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
        while not self._stop:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10, close_timeout=5) as ws:
                    self.status["binance"]["connected"] = True
                    while not self._stop:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.WS_IDLE_TIMEOUT_SEC)
                        data = json.loads(raw)
                        price = float(data["p"])
                        event_ms = float(data.get("E", time.time() * 1000))
                        now = time.time()
                        lat = max(0.0, now * 1000.0 - event_ms)
                        self.status["binance"].update({
                            "connected": True,
                            "last_price": price,
                            "latency_ms": round(lat, 1),
                            "last_msg_ts": now,
                            "last_error": "",
                        })
                        self.on_price("binance", price, now)
            except Exception as e:
                self.status["binance"]["connected"] = False
                self.status["binance"]["errors"] += 1
                self.status["binance"]["last_error"] = _fmt_feed_error(e)
                await asyncio.sleep(2.0)

    async def run_binance_perp(self):
        """
        Leading orderflow stream: Binance USDT-M Perpetual Futures aggTrade.
        v27 Fix: Uses Binance 2026 `/market` WebSocket route (`/market/ws/btcusdt@aggTrade`),
        with 15s idle-stall watchdog and structured Chinese `last_error` reporting.
        """
        primary_url = "wss://fstream.binance.com/market/ws/btcusdt@aggTrade"
        fallback_url = "wss://fstream.binance.com/ws/btcusdt@aggTrade"
        use_fallback = False
        while not self._stop:
            url = fallback_url if use_fallback else primary_url
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10, close_timeout=5) as ws:
                    self.status["binance_perp"]["connected"] = True
                    while not self._stop:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.WS_IDLE_TIMEOUT_SEC)
                        data = json.loads(raw)
                        if "p" not in data or "q" not in data:
                            continue
                        price = float(data["p"])
                        qty = float(data["q"])
                        is_buyer_maker = bool(data.get("m", False))
                        event_ms = float(data.get("E", time.time() * 1000))
                        now = time.time()
                        lat = max(0.0, now * 1000.0 - event_ms)
                        self.status["binance_perp"].update({
                            "connected": True,
                            "perp_stale": False,
                            "last_price": price,
                            "latency_ms": round(lat, 1),
                            "last_msg_ts": now,
                            "last_error": "",
                        })
                        self.record_perp_trade(price, qty, is_buyer_maker, now)
            except Exception as e:
                err_txt = _fmt_feed_error(e)
                # Only fall back if primary path returned 404; always revert back to primary on next failure
                if (not use_fallback) and ("404" in err_txt):
                    use_fallback = True
                else:
                    use_fallback = False
                self.status["binance_perp"]["connected"] = False
                self.status["binance_perp"]["perp_stale"] = True
                self.status["binance_perp"]["errors"] += 1
                self.status["binance_perp"]["last_error"] = err_txt
                await asyncio.sleep(2.5 if "451" not in err_txt else 10.0)

