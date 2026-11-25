import asyncio
import json
import math
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Optional
import aiohttp
import websockets

from backend.config import (
    CLOB_API_BASE,
    CRYPTO_PRICE_API,
    GAMMA_API_BASE,
    GEOBLOCK_URL,
    HARD_LIMITS,
    PAST_RESULTS_API,
    POLYBOLT_WS_URL,
    POLY_HEADERS,
    POLY_PROXY,
    RTDS_WS_URL,
    ws_headers_kwarg,
)
from backend.fast_json import loads as fast_loads
from backend.feeds.clock import ServerClock


class PolymarketOracleSync:
    """
    Polymarket Official Oracle & Four-Source Settlement Engine (v37):
    1. Primary native official TWAP60 streams: PolyBolt (`price.crypto.twap`) + Public RTDS (`crypto_prices_twap_sixty`).
    2. Local 900s Chainlink spot buffer with `TWAP_LAG_SEC = 2.5` linear interpolation fallback + `twap_err` monitoring.
    3. Dynamic Gamma metadata parsing: `tick_size`, `min_size`, `fees_enabled`, `taker_fee_rate`, `accepting_orders`, `neg_risk`.
    4. Official `crypto-price` API (429 exponential backoff 3s->15s) + `past-results` 8-round batch API.
    5. Four-source parallel official settlement (Gamma + CLOB `/markets/{id}` + `crypto-price` + `past-results`).
    6. Integrated `ServerClock` fed by WS timestamps, HTTP `Date` headers, and Binance RTT checks.
    """

    TWAP_LAG_SEC = 2.5

    def __init__(self, on_oracle_tick: Optional[Callable] = None, clock: Optional[ServerClock] = None):
        self.on_oracle_tick = on_oracle_tick
        self.clock = clock or ServerClock()
        self.creds: Optional[dict] = None
        self.latest_spot = Decimal("0")
        self.latest_twap60 = Decimal("0")
        self.latest_spot_float: float = 0.0
        self.latest_twap60_float: float = 0.0
        self.latest_twap_ts: float = 0.0
        self.active_oracle_source: str = "RTDS"

        self.twap_history: list[tuple[float, Decimal]] = []
        self.spot_history: deque[tuple[int, float]] = deque(maxlen=960)
        self._twap_errors: deque[float] = deque(maxlen=120)

        self._cp_backoff_sec: float = 3.0
        self._cp_next_allowed_ts: float = 0.0

        self._stop = False
        self._reconnect_polybolt = asyncio.Event()
        self._http_session: Optional[aiohttp.ClientSession] = None

        self.status = {
            "rtds": {
                "connected": False,
                "latency_ms": 0.0,
                "last_msg_ts": 0.0,
                "last_spot": "0",
                "last_twap60": "0",
                "errors": 0,
                "last_error": "",
                "note": "正在连接 Polymarket 公开 Chainlink 60s TWAP 流...",
            },
            "polybolt": {
                "connected": False,
                "authenticated": False,
                "latency_ms": 0.0,
                "last_msg_ts": 0.0,
                "last_spot": "0",
                "last_twap60": "0",
                "errors": 0,
                "last_error": "",
                "note": "未配置API凭证(已自动使用公开RTDS真实流)",
            },
            "gamma": {"connected": False, "latency_ms": 0.0, "last_msg_ts": 0.0, "errors": 0, "last_error": ""},
            "geo_check": {"checked": False, "blocked": False, "country": "UNKNOWN", "clock_drift_ms": 0.0},
            "twap_err": {"samples": 0, "mean_usd": 0.0, "max_abs_usd": 0.0},
        }

    def set_credentials(self, api_key: str, secret: str, passphrase: str):
        if api_key and secret and passphrase:
            self.creds = {"apiKey": api_key, "secret": secret, "passphrase": passphrase}
            self.status["polybolt"]["note"] = "正在认证 PolyBolt 主流..."
            self._reconnect_polybolt.set()
        else:
            self.creds = None
            self.status["polybolt"].update({
                "connected": False,
                "authenticated": False,
                "note": "未配置API凭证(已自动使用公开RTDS真实流)",
            })

    def stop(self):
        self._stop = True
        self._reconnect_polybolt.set()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            connector = aiohttp.TCPConnector(limit=12, ttl_dns_cache=300)
            self._http_session = aiohttp.ClientSession(connector=connector, headers=POLY_HEADERS)
        return self._http_session

    async def close(self):
        if self._http_session and not self._http_session.closed:
            try:
                await self._http_session.close()
            except Exception:
                pass
            self._http_session = None

    def get_window_ts(self, now: Optional[float] = None) -> int:
        t = self.clock.server_now() if now is None else float(now)
        return int(t // 300) * 300

    @staticmethod
    def _iso_utc(ts: int) -> str:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _add_spot_point(self, ev_sec: float, price_f: float):
        sec_i = int(round(ev_sec))
        if sec_i <= 0 or price_f <= 0:
            return
        if self.spot_history:
            last_sec, _ = self.spot_history[-1]
            if sec_i < last_sec:
                return
            if sec_i == last_sec:
                self.spot_history[-1] = (sec_i, price_f)
                return
        self.spot_history.append((sec_i, price_f))

    def calc_local_twap(self, t_sec: float, lookback: int = 60) -> Optional[Decimal]:
        if len(self.spot_history) < max(10, lookback - 2):
            return None
        target_end = float(t_sec) - self.TWAP_LAG_SEC
        end_lo = math.floor(target_end)
        frac = target_end - end_lo
        end_hi = end_lo + 1

        def _window_avg(end_i: int) -> Optional[float]:
            start_i = end_i - lookback
            vals = [v for ts, v in self.spot_history if start_i <= ts < end_i]
            if len(vals) < max(5, lookback - 2):
                return None
            return sum(vals) / len(vals)

        avg_lo = _window_avg(end_lo)
        avg_hi = _window_avg(end_hi)
        if avg_lo is not None and avg_hi is not None:
            est = avg_lo * (1.0 - frac) + avg_hi * frac
        elif avg_lo is not None:
            est = avg_lo
        elif avg_hi is not None:
            est = avg_hi
        else:
            return None
        return Decimal(f"{est:.2f}")

    def get_effective_twap60(self, now_sec: Optional[float] = None) -> Decimal:
        t = self.clock.server_now() if now_sec is None else float(now_sec)
        if self.latest_twap60 > 0 and (t - self.latest_twap_ts) <= 15.0:
            return self.latest_twap60
        local_est = self.calc_local_twap(t, lookback=60)
        if local_est is not None and local_est > 0:
            return local_est
        return self.latest_twap60

    def get_twap_at_boundary_detail(self, target_ts: int, max_delta_sec: float = 15.0) -> tuple[Optional[Decimal], float]:
        if not self.twap_history:
            est = self.calc_local_twap(float(target_ts), lookback=60)
            return (est, 3.0) if est is not None else (None, 999.0)
        best_px = None
        best_dt = 1e9
        best_score = 1e9
        for ts, px in self.twap_history:
            dt = abs(ts - target_ts)
            if dt <= max_delta_sec:
                score = dt - (0.15 if ts >= target_ts else 0.0)
                if score < best_score:
                    best_score = score
                    best_dt = dt
                    best_px = px
        if best_px is not None:
            return best_px, best_dt
        est = self.calc_local_twap(float(target_ts), lookback=60)
        return (est, 3.0) if est is not None else (None, 999.0)

    def get_twap_at_boundary(self, target_ts: int, max_delta_sec: float = 15.0) -> Optional[Decimal]:
        px, _ = self.get_twap_at_boundary_detail(target_ts, max_delta_sec=max_delta_sec)
        return px

    @staticmethod
    def _parse_dec(val, allow_e18: bool = False) -> Optional[Decimal]:
        if val is None or val == "":
            return None
        try:
            d = Decimal(str(val))
            if allow_e18 and d > Decimal("1e12"):
                d = d / Decimal("1e18")
            return d if d > 0 else None
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _parse_ts_sec(item: dict, default_ts: float) -> float:
        for k in ("timestamp", "ts", "time"):
            raw = item.get(k)
            if raw is not None:
                try:
                    v = float(raw)
                    if v > 1e12:
                        return v / 1000.0
                    if v > 1e9:
                        return v
                except (TypeError, ValueError):
                    pass
        return default_ts

    def _record_oracle_price(self, kind: str, price: Decimal, ev_sec: float, source_name: str, is_snapshot: bool):
        if source_name == "RTDS" and self.status["polybolt"]["connected"] and (time.time() - self.status["polybolt"]["last_msg_ts"] < 12):
            return
        self.active_oracle_source = source_name
        if not is_snapshot and ev_sec > 1e9:
            self.clock.observe(ev_sec * 1000.0)
            self.status["geo_check"]["clock_drift_ms"] = self.clock.drift_ms()

        if kind == "spot":
            self.latest_spot = price
            self.latest_spot_float = float(price)
            self._add_spot_point(ev_sec, float(price))
        elif kind == "twap60":
            self.latest_twap60 = price
            self.latest_twap60_float = float(price)
            self.latest_twap_ts = ev_sec if ev_sec > 1e9 else time.time()
            self.twap_history.append((ev_sec, price))
            if len(self.twap_history) > 1000:
                self.twap_history = self.twap_history[-700:]
            if not is_snapshot:
                local_est = self.calc_local_twap(ev_sec, lookback=60)
                if local_est is not None:
                    err = float(local_est - price)
                    if abs(err) < 500.0:
                        self._twap_errors.append(err)
                        n = len(self._twap_errors)
                        self.status["twap_err"] = {
                            "samples": n,
                            "mean_usd": round(sum(self._twap_errors) / n, 2),
                            "max_abs_usd": round(max(abs(x) for x in self._twap_errors), 2),
                        }

        if self.on_oracle_tick:
            self.on_oracle_tick(kind, price, ev_sec, is_snapshot, source_name)

    async def check_clock_drift(self) -> Optional[float]:
        try:
            session = await self._get_session()
            t0 = time.time() * 1000.0
            async with session.get(
                "https://api.binance.com/api/v3/time",
                proxy=POLY_PROXY or None,
                timeout=aiohttp.ClientTimeout(total=4),
            ) as r:
                t1 = time.time() * 1000.0
                if r.status == 200:
                    srv_ms = float((await r.json()).get("serverTime", 0))
                    if srv_ms > 0:
                        self.clock.observe_rtt_server_ms(srv_ms, rtt_ms=(t1 - t0))
                        drift = self.clock.drift_ms()
                        self.status["geo_check"]["clock_drift_ms"] = drift
                        self.status["geo_check"]["clock_checked_at"] = round(t1 / 1000.0, 1)
                        return drift
        except Exception:
            pass
        return None

    async def check_geo_and_clock(self):
        try:
            session = await self._get_session()
            t0 = time.time()
            async with session.get(
                GEOBLOCK_URL,
                proxy=POLY_PROXY or None,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as r:
                self.clock.observe_http_date(r.headers.get("Date"), rtt_ms=(time.time() - t0) * 1000.0)
                if r.status == 200:
                    data = await r.json()
                    self.status["geo_check"].update({
                        "checked": True,
                        "blocked": bool(data.get("blocked", False)),
                        "country": data.get("country", "UNKNOWN"),
                        "ip": data.get("ip", ""),
                    })
        except Exception:
            pass
        await self.check_clock_drift()

    async def fetch_gamma_round(self, window_ts: int, bust_cache: bool = False) -> Optional[dict]:
        slug = f"btc-updown-5m-{window_ts}"
        t0 = time.time()
        try:
            session = await self._get_session()
            data = None
            cb_val = str(int(t0 * 1000)) if bust_cache else None
            param_variants = [
                {"slug": slug, **({"_t": cb_val} if cb_val else {})},
                {"slug": slug, "closed": "true", **({"_t": cb_val} if cb_val else {})},
            ]
            for params in param_variants:
                req_t0 = time.time()
                async with session.get(
                    f"{GAMMA_API_BASE}/events",
                    params=params,
                    proxy=POLY_PROXY or None,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    rtt_ms = (time.time() - req_t0) * 1000.0
                    self.clock.observe_http_date(resp.headers.get("Date"), rtt_ms=rtt_ms)
                    lat = round((time.time() - t0) * 1000.0, 1)
                    if resp.status != 200:
                        self.status["gamma"]["last_error"] = f"HTTP {resp.status}"
                        continue
                    candidate = await resp.json()
                    self.status["gamma"].update({"connected": True, "latency_ms": lat, "last_msg_ts": time.time(), "last_error": ""})
                    if candidate:
                        data = candidate
                        break
            if not data:
                return None

            event = data[0] if isinstance(data, list) else data
            markets = event.get("markets") or []
            if not markets:
                return None
            market = markets[0]
            meta = event.get("eventMetadata") or {}

            price_to_beat = self._parse_dec(meta.get("priceToBeat"))
            final_price = self._parse_dec(meta.get("finalPrice"))

            tick_size = 0.01
            try:
                raw_tick = market.get("orderPriceMinTickSize")
                if raw_tick not in (None, ""):
                    tf = float(raw_tick)
                    if 0.0001 <= tf <= 0.1:
                        tick_size = round(tf, 4)
            except (TypeError, ValueError):
                pass

            min_size = 5.0
            try:
                raw_ms = market.get("orderMinSize")
                if raw_ms not in (None, ""):
                    msf = float(raw_ms)
                    if msf > 0:
                        min_size = round(msf, 2)
            except (TypeError, ValueError):
                pass

            fees_enabled_raw = market.get("feesEnabled")
            taker_fee_rate = float(HARD_LIMITS["taker_fee_rate"])
            if fees_enabled_raw is False:
                taker_fee_rate = 0.0
            else:
                sched = market.get("feeSchedule") or {}
                if isinstance(sched, str):
                    try:
                        sched = json.loads(sched)
                    except Exception:
                        sched = {}
                if isinstance(sched, dict) and sched.get("rate") not in (None, ""):
                    try:
                        r_f = float(sched["rate"])
                        if 0.0 <= r_f <= 0.25:
                            taker_fee_rate = r_f
                    except (TypeError, ValueError):
                        pass

            accepting_orders = True if market.get("acceptingOrders") is None else bool(market.get("acceptingOrders"))
            neg_risk = bool(market.get("negRisk") or event.get("negRisk") or False)
            raw_ver = market.get("version") if market.get("version") not in (None, "") else event.get("version")
            if str(raw_ver or "").strip().lower() in ("2", "v2"):
                market_version = 2
            elif market.get("positionIds") and not market.get("clobTokenIds"):
                market_version = 2
            else:
                market_version = 1
            twap_lookback = 60
            cmc = market.get("cryptoMarketConfig") or event.get("cryptoMarketConfig") or {}
            if isinstance(cmc, dict) and cmc.get("twapLookbackSeconds"):
                try:
                    twap_lookback = max(10, int(cmc["twapLookbackSeconds"]))
                except (TypeError, ValueError):
                    pass

            raw_tokens = market.get("clobTokenIds") or market.get("positionIds")
            if isinstance(raw_tokens, str):
                raw_tokens = json.loads(raw_tokens)

            outcome_prices = market.get("outcomePrices")
            if isinstance(outcome_prices, str):
                try:
                    outcome_prices = json.loads(outcome_prices)
                except Exception:
                    outcome_prices = None

            outcomes = market.get("outcomes")
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except Exception:
                    outcomes = ["Up", "Down"]
            if not isinstance(outcomes, list) or len(outcomes) != 2:
                outcomes = ["Up", "Down"]

            up_token = None
            down_token = None
            if raw_tokens and len(raw_tokens) >= 2:
                o0 = str(outcomes[0]).strip().upper()
                o1 = str(outcomes[1]).strip().upper()
                if o0 == "DOWN" and o1 == "UP":
                    up_token, down_token = str(raw_tokens[1]), str(raw_tokens[0])
                else:
                    up_token, down_token = str(raw_tokens[0]), str(raw_tokens[1])

            resolved_outcome = None
            is_closed = bool(market.get("closed"))
            if is_closed and outcome_prices and len(outcome_prices) == 2:
                try:
                    f0, f1 = float(outcome_prices[0]), float(outcome_prices[1])
                    o0 = str(outcomes[0]).strip().upper()
                    o1 = str(outcomes[1]).strip().upper()
                    if f0 >= 0.995 and f1 <= 0.005:
                        resolved_outcome = "UP" if o0 == "UP" else "DOWN"
                    elif f1 >= 0.995 and f0 <= 0.005:
                        resolved_outcome = "UP" if o1 == "UP" else "DOWN"
                except Exception:
                    pass

            return {
                "slug": slug,
                "window_ts": window_ts,
                "condition_id": market.get("conditionId"),
                "up_token_id": up_token,
                "down_token_id": down_token,
                "price_to_beat": price_to_beat,
                "final_price": final_price,
                "closed": is_closed,
                "resolved_outcome": resolved_outcome,
                "tick_size": tick_size,
                "min_size": min_size,
                "fees_enabled": (fees_enabled_raw is not False),
                "taker_fee_rate": taker_fee_rate,
                "accepting_orders": accepting_orders,
                "neg_risk": neg_risk,
                "market_version": market_version,
                "twap_lookback": twap_lookback,
            }
        except Exception as e:
            self.status["gamma"]["connected"] = False
            self.status["gamma"]["errors"] += 1
            self.status["gamma"]["last_error"] = "接口超时" if isinstance(e, asyncio.TimeoutError) else (str(e)[:48] or type(e).__name__)
            return None

    async def fetch_crypto_price(self, window_ts: int, lookback: int = 60) -> Optional[dict]:
        now = time.time()
        if now < self._cp_next_allowed_ts:
            return None
        try:
            session = await self._get_session()
            t0 = time.time()
            async with session.get(
                CRYPTO_PRICE_API,
                params={
                    "symbol": "BTC",
                    "eventStartTime": self._iso_utc(window_ts),
                    "variant": "fiveminute",
                    "endDate": self._iso_utc(window_ts + 300),
                    "twapEnabled": "true",
                    "twapLookbackSeconds": str(int(lookback)),
                },
                proxy=POLY_PROXY or None,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                self.clock.observe_http_date(resp.headers.get("Date"), rtt_ms=(time.time() - t0) * 1000.0)
                if resp.status == 429:
                    self._cp_next_allowed_ts = time.time() + self._cp_backoff_sec
                    self._cp_backoff_sec = min(15.0, self._cp_backoff_sec * 2.0)
                    return None
                if resp.status != 200:
                    return None
                self._cp_backoff_sec = 3.0
                d = await resp.json()
                open_px = self._parse_dec(d.get("openPrice"))
                close_px = self._parse_dec(d.get("closePrice"))
                completed = bool(d.get("completed"))
                outcome = None
                if completed and open_px is not None and close_px is not None:
                    outcome = "UP" if close_px >= open_px else "DOWN"
                return {
                    "open_price": open_px,
                    "close_price": close_px,
                    "completed": completed,
                    "outcome": outcome,
                }
        except Exception:
            return None

    async def fetch_past_results(self, base_window_ts: int, count: int = 8) -> dict[int, dict]:
        slugs = [f"btc-updown-5m-{base_window_ts - i * 300}" for i in range(max(1, count))]
        try:
            session = await self._get_session()
            async with session.get(
                PAST_RESULTS_API,
                params={"pastEventSlugs": ",".join(slugs)},
                proxy=POLY_PROXY or None,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status != 200:
                    return {}
                d = await resp.json()
                out_map: dict[int, dict] = {}
                by_slug = d.get("outcomesBySlug") or d.get("resultsBySlug") or {}
                if isinstance(by_slug, dict):
                    for slug, val in by_slug.items():
                        try:
                            w_ts = int(str(slug).rsplit("-", 1)[-1])
                        except (TypeError, ValueError):
                            continue
                        if isinstance(val, str):
                            u = val.strip().upper()
                            if u in ("UP", "DOWN"):
                                out_map[w_ts] = {"outcome": u, "open_price": None, "close_price": None}
                        elif isinstance(val, dict):
                            u = str(val.get("outcome") or val.get("winner") or "").strip().upper()
                            if u in ("UP", "DOWN"):
                                out_map[w_ts] = {
                                    "outcome": u,
                                    "open_price": self._parse_dec(val.get("openPrice") or val.get("priceToBeat")),
                                    "close_price": self._parse_dec(val.get("closePrice") or val.get("finalPrice")),
                                }
                if isinstance(d.get("results"), list):
                    for item in d["results"]:
                        if not isinstance(item, dict):
                            continue
                        slug = str(item.get("slug") or "")
                        if not slug.startswith("btc-updown-5m-"):
                            continue
                        try:
                            w_ts = int(slug.rsplit("-", 1)[-1])
                        except ValueError:
                            continue
                        u = str(item.get("outcome") or item.get("winner") or "").strip().upper()
                        if u in ("UP", "DOWN"):
                            out_map[w_ts] = {
                                "outcome": u,
                                "open_price": self._parse_dec(item.get("openPrice")),
                                "close_price": self._parse_dec(item.get("closePrice")),
                            }
                return out_map
        except Exception:
            return {}

    async def fetch_clob_winner(self, condition_id: Optional[str]) -> Optional[str]:
        if not condition_id:
            return None
        try:
            session = await self._get_session()
            async with session.get(
                f"{CLOB_API_BASE}/markets/{condition_id}",
                params={"_t": str(int(time.time() * 1000))},
                proxy=POLY_PROXY or None,
                timeout=aiohttp.ClientTimeout(total=4),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                for tok in (data.get("tokens") or []):
                    if tok.get("winner") is True:
                        out = str(tok.get("outcome") or "").strip().upper()
                        if out in ("UP", "DOWN"):
                            return out
        except Exception:
            pass
        return None

    async def wait_official_settlement(
        self,
        window_ts: int,
        fallback_twap: Decimal,
        fallback_ptb: Optional[Decimal],
        known_condition_id: Optional[str] = None,
        timeout_sec: int = 420,
    ) -> dict:
        start_ts = time.time()
        deadline = start_ts + timeout_sec
        boundary_twap = self.get_twap_at_boundary(window_ts + 300, max_delta_sec=10.0)
        settlement_price = boundary_twap if (boundary_twap and boundary_twap > 0) else fallback_twap
        official_ptb = fallback_ptb
        condition_id = known_condition_id
        poll_idx = 0

        while time.time() < deadline and not self._stop:
            poll_idx += 1
            tasks = [
                self.fetch_gamma_round(window_ts, bust_cache=True),
                self.fetch_gamma_round(window_ts + 300, bust_cache=True),
                self.fetch_crypto_price(window_ts),
                self.fetch_clob_winner(condition_id) if condition_id else asyncio.sleep(0, result=None),
                self.fetch_past_results(window_ts, count=8) if (poll_idx % 2 == 0 or poll_idx == 1) else asyncio.sleep(0, result={}),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            cur_round = results[0] if isinstance(results[0], dict) else None
            next_round = results[1] if isinstance(results[1], dict) else None
            cp_res = results[2] if isinstance(results[2], dict) else None
            clob_winner = results[3] if isinstance(results[3], str) else None
            past_map = results[4] if isinstance(results[4], dict) else {}

            official_outcome = None

            if cur_round:
                if not condition_id and cur_round.get("condition_id"):
                    condition_id = cur_round["condition_id"]
                    if not clob_winner:
                        clob_winner = await self.fetch_clob_winner(condition_id)
                if cur_round["price_to_beat"] is not None:
                    official_ptb = cur_round["price_to_beat"]
                if cur_round["final_price"] is not None:
                    settlement_price = cur_round["final_price"]
                if cur_round.get("resolved_outcome") in ("UP", "DOWN"):
                    official_outcome = cur_round["resolved_outcome"]

            if next_round and next_round.get("price_to_beat") is not None:
                if not (cur_round and cur_round.get("final_price") is not None):
                    settlement_price = next_round["price_to_beat"]

            if cp_res:
                if cp_res.get("open_price") is not None and official_ptb is None:
                    official_ptb = cp_res["open_price"]
                if cp_res.get("close_price") is not None:
                    settlement_price = cp_res["close_price"]
                if not official_outcome and cp_res.get("outcome") in ("UP", "DOWN"):
                    official_outcome = cp_res["outcome"]

            if not official_outcome and clob_winner in ("UP", "DOWN"):
                official_outcome = clob_winner

            if not official_outcome and window_ts in past_map:
                pr = past_map[window_ts]
                if pr.get("open_price") is not None and official_ptb is None:
                    official_ptb = pr["open_price"]
                if pr.get("close_price") is not None:
                    settlement_price = pr["close_price"]
                if pr.get("outcome") in ("UP", "DOWN"):
                    official_outcome = pr["outcome"]

            if official_outcome in ("UP", "DOWN"):
                return {
                    "window_ts": window_ts,
                    "price_to_beat": str(official_ptb.quantize(Decimal("0.01"))) if official_ptb is not None else "--",
                    "settlement_price": str(settlement_price.quantize(Decimal("0.01"))) if settlement_price > 0 else "--",
                    "official_outcome": official_outcome,
                    "verified": True,
                }

            elapsed_wait = time.time() - start_ts
            await asyncio.sleep(2.5 if elapsed_wait < 60 else 4.0)

        return {
            "window_ts": window_ts,
            "price_to_beat": str(official_ptb.quantize(Decimal("0.01"))) if official_ptb is not None else "--",
            "settlement_price": str(settlement_price.quantize(Decimal("0.01"))) if settlement_price > 0 else "--",
            "official_outcome": None,
            "verified": False,
        }

    def _extract_items(self, msg: dict) -> list[dict]:
        payload = msg.get("payload")
        if payload is None:
            payload = msg.get("data")
        if payload is None:
            return []
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            if isinstance(payload.get("data"), list):
                return [x for x in payload["data"] if isinstance(x, dict)]
            if isinstance(payload.get("items"), list):
                return [x for x in payload["items"] if isinstance(x, dict)]
            return [payload]
        return []

    async def run_rtds(self):
        compact_filter = json.dumps({"symbol": "btc/usd"}, separators=(",", ":"))
        sub_frame = json.dumps({
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices_chainlink", "type": "*", "filters": compact_filter},
                {"topic": "crypto_prices_twap_sixty", "type": "*", "filters": compact_filter},
            ],
        }, separators=(",", ":"))

        while not self._stop:
            try:
                async with websockets.connect(
                    RTDS_WS_URL,
                    ping_interval=None,
                    close_timeout=5,
                    **ws_headers_kwarg(websockets.connect),
                ) as ws:
                    await ws.send(sub_frame)
                    self.status["rtds"].update({
                        "connected": True,
                        "note": "已直连 Polymarket 公开 Chainlink 60s TWAP 真实流",
                    })
                    async def _rtds_pinger():
                        try:
                            while not self._stop:
                                await asyncio.sleep(5.0)
                                await ws.send("PING")
                        except Exception:
                            pass

                    ping_task = asyncio.create_task(_rtds_pinger())
                    try:
                        async for raw in ws:
                            if self._stop:
                                break
                            if not raw or raw in ("PONG", "pong"):
                                continue
                            try:
                                msg = fast_loads(raw)
                            except Exception:
                                continue

                            topic = msg.get("topic") or ""
                            msg_type = msg.get("type") or ""
                            if topic not in ("crypto_prices_chainlink", "crypto_prices_twap_sixty"):
                                continue

                            items = self._extract_items(msg)
                            if not items:
                                continue

                            is_snapshot = (msg_type == "subscribe")
                            recv_now = time.time()
                            kind = "twap60" if topic == "crypto_prices_twap_sixty" else "spot"

                            for item in items:
                                sym = str(item.get("symbol") or "").lower()
                                if sym and sym not in ("btc/usd", "btcusd"):
                                    continue
                                price = self._parse_dec(item.get("full_accuracy_value"), allow_e18=True)
                                if price is None:
                                    price = self._parse_dec(item.get("value"), allow_e18=False)
                                if price is None:
                                    continue

                                ev_sec = self._parse_ts_sec(item, recv_now)
                                lat = max(0.0, (self.clock.server_now() - ev_sec) * 1000.0) if not is_snapshot else 0.0
                                if kind == "spot":
                                    self.status["rtds"]["last_spot"] = str(price.quantize(Decimal("0.01")))
                                else:
                                    self.status["rtds"]["last_twap60"] = str(price.quantize(Decimal("0.01")))
                                self.status["rtds"].update({
                                    "latency_ms": round(lat, 1),
                                    "last_msg_ts": recv_now,
                                    "last_error": "",
                                })
                                self._record_oracle_price(kind, price, ev_sec, "RTDS", is_snapshot)
                    finally:
                        ping_task.cancel()
            except Exception as e:
                self.status["rtds"]["connected"] = False
                self.status["rtds"]["errors"] += 1
                err_s = "连接超时" if isinstance(e, asyncio.TimeoutError) else (str(e)[:48] or type(e).__name__)
                self.status["rtds"]["last_error"] = err_s
                self.status["rtds"]["note"] = f"RTDS 重连中: {err_s}"
                await asyncio.sleep(2.5)

    async def run_polybolt(self):
        while not self._stop:
            self._reconnect_polybolt.clear()
            if not self.creds:
                self.status["polybolt"]["connected"] = False
                self.status["polybolt"]["authenticated"] = False
                await asyncio.sleep(2.0)
                continue

            try:
                async with websockets.connect(
                    POLYBOLT_WS_URL,
                    ping_interval=None,
                    close_timeout=5,
                    **ws_headers_kwarg(websockets.connect),
                ) as ws:
                    auth_frame = {
                        "op": "auth",
                        "rid": "a1",
                        "auth": {
                            "apiKey": self.creds["apiKey"],
                            "secret": self.creds["secret"],
                            "passphrase": self.creds["passphrase"],
                        },
                    }
                    await ws.send(json.dumps(auth_frame))
                    raw_auth = await asyncio.wait_for(ws.recv(), timeout=10.0)
                    auth_reply = json.loads(raw_auth)
                    if auth_reply.get("op") == "error" or auth_reply.get("error"):
                        err_msg = auth_reply.get("message") or auth_reply.get("error") or str(auth_reply)
                        self.status["polybolt"].update({
                            "connected": False,
                            "authenticated": False,
                            "errors": self.status["polybolt"]["errors"] + 1,
                            "note": f"PolyBolt 认证失败(已自动回退公开RTDS): {str(err_msg)[:50]}",
                        })
                        await asyncio.sleep(6.0)
                        continue

                    await ws.send(json.dumps({
                        "op": "subscribe",
                        "rid": "s1",
                        "subscriptions": [
                            {"channel": "price.crypto", "filter": {"symbol": "btcusd"}},
                            {"channel": "price.crypto.twap", "filter": {"symbol": "btcusd", "window_seconds": 60}},
                        ],
                    }))
                    self.status["polybolt"].update({
                        "connected": True,
                        "authenticated": True,
                        "note": "已认证并直连 PolyBolt 官方60s TWAP(主源)",
                    })

                    while not self._stop and not self._reconnect_polybolt.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=35.0)
                        except asyncio.TimeoutError:
                            continue
                        msg = fast_loads(raw)
                        op = msg.get("op")
                        if op == "error":
                            self.status["polybolt"]["note"] = f"订阅错误: {msg.get('message') or msg.get('code') or str(msg)[:60]}"
                            continue
                        if op in ("subscribed", "authenticated", "pong"):
                            continue

                        channel = msg.get("channel") or msg.get("topic") or ""
                        items = self._extract_items(msg)
                        if not channel or not items:
                            continue

                        recv_now = time.time()
                        is_snapshot = bool(msg.get("snapshot"))
                        kind = "twap60" if "twap" in channel else "spot"

                        for item in items:
                            price = self._parse_dec(item.get("full_accuracy_value"), allow_e18=False)
                            if price is None:
                                price = self._parse_dec(item.get("value"), allow_e18=False)
                            if price is None:
                                continue

                            ev_sec = self._parse_ts_sec(item, recv_now)
                            lat = max(0.0, (self.clock.server_now() - ev_sec) * 1000.0) if not is_snapshot else 0.0
                            if kind == "spot":
                                self.status["polybolt"]["last_spot"] = str(price.quantize(Decimal("0.01")))
                            else:
                                self.status["polybolt"]["last_twap60"] = str(price.quantize(Decimal("0.01")))
                            self.status["polybolt"].update({
                                "latency_ms": round(lat, 1),
                                "last_msg_ts": recv_now,
                                "last_error": "",
                            })
                            self._record_oracle_price(kind, price, ev_sec, "PolyBolt", is_snapshot)
            except websockets.exceptions.ConnectionClosed as ce:
                self.status["polybolt"]["connected"] = False
                self.status["polybolt"]["authenticated"] = False
                self.status["polybolt"]["errors"] += 1
                code_hint = {4001: "认证被拒(4001)", 4002: "消费过慢(4002)", 4003: "服务端维护(4003)", 4008: "策略限制(4008)"}.get(ce.code, f"断开({ce.code})")
                self.status["polybolt"]["last_error"] = code_hint
                self.status["polybolt"]["note"] = f"PolyBolt {code_hint}，回退RTDS并重连..."
                await asyncio.sleep(3.0)
            except Exception as e:
                self.status["polybolt"]["connected"] = False
                self.status["polybolt"]["errors"] += 1
                err_s = "连接超时" if isinstance(e, asyncio.TimeoutError) else (str(e)[:48] or type(e).__name__)
                self.status["polybolt"]["last_error"] = err_s
                self.status["polybolt"]["note"] = f"重连中: {err_s}"
                await asyncio.sleep(3.0)
