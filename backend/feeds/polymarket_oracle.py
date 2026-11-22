import asyncio
import json
import time
from decimal import Decimal, InvalidOperation
from typing import Callable, Optional
import aiohttp
import websockets

from backend.config import CLOB_API_BASE, GAMMA_API_BASE, POLYBOLT_WS_URL, RTDS_WS_URL


class PolymarketOracleSync:
    """
    100% Real Polymarket Official Truth Source (Dual-Stream Auto-Failover):
    1. Public Polymarket RTDS (wss://ws-live-data.polymarket.com):
       - Zero credentials required! Connects immediately on startup.
       - Subscribes with exact compact JSON filters to `crypto_prices_chainlink` & `crypto_prices_twap_sixty`.
       - Converts E18 fixed-point `full_accuracy_value` to exact Decimal.
    2. Authenticated Polymarket PolyBolt (wss://ws-live-v2.polymarket.com/ws):
       - Automatically activates as primary source when CLOB API credentials are configured.
       - Uses official nested auth frame, waits for ack, then subscribes to `price.crypto` & `price.crypto.twap` (60s).
    3. Gamma API (gamma-api.polymarket.com):
       - Discovers real 5m BTC rounds (`btc-updown-5m-{ts}`), locks official `priceToBeat` / `finalPrice`,
         and confirms official settlement outcome (`UP` / `DOWN`).
    """

    def __init__(self, on_oracle_tick: Optional[Callable] = None):
        self.on_oracle_tick = on_oracle_tick
        self.creds: Optional[dict] = None
        self.latest_spot = Decimal("0")
        self.latest_twap60 = Decimal("0")
        self.active_oracle_source: str = "NONE"
        self.twap_history: list[tuple[float, Decimal]] = []
        self._stop = False
        self._reconnect_polybolt = asyncio.Event()
        self._http_session: Optional[aiohttp.ClientSession] = None

        self.status = {
            "polybolt": {
                "connected": False,
                "authenticated": False,
                "last_spot": "0",
                "last_twap60": "0",
                "latency_ms": 0.0,
                "last_msg_ts": 0.0,
                "errors": 0,
                "last_error": "",
                "note": "未配置API凭证(已自动启用公开RTDS真实预言机流)",
            },
            "rtds": {
                "connected": False,
                "last_spot": "0",
                "last_twap60": "0",
                "latency_ms": 0.0,
                "last_msg_ts": 0.0,
                "errors": 0,
                "last_error": "",
                "note": "正在连接 Polymarket 公开 Chainlink 60s TWAP 流...",
            },
            "gamma": {"connected": False, "latency_ms": 0.0, "last_msg_ts": 0.0, "errors": 0, "last_error": ""},
            "geo_check": {"checked": False, "blocked": False, "country": "UNKNOWN", "clock_drift_ms": 0.0},
        }

    def set_credentials(self, api_key: str, secret: str, passphrase: str):
        if api_key and secret and passphrase:
            self.creds = {
                "apiKey": api_key,
                "secret": secret,
                "passphrase": passphrase,
            }
            self.status["polybolt"]["note"] = "凭证已加载，正在认证 PolyBolt..."
            self._reconnect_polybolt.set()
        else:
            self.creds = None
            self.status["polybolt"]["note"] = "未配置API凭证(使用公开RTDS真实预言机流)"

    def stop(self):
        self._stop = True
        self._reconnect_polybolt.set()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            connector = aiohttp.TCPConnector(limit=16, ttl_dns_cache=300, enable_cleanup_closed=True)
            self._http_session = aiohttp.ClientSession(
                connector=connector,
                headers={
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Pragma": "no-cache",
                },
            )
        return self._http_session

    async def close(self):
        if self._http_session and not self._http_session.closed:
            try:
                await self._http_session.close()
            except Exception:
                pass
            self._http_session = None

    @staticmethod
    def get_window_ts(now: Optional[float] = None) -> int:
        ts = int(now or time.time())
        return (ts // 300) * 300

    def get_twap_at_boundary_detail(self, target_ts: int, max_delta_sec: float = 15.0) -> tuple[Optional[Decimal], float]:
        """
        Find closest real Polymarket 60s TWAP tick around exact window boundary timestamp.
        On ties or near-ties, slightly prefers the tick at/just after target_ts (matching official boundary).
        Returns (price, abs_delta_sec).
        """
        if not self.twap_history:
            return None, 999.0
        best_score = max_delta_sec + 10.0
        best_dt = 999.0
        best_px = None
        for ts, px in self.twap_history:
            dt = abs(ts - target_ts)
            if dt <= max_delta_sec:
                # Slight preference (0.15s) for tick >= target_ts over tick < target_ts
                score = dt + (0.15 if ts < target_ts else 0.0)
                if score < best_score:
                    best_score = score
                    best_dt = dt
                    best_px = px
        return best_px, best_dt

    def get_twap_at_boundary(self, target_ts: int, max_delta_sec: float = 15.0) -> Optional[Decimal]:
        px, _ = self.get_twap_at_boundary_detail(target_ts, max_delta_sec=max_delta_sec)
        return px

    @staticmethod
    def _parse_dec(val, allow_e18: bool = False) -> Optional[Decimal]:
        if val is None or str(val).strip() == "":
            return None
        try:
            d = Decimal(str(val))
            if allow_e18 and d > Decimal("1000000000000"):
                d = d / Decimal("1000000000000000000")
            return d if d > 0 else None
        except InvalidOperation:
            return None

    @staticmethod
    def _parse_ts_sec(item: dict, default_ts: float) -> float:
        ev_ts = item.get("timestamp") or item.get("ts") or item.get("t")
        if ev_ts is None:
            return default_ts
        try:
            v = float(ev_ts)
            if v > 1e14:
                return v / 1e6
            if v > 1e11:
                return v / 1000.0
            return v
        except Exception:
            return default_ts

    def _record_oracle_price(self, kind: str, price: Decimal, ev_sec: float, source_name: str, is_snapshot: bool):
        """Update unified real oracle state; prioritize PolyBolt if fresh, otherwise use RTDS seamlessly."""
        now = time.time()
        polybolt_fresh = (
            self.status["polybolt"]["connected"]
            and (now - float(self.status["polybolt"]["last_msg_ts"])) < 12.0
        )
        # If PolyBolt is live and fresh, ignore RTDS updates so we don't flip-flop between streams
        if source_name == "RTDS" and polybolt_fresh:
            return

        self.active_oracle_source = source_name
        if kind == "spot":
            self.latest_spot = price
        elif kind == "twap60":
            self.latest_twap60 = price
            self.twap_history.append((ev_sec, price))
            if len(self.twap_history) > 1000:
                self.twap_history = self.twap_history[-700:]

        if self.on_oracle_tick:
            self.on_oracle_tick(kind, price, ev_sec, is_snapshot, source_name)

    async def check_clock_drift(self) -> Optional[float]:
        """Lightweight RTT-compensated UTC clock drift check against Binance /api/v3/time."""
        try:
            session = await self._get_session()
            if not session:
                return None
            t0 = time.time() * 1000.0
            async with session.get("https://api.binance.com/api/v3/time", timeout=aiohttp.ClientTimeout(total=5)) as resp2:
                t1 = time.time() * 1000.0
                if resp2.status == 200:
                    d2 = await resp2.json()
                    srv_ms = float(d2.get("serverTime", t1))
                    local_mid_ms = 0.5 * (t0 + t1)
                    drift = round(local_mid_ms - srv_ms, 1)
                    self.status["geo_check"]["clock_drift_ms"] = drift
                    self.status["geo_check"]["clock_checked_at"] = round(t1 / 1000.0, 1)
                    return drift
        except Exception:
            pass
        return None

    async def check_geo_and_clock(self):
        try:
            session = await self._get_session()
            if session:
                async with session.get("https://polymarket.com/api/geoblock", timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.status["geo_check"].update({
                            "checked": True,
                            "blocked": bool(data.get("blocked", False)),
                            "country": str(data.get("country", "UNKNOWN")),
                        })
        except Exception:
            pass
        await self.check_clock_drift()

    async def fetch_gamma_round(self, window_ts: int, bust_cache: bool = False) -> Optional[dict]:
        """Fetch real Polymarket 5m BTC event metadata, priceToBeat, finalPrice, token IDs & official outcome."""
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
                async with session.get(
                    f"{GAMMA_API_BASE}/events",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
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

            raw_tokens = market.get("clobTokenIds")
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

            # Explicitly map clobTokenIds by matching outcomes label ("UP" / "DOWN")
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
            }
        except Exception as e:
            self.status["gamma"]["connected"] = False
            self.status["gamma"]["errors"] += 1
            self.status["gamma"]["last_error"] = "接口超时" if isinstance(e, asyncio.TimeoutError) else (str(e)[:48] or type(e).__name__)
            return None

    async def fetch_clob_winner(self, condition_id: Optional[str]) -> Optional[str]:
        """Fast official confirmation via Polymarket CLOB /markets/{condition_id} tokens[].winner (often faster than Gamma CDN)."""
        if not condition_id:
            return None
        try:
            session = await self._get_session()
            async with session.get(
                f"{CLOB_API_BASE}/markets/{condition_id}",
                params={"_t": str(int(time.time() * 1000))},
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
        """
        v13 Accelerated Official Settlement Waiter:
        - Queries Gamma API (cache-busted) AND CLOB /markets/{condition_id} IN PARALLEL using persistent HTTP keepalive.
        - Polls every 2.0s during first 60s, then 3.5s.
        - Strictly NEVER guesses or defaults to DOWN on timeout; returns verified=False if unresolved.
        """
        start_ts = time.time()
        deadline = start_ts + timeout_sec
        boundary_twap = self.get_twap_at_boundary(window_ts + 300, max_delta_sec=10.0)
        settlement_price = boundary_twap if (boundary_twap and boundary_twap > 0) else fallback_twap
        official_ptb = fallback_ptb
        condition_id = known_condition_id

        while time.time() < deadline and not self._stop:
            tasks = [
                self.fetch_gamma_round(window_ts, bust_cache=True),
                self.fetch_gamma_round(window_ts + 300, bust_cache=True),
            ]
            if condition_id:
                tasks.append(self.fetch_clob_winner(condition_id))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            cur_round = results[0] if isinstance(results[0], dict) else None
            next_round = results[1] if isinstance(results[1], dict) else None
            clob_winner = results[2] if (len(results) > 2 and isinstance(results[2], str)) else None

            if cur_round:
                if not condition_id and cur_round.get("condition_id"):
                    condition_id = cur_round["condition_id"]
                    if not clob_winner:
                        clob_winner = await self.fetch_clob_winner(condition_id)
                if cur_round["price_to_beat"] is not None:
                    official_ptb = cur_round["price_to_beat"]
                if cur_round["final_price"] is not None:
                    settlement_price = cur_round["final_price"]

            if next_round and next_round["price_to_beat"] is not None and settlement_price <= 0:
                settlement_price = next_round["price_to_beat"]

            official_outcome = clob_winner or (cur_round["resolved_outcome"] if cur_round else None)

            if official_outcome in ("UP", "DOWN"):
                # Only wait briefly if we have NO settlement price at all
                if settlement_price <= 0 and time.time() + 10 < deadline:
                    await asyncio.sleep(2.0)
                    retry = await self.fetch_gamma_round(window_ts, bust_cache=True)
                    if retry and retry["final_price"] is not None:
                        settlement_price = retry["final_price"]
                return {
                    "window_ts": window_ts,
                    "price_to_beat": str(official_ptb.quantize(Decimal("0.01"))) if official_ptb is not None else None,
                    "settlement_price": str(settlement_price.quantize(Decimal("0.01"))) if settlement_price > 0 else "--",
                    "official_outcome": official_outcome,
                    "verified": True,
                }

            sleep_sec = 2.0 if (time.time() - start_ts) < 60.0 else 3.5
            await asyncio.sleep(sleep_sec)

        return {
            "window_ts": window_ts,
            "price_to_beat": str(official_ptb.quantize(Decimal("0.01"))) if official_ptb is not None else None,
            "settlement_price": str(settlement_price.quantize(Decimal("0.01"))) if settlement_price > 0 else "--",
            "official_outcome": None,
            "verified": False,
        }

    def _extract_items(self, msg: dict) -> list[dict]:
        """Extract list of price items from both snapshot (payload.data) and live tick (payload) envelopes."""
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
        """
        Public Polymarket RTDS WebSocket (wss://ws-live-data.polymarket.com).
        Requires NO wallet and NO API key — delivers real Chainlink btc/usd spot & 60s TWAP immediately!
        CRITICAL: filters MUST be compact JSON strings with NO spaces, and client MUST send PING every 5s.
        """
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
                async with websockets.connect(RTDS_WS_URL, ping_interval=None, close_timeout=5) as ws:
                    await ws.send(sub_frame)
                    self.status["rtds"].update({
                        "connected": True,
                        "note": "已直连 Polymarket 公开 Chainlink 60s TWAP 真实流",
                    })
                    last_ping = time.time()

                    while not self._stop:
                        now = time.time()
                        if now - last_ping >= 5.0:
                            await ws.send("PING")
                            last_ping = now
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        except asyncio.TimeoutError:
                            continue

                        if not raw or raw in ("PONG", "pong"):
                            continue
                        try:
                            msg = json.loads(raw)
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
                            # RTDS full_accuracy_value is E18 integer string; fallback to value
                            price = self._parse_dec(item.get("full_accuracy_value"), allow_e18=True)
                            if price is None:
                                price = self._parse_dec(item.get("value"), allow_e18=False)
                            if price is None:
                                continue

                            ev_sec = self._parse_ts_sec(item, recv_now)
                            lat = max(0.0, (recv_now - ev_sec) * 1000.0) if not is_snapshot else 0.0
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
            except Exception as e:
                self.status["rtds"]["connected"] = False
                self.status["rtds"]["errors"] += 1
                err_s = "连接超时" if isinstance(e, asyncio.TimeoutError) else (str(e)[:48] or type(e).__name__)
                self.status["rtds"]["last_error"] = err_s
                self.status["rtds"]["note"] = f"RTDS 重连中: {err_s}"
                await asyncio.sleep(2.5)

    async def run_polybolt(self):
        """Authenticated Polymarket PolyBolt WS (primary oracle whenever CLOB API credentials are configured)."""
        while not self._stop:
            self._reconnect_polybolt.clear()
            if not self.creds:
                self.status["polybolt"]["connected"] = False
                self.status["polybolt"]["authenticated"] = False
                await asyncio.sleep(2.0)
                continue

            try:
                async with websockets.connect(POLYBOLT_WS_URL, ping_interval=None, close_timeout=5) as ws:
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
                        msg = json.loads(raw)
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
                            lat = max(0.0, (recv_now - ev_sec) * 1000.0) if not is_snapshot else 0.0
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
