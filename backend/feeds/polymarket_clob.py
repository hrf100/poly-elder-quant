import asyncio
import json
import time
from collections import deque
from typing import Callable, Optional
import aiohttp
import websockets

from backend.config import CLOB_API_BASE, CLOB_WS_URL


class PolymarketClobFeed:
    """
    Polymarket CLOB L2 Orderbook Feed (v12 Zero-Delay & Self-Healing):
    1. Instant token switch: races ws.recv() against _switch_event.wait() -> 0ms switch latency (no 8s hang!).
    2. Official 2026 WS subscription: sends custom_feature_enabled=True & initial_dump=True (enables best_bid_ask).
    3. Strict 10s application-level "PING" heartbeat independent of incoming message flow (prevents silent WS drop).
    4. Full in-memory L2 level map updated by both `book` snapshots AND `price_change` deltas (handles missing best_bid/ask).
    5. Fast REST /book bootstrap & 1.5s retry whenever quotes are 0/0, plus 12s zombie-WS auto-reconnect.
    6. Accurate `live_quotes` status flag so UI never displays a misleading green dot on 0/0 quotes.
    """

    def __init__(self, on_book_update: Optional[Callable] = None):
        self.on_book_update = on_book_update
        self.up_token_id: Optional[str] = None
        self.down_token_id: Optional[str] = None
        self._switch_event = asyncio.Event()
        self._stop = False

        self._bid_maps: dict[str, dict[float, float]] = {"UP": {}, "DOWN": {}}
        self._ask_maps: dict[str, dict[float, float]] = {"UP": {}, "DOWN": {}}
        self._depth_hist: dict[str, deque] = {"UP": deque(maxlen=40), "DOWN": deque(maxlen=40)}
        self.books: dict[str, dict] = {
            "UP": self._empty_book(),
            "DOWN": self._empty_book(),
        }
        self.status = {
            "connected": False,
            "live_quotes": False,
            "book_age_sec": 999.0,
            "last_msg_ts": 0.0,
            "errors": 0,
            "last_error": "",
            "active_tokens": [],
        }

    @staticmethod
    def _empty_book() -> dict:
        return {
            "bids": [],
            "asks": [],
            "best_bid": 0.0,
            "best_ask": 0.0,
            "mid": 0.5,
            "spread": 0.0,
            "imbalance": 0.5,
            "liquidity_pull": 0.0,
            "last_trade": 0.0,
            "last_trade_ts": 0.0,
            "last_trade_side": "",
            "updated_at": 0.0,
        }

    def stop(self):
        self._stop = True
        self._switch_event.set()

    def set_tokens(self, up_token_id: Optional[str], down_token_id: Optional[str]):
        if up_token_id != self.up_token_id or down_token_id != self.down_token_id:
            self.up_token_id = up_token_id
            self.down_token_id = down_token_id
            self.status["active_tokens"] = [t for t in (up_token_id, down_token_id) if t]
            self.status["connected"] = False
            self.status["live_quotes"] = False
            self.status["book_age_sec"] = 999.0
            self._bid_maps = {"UP": {}, "DOWN": {}}
            self._ask_maps = {"UP": {}, "DOWN": {}}
            self._depth_hist = {"UP": deque(maxlen=40), "DOWN": deque(maxlen=40)}
            self.books["UP"] = self._empty_book()
            self.books["DOWN"] = self._empty_book()
            self._switch_event.set()

    def refresh_status_metrics(self) -> dict:
        now = time.time()
        up_ts = float(self.books["UP"].get("updated_at") or 0.0)
        dn_ts = float(self.books["DOWN"].get("updated_at") or 0.0)
        latest = max(up_ts, dn_ts)
        age = round(now - latest, 1) if latest > 0 else 999.0
        has_quotes = (
            (self.books["UP"]["best_bid"] > 0 or self.books["UP"]["best_ask"] > 0)
            and (self.books["DOWN"]["best_bid"] > 0 or self.books["DOWN"]["best_ask"] > 0)
        )
        self.status["book_age_sec"] = age
        self.status["live_quotes"] = bool(has_quotes and age <= 12.0)
        if self.status["live_quotes"]:
            self.status["last_error"] = ""
        return self.status

    def is_book_fresh(self, side: str, max_age_sec: float = 8.0) -> bool:
        b = self.books.get(side)
        if not b:
            return False
        if b["best_bid"] <= 0.0 or b["best_ask"] <= 0.0 or b["best_bid"] > b["best_ask"]:
            return False
        return (time.time() - float(b.get("updated_at") or 0.0)) <= max_age_sec

    def is_parity_ok(self, max_tol: float = 0.08) -> bool:
        ub = self.books["UP"]
        db = self.books["DOWN"]
        if ub["best_bid"] <= 0 or db["best_ask"] <= 0 or db["best_bid"] <= 0 or ub["best_ask"] <= 0:
            return True
        err1 = abs((ub["best_bid"] + db["best_ask"]) - 1.0)
        err2 = abs((db["best_bid"] + ub["best_ask"]) - 1.0)
        return err1 <= max_tol and err2 <= max_tol

    def _side_for_token(self, asset_id: str) -> Optional[str]:
        if asset_id == self.up_token_id:
            return "UP"
        if asset_id == self.down_token_id:
            return "DOWN"
        return None

    def _emit_update(self, side: str, event_type: str):
        self.refresh_status_metrics()
        if self.on_book_update:
            self.on_book_update(side, self.books[side], event_type)

    def _recompute_from_maps(self, side: str, event_type: str):
        parsed_bids = sorted(self._bid_maps[side].items(), key=lambda x: -x[0])
        parsed_asks = sorted(self._ask_maps[side].items(), key=lambda x: x[0])

        best_bid = parsed_bids[0][0] if parsed_bids else 0.0
        best_ask = parsed_asks[0][0] if parsed_asks else 0.0
        if best_bid > 0 and best_ask > 0 and best_bid > best_ask:
            return  # Reject crossed state until next clean snapshot

        mid = round((best_bid + best_ask) / 2.0, 4) if (best_bid > 0 and best_ask > 0) else (best_bid or best_ask or 0.5)
        spread = round(max(0.0, best_ask - best_bid), 4) if (best_bid > 0 and best_ask > 0) else 0.0

        top_bid_vol = sum(sz for _, sz in parsed_bids[:3])
        top_ask_vol = sum(sz for _, sz in parsed_asks[:3])
        denom = top_bid_vol + top_ask_vol
        imbalance = round(top_bid_vol / denom, 4) if denom > 0 else 0.5

        now_ts = time.time()
        hist = self._depth_hist[side]
        hist.append((now_ts, top_bid_vol, top_ask_vol))
        # Compare current top-3 depth vs ~1.5s ago to detect sudden MM liquidity pull
        pull_score = 0.0
        ref_b, ref_a = None, None
        for h_ts, h_b, h_a in hist:
            if now_ts - h_ts <= 2.5:
                ref_b, ref_a = h_b, h_a
                break
        if ref_b is not None and ref_a is not None:
            ask_pull = max(0.0, (ref_a - top_ask_vol) / max(10.0, ref_a)) if ref_a >= 20.0 else 0.0
            bid_pull = max(0.0, (ref_b - top_bid_vol) / max(10.0, ref_b)) if ref_b >= 20.0 else 0.0
            # Positive = asks pulled faster than bids (bullish pressure on this token)
            pull_score = round(max(-1.0, min(1.0, ask_pull - bid_pull)), 3)

        self.books[side].update({
            "bids": parsed_bids[:5],
            "asks": parsed_asks[:5],
            "best_bid": round(best_bid, 4),
            "best_ask": round(best_ask, 4),
            "mid": mid,
            "spread": spread,
            "imbalance": imbalance,
            "liquidity_pull": pull_score,
            "updated_at": now_ts,
        })
        self._emit_update(side, event_type)

    def _apply_levels(self, side: str, bids: list, asks: list, event_type: str = "book"):
        new_bids: dict[float, float] = {}
        new_asks: dict[float, float] = {}
        for b in bids or []:
            try:
                p, sz = float(b["price"]), float(b["size"])
                if 0.0 < p < 1.0 and sz > 0:
                    new_bids[round(p, 4)] = sz
            except (KeyError, TypeError, ValueError):
                continue
        for a in asks or []:
            try:
                p, sz = float(a["price"]), float(a["size"])
                if 0.0 < p < 1.0 and sz > 0:
                    new_asks[round(p, 4)] = sz
            except (KeyError, TypeError, ValueError):
                continue

        # Do not wipe an existing live book with a transient empty payload
        if not new_bids and not new_asks:
            return
        self._bid_maps[side] = new_bids
        self._ask_maps[side] = new_asks
        self._recompute_from_maps(side, event_type)

    def _apply_price_change(self, ch: dict, now: float):
        s2 = self._side_for_token(str(ch.get("asset_id", "")))
        if not s2:
            return

        # 1. Apply delta to in-memory L2 map if price/size/side are present
        raw_p = ch.get("price")
        raw_sz = ch.get("size")
        raw_side = str(ch.get("side") or "").strip().upper()
        map_updated = False
        if raw_p not in (None, "") and raw_sz not in (None, "") and raw_side in ("BUY", "SELL", "BID", "ASK"):
            try:
                p_f = round(float(raw_p), 4)
                sz_f = float(raw_sz)
                if 0.0 < p_f < 1.0:
                    target_map = self._bid_maps[s2] if raw_side in ("BUY", "BID") else self._ask_maps[s2]
                    if sz_f <= 0:
                        target_map.pop(p_f, None)
                    else:
                        target_map[p_f] = sz_f
                    map_updated = True
            except (TypeError, ValueError):
                pass

        # 2. Prefer explicit top-of-book best_bid/best_ask when provided by Polymarket
        bb = ch.get("best_bid")
        ba = ch.get("best_ask")
        if bb not in (None, "") and ba not in (None, ""):
            try:
                b_f, a_f = float(bb), float(ba)
                if 0.0 < b_f < 1.0 and 0.0 < a_f < 1.0 and b_f <= a_f:
                    self.books[s2]["best_bid"] = round(b_f, 4)
                    self.books[s2]["best_ask"] = round(a_f, 4)
                    self.books[s2]["mid"] = round((b_f + a_f) / 2.0, 4)
                    self.books[s2]["spread"] = round(max(0.0, a_f - b_f), 4)
                    self.books[s2]["updated_at"] = now
                    self._emit_update(s2, "price_change")
                    return
            except (TypeError, ValueError):
                pass

        # 3. Fallback: recompute best_bid/best_ask from maintained L2 level maps
        if map_updated:
            self._recompute_from_maps(s2, "price_change")

    async def fetch_rest_snapshot(self) -> bool:
        """Bootstrap or resync real L2 orderbooks via Polymarket CLOB REST API concurrently."""
        up_tid, dn_tid = self.up_token_id, self.down_token_id
        if not up_tid or not dn_tid:
            return False

        ok_any = False
        async with aiohttp.ClientSession() as session:
            async def _fetch_one(side: str, tid: str):
                nonlocal ok_any
                try:
                    async with session.get(
                        f"{CLOB_API_BASE}/book",
                        params={"token_id": tid},
                        timeout=aiohttp.ClientTimeout(total=3.5),
                    ) as r:
                        if r.status == 200:
                            d = await r.json()
                            # Guard against race if token switched during HTTP request
                            if (side == "UP" and tid == self.up_token_id) or (side == "DOWN" and tid == self.down_token_id):
                                self._apply_levels(side, d.get("bids", []), d.get("asks", []), event_type="book")
                                if self.books[side]["best_bid"] > 0 and self.books[side]["best_ask"] > 0:
                                    ok_any = True
                except Exception:
                    pass

            await asyncio.gather(_fetch_one("UP", up_tid), _fetch_one("DOWN", dn_tid))
        return ok_any

    async def _interruptible_sleep(self, seconds: float):
        try:
            await asyncio.wait_for(self._switch_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def run(self):
        while not self._stop:
            self._switch_event.clear()
            up_tid, dn_tid = self.up_token_id, self.down_token_id
            if not up_tid or not dn_tid:
                self.status["connected"] = False
                self.status["live_quotes"] = False
                await self._interruptible_sleep(0.5)
                continue

            # Immediate concurrent REST bootstrap (don't block WS connect if REST is slow)
            await self.fetch_rest_snapshot()
            if self._switch_event.is_set() or self._stop:
                continue

            tokens = [up_tid, dn_tid]
            try:
                async with websockets.connect(CLOB_WS_URL, ping_interval=None, close_timeout=4) as ws:
                    # Official 2026 subscription with custom_feature_enabled=True (enables best_bid_ask)
                    await ws.send(json.dumps({
                        "assets_ids": tokens,
                        "type": "market",
                        "initial_dump": True,
                        "custom_feature_enabled": True,
                    }))
                    self.status["connected"] = True
                    last_ping_ts = time.time()
                    last_rest_retry_ts = time.time()
                    connected_at = time.time()

                    while not self._stop and not self._switch_event.is_set():
                        now = time.time()

                        # 1. Strict 10s application-level PING required by Polymarket CLOB WS
                        if now - last_ping_ts >= 10.0:
                            await ws.send("PING")
                            last_ping_ts = now

                        # 2. Fast REST self-healing if quotes are still 0/0 after connect (every 1.5s)
                        self.refresh_status_metrics()
                        if not self.status["live_quotes"]:
                            retry_gap = 1.5 if (now - connected_at) < 15.0 else 4.0
                            if now - last_rest_retry_ts >= retry_gap:
                                last_rest_retry_ts = now
                                await self.fetch_rest_snapshot()
                                if self._switch_event.is_set() or self._stop:
                                    break

                        # 3. Silent / zombie WebSocket watchdog: if connected > 15s and book stale > 15s -> reconnect!
                        if (now - connected_at) >= 15.0 and self.status["book_age_sec"] >= 15.0:
                            await self.fetch_rest_snapshot()
                            if self.status["book_age_sec"] >= 15.0:
                                raise RuntimeError("CLOB WS 静默超时，主动重连")

                        # 4. Race ws.recv() against _switch_event so token switch wakes up in 0ms!
                        recv_task = asyncio.create_task(ws.recv())
                        switch_task = asyncio.create_task(self._switch_event.wait())
                        done, pending = await asyncio.wait(
                            {recv_task, switch_task},
                            timeout=1.5,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        for p in pending:
                            p.cancel()
                        if pending:
                            await asyncio.gather(*pending, return_exceptions=True)

                        if switch_task in done or self._switch_event.is_set():
                            break
                        if recv_task not in done:
                            continue

                        raw = recv_task.result()
                        if not raw or raw in ("PONG", "pong"):
                            continue

                        now = time.time()
                        self.status["last_msg_ts"] = now
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        events = data if isinstance(data, list) else [data]
                        for ev in events:
                            if not isinstance(ev, dict):
                                continue
                            ev_type = ev.get("event_type")
                            asset_id = str(ev.get("asset_id", ""))
                            side = self._side_for_token(asset_id)

                            if ev_type == "book" and side:
                                self._apply_levels(side, ev.get("bids", []), ev.get("asks", []), event_type="book")

                            elif ev_type == "price_change":
                                changes = ev.get("price_changes")
                                if isinstance(changes, list):
                                    for ch in changes:
                                        if isinstance(ch, dict):
                                            self._apply_price_change(ch, now)
                                else:
                                    self._apply_price_change(ev, now)

                            elif ev_type == "best_bid_ask" and side:
                                bb = ev.get("best_bid")
                                ba = ev.get("best_ask")
                                if bb not in (None, "") and ba not in (None, ""):
                                    try:
                                        b_f, a_f = float(bb), float(ba)
                                        if 0.0 < b_f < 1.0 and 0.0 < a_f < 1.0 and b_f <= a_f:
                                            self.books[side]["best_bid"] = round(b_f, 4)
                                            self.books[side]["best_ask"] = round(a_f, 4)
                                            self.books[side]["mid"] = round((b_f + a_f) / 2.0, 4)
                                            self.books[side]["spread"] = round(max(0.0, a_f - b_f), 4)
                                            self.books[side]["updated_at"] = now
                                            self._emit_update(side, "price_change")
                                    except (TypeError, ValueError):
                                        pass

                            elif ev_type == "last_trade_price" and side:
                                try:
                                    p = float(ev.get("price", 0))
                                except (TypeError, ValueError):
                                    p = 0.0
                                if 0.0 < p < 1.0:
                                    trade_side = str(ev.get("side") or "").strip().upper()
                                    self.books[side]["last_trade"] = round(p, 4)
                                    self.books[side]["last_trade_ts"] = now
                                    self.books[side]["last_trade_side"] = trade_side
                                    # Do NOT blindly stamp updated_at if best_bid/ask are still 0/0!
                                    if self.books[side]["best_bid"] > 0 and self.books[side]["best_ask"] > 0:
                                        self.books[side]["updated_at"] = now
                                    self._emit_update(side, "trade")
            except Exception as e:
                self.status["connected"] = False
                self.status["live_quotes"] = False
                self.status["errors"] += 1
                msg = str(e).strip().replace("\n", " ")[:48]
                if "429" in msg:
                    self.status["last_error"] = "HTTP 429 盘口限流"
                elif "403" in msg or "451" in msg:
                    self.status["last_error"] = "盘口接口访问受限"
                elif msg:
                    self.status["last_error"] = msg
                else:
                    self.status["last_error"] = f"盘口连接异常({type(e).__name__})"
                await self._interruptible_sleep(1.5)
