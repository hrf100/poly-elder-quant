import asyncio
import heapq
import json
import math
import time
from collections import deque
from typing import Callable, Optional
import aiohttp
import websockets

from backend.config import CLOB_API_BASE, CLOB_WS_URL, POLY_HEADERS, POLY_PROXY, ws_headers_kwarg
from backend.fast_json import loads as fast_loads
from backend.feeds.clock import ServerClock


class PolymarketClobFeed:
    """
    Hardened Real-Time L2 Orderbook Feed for Polymarket 5m BTC UP & DOWN tokens (v37):
    - Zero-gap incremental WS subscription + 90s next-round pre-subscription (no WS disconnect on rollover).
    - `best_bid=0` / `best_ask=1` empty-side normalization (never drops one-sided books).
    - `verify_top` ghost-level pruning against authoritative exchange BBO (clean imbalance & pull signals).
    - Monotonic server timestamp guard (`is_stale`) preventing out-of-order overwrites.
    - Dynamic `tick_size_change` tracking + website-parity `display_prob`.
    - Socket backlog detector (>5s latency for 3 consecutive ticks => clean reconnect with 20s cooldown).
    - Pre-order REST fresh-book check (`verify_fresh_book_rest`) for LIVE execution safety.
    """

    BACKLOG_DELAY_MS = 5000.0
    BACKLOG_CONSEC_TICKS = 3
    BACKLOG_RECONNECT_COOLDOWN = 20.0

    def __init__(
        self,
        on_book_update: Optional[Callable[[str, dict, str], None]] = None,
        clock: Optional[ServerClock] = None,
    ):
        self.on_book_update = on_book_update
        self.clock = clock or ServerClock()
        self.up_token_id: Optional[str] = None
        self.down_token_id: Optional[str] = None
        self.presub_up_token_id: Optional[str] = None
        self.presub_down_token_id: Optional[str] = None

        self._sub_change_event = asyncio.Event()
        self._stop = False
        self._http_session: Optional[aiohttp.ClientSession] = None

        self._bid_maps: dict[str, dict[float, float]] = {"UP": {}, "DOWN": {}}
        self._ask_maps: dict[str, dict[float, float]] = {"UP": {}, "DOWN": {}}
        self._depth_hist: dict[str, deque] = {"UP": deque(maxlen=40), "DOWN": deque(maxlen=40)}

        self._pre_bid_maps: dict[str, dict[float, float]] = {}
        self._pre_ask_maps: dict[str, dict[float, float]] = {}
        self._last_token_ts: dict[str, int] = {}
        self._tick_sizes: dict[str, float] = {}

        self._backlog_ticks: int = 0
        self._last_backlog_reconnect_ts: float = 0.0

        self.books: dict[str, dict] = {
            "UP": self._empty_book(),
            "DOWN": self._empty_book(),
        }
        self.status = {
            "connected": False,
            "live_quotes": False,
            "book_age_sec": 999.0,
            "last_msg_ts": 0.0,
            "last_delay_ms": None,
            "mismatches": 0,
            "tick_size": 0.01,
            "presubscribed": False,
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
        self._sub_change_event.set()

    async def close(self):
        if self._http_session and not self._http_session.closed:
            try:
                await self._http_session.close()
            except Exception:
                pass
            self._http_session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            connector = aiohttp.TCPConnector(limit=8, ttl_dns_cache=300)
            self._http_session = aiohttp.ClientSession(connector=connector, headers=POLY_HEADERS)
        return self._http_session

    def _wanted_tokens(self) -> set[str]:
        return {
            t for t in (self.up_token_id, self.down_token_id, self.presub_up_token_id, self.presub_down_token_id)
            if t
        }

    def set_tick_size(self, token_id: Optional[str], tick: float):
        if token_id and tick > 0:
            t_val = round(float(tick), 4)
            self._tick_sizes[str(token_id)] = t_val
            if token_id in (self.up_token_id, self.down_token_id):
                self.status["tick_size"] = t_val

    def get_tick_size(self, side: str = "UP") -> float:
        tid = self.up_token_id if side == "UP" else self.down_token_id
        if tid and tid in self._tick_sizes:
            return self._tick_sizes[tid]
        return float(self.status.get("tick_size") or 0.01)

    def round_to_tick(self, price: float, side: str = "UP", mode: str = "nearest") -> float:
        tick = max(0.0001, self.get_tick_size(side))
        ratio = float(price) / tick
        if mode == "floor":
            steps = math.floor(ratio + 1e-9)
        elif mode == "ceil":
            steps = math.ceil(ratio - 1e-9)
        else:
            steps = round(ratio)
        px = round(steps * tick, 4)
        return max(tick, min(round(1.0 - tick, 4), px))

    def set_presubscribe_tokens(self, up_token_id: Optional[str], down_token_id: Optional[str]):
        if up_token_id == self.presub_up_token_id and down_token_id == self.presub_down_token_id:
            return
        self.presub_up_token_id = up_token_id
        self.presub_down_token_id = down_token_id
        for tid in (up_token_id, down_token_id):
            if tid:
                self._pre_bid_maps.setdefault(tid, {})
                self._pre_ask_maps.setdefault(tid, {})
        self.status["presubscribed"] = bool(up_token_id and down_token_id)
        self._sub_change_event.set()

    def set_tokens(self, up_token_id: Optional[str], down_token_id: Optional[str], tick_size: Optional[float] = None):
        if tick_size and tick_size > 0:
            if up_token_id:
                self._tick_sizes[up_token_id] = round(float(tick_size), 4)
            if down_token_id:
                self._tick_sizes[down_token_id] = round(float(tick_size), 4)
            self.status["tick_size"] = round(float(tick_size), 4)

        if up_token_id == self.up_token_id and down_token_id == self.down_token_id:
            return

        self.up_token_id = up_token_id
        self.down_token_id = down_token_id
        self.status["active_tokens"] = [t for t in (up_token_id, down_token_id) if t]

        if up_token_id and up_token_id == self.presub_up_token_id:
            self.presub_up_token_id = None
        if down_token_id and down_token_id == self.presub_down_token_id:
            self.presub_down_token_id = None
        self.status["presubscribed"] = bool(self.presub_up_token_id and self.presub_down_token_id)

        promoted_up_b = self._pre_bid_maps.pop(up_token_id, {}) if up_token_id else {}
        promoted_up_a = self._pre_ask_maps.pop(up_token_id, {}) if up_token_id else {}
        promoted_dn_b = self._pre_bid_maps.pop(down_token_id, {}) if down_token_id else {}
        promoted_dn_a = self._pre_ask_maps.pop(down_token_id, {}) if down_token_id else {}

        self._bid_maps = {"UP": promoted_up_b, "DOWN": promoted_dn_b}
        self._ask_maps = {"UP": promoted_up_a, "DOWN": promoted_dn_a}
        self._depth_hist = {"UP": deque(maxlen=40), "DOWN": deque(maxlen=40)}
        self.books["UP"] = self._empty_book()
        self.books["DOWN"] = self._empty_book()

        keep = self._wanted_tokens()
        for k in list(self._pre_bid_maps.keys()):
            if k not in keep:
                self._pre_bid_maps.pop(k, None)
                self._pre_ask_maps.pop(k, None)
        for k in list(self._last_token_ts.keys()):
            if k not in keep:
                self._last_token_ts.pop(k, None)

        now = time.time()
        if promoted_up_b or promoted_up_a:
            self._recompute_from_maps("UP", "book", now_ts=now)
        if promoted_dn_b or promoted_dn_a:
            self._recompute_from_maps("DOWN", "book", now_ts=now)

        self.refresh_status_metrics()
        self._sub_change_event.set()

    def refresh_status_metrics(self) -> dict:
        now = time.time()
        up_ts = float(self.books["UP"].get("updated_at") or 0.0)
        dn_ts = float(self.books["DOWN"].get("updated_at") or 0.0)
        latest_book_ts = max(up_ts, dn_ts)
        age = round(now - latest_book_ts, 1) if latest_book_ts > 0 else 999.0
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

    def get_display_prob(self, side: str = "UP") -> Optional[float]:
        """Polymarket official display rule: spread <= 0.10 -> mid; else last trade; else 1 - opp last trade."""
        bk = self.books.get(side)
        opp = self.books.get("DOWN" if side == "UP" else "UP")
        if not bk:
            return None
        bb, ba = float(bk["best_bid"]), float(bk["best_ask"])
        if bb > 0.0 and ba > 0.0 and (ba - bb) <= 0.10 + 1e-9:
            return round((bb + ba) * 0.5, 4)
        lt = float(bk.get("last_trade") or 0.0)
        if 0.0 < lt < 1.0:
            return round(lt, 4)
        if opp:
            olt = float(opp.get("last_trade") or 0.0)
            if 0.0 < olt < 1.0:
                return round(1.0 - olt, 4)
        return round(float(bk.get("mid") or 0.5), 4) if (bb > 0.0 or ba > 0.0) else None

    def _side_for_token(self, asset_id: str) -> Optional[str]:
        if asset_id == self.up_token_id:
            return "UP"
        if asset_id == self.down_token_id:
            return "DOWN"
        return None

    def _is_presub_token(self, asset_id: str) -> bool:
        return bool(asset_id) and asset_id in (self.presub_up_token_id, self.presub_down_token_id)

    @staticmethod
    def _parse_ev_ts_ms(ev: dict) -> int:
        raw = ev.get("timestamp")
        if raw in (None, ""):
            return 0
        try:
            v = float(raw)
            if v <= 0:
                return 0
            return int(v if v > 1e12 else v * 1000.0)
        except (TypeError, ValueError):
            return 0

    def _is_stale(self, asset_id: str, ts_ms: int) -> bool:
        if ts_ms <= 0 or not asset_id:
            return False
        prev = self._last_token_ts.get(asset_id, 0)
        if ts_ms < prev:
            return True
        self._last_token_ts[asset_id] = ts_ms
        return False

    @staticmethod
    def _normalize_bbo(raw_bid, raw_ask) -> tuple[Optional[float], Optional[float], bool]:
        """Polymarket encodes empty bid as 0 and empty ask as 0 or 1."""
        try:
            b_f = float(raw_bid) if raw_bid not in (None, "") else 0.0
            a_f = float(raw_ask) if raw_ask not in (None, "") else 0.0
        except (TypeError, ValueError):
            return None, None, False
        if b_f < 0.0 or b_f >= 1.0 or a_f < 0.0 or a_f > 1.0:
            return None, None, False
        exp_bid = round(b_f, 4) if b_f > 0.0 else None
        exp_ask = round(a_f, 4) if (0.0 < a_f < 1.0) else None
        if exp_bid is not None and exp_ask is not None and exp_bid > exp_ask:
            return None, None, False
        return exp_bid, exp_ask, True

    def _verify_top_maps(self, bmap: dict[float, float], amap: dict[float, float], exp_bid: Optional[float], exp_ask: Optional[float]) -> bool:
        """Prune ghost levels strictly better than authoritative exchange BBO."""
        if exp_bid is None:
            bmap.clear()
        else:
            for p in [p for p in bmap if p > exp_bid + 1e-9]:
                del bmap[p]

        if exp_ask is None:
            amap.clear()
        else:
            for p in [p for p in amap if p < exp_ask - 1e-9]:
                del amap[p]

        top_b = max(bmap) if bmap else None
        top_a = min(amap) if amap else None
        ok_b = (top_b is None and exp_bid is None) or (top_b is not None and exp_bid is not None and abs(top_b - exp_bid) <= 1e-6)
        ok_a = (top_a is None and exp_ask is None) or (top_a is not None and exp_ask is not None and abs(top_a - exp_ask) <= 1e-6)
        return ok_b and ok_a

    def _emit_update(self, side: str, event_type: str):
        if self.on_book_update:
            self.on_book_update(side, self.books[side], event_type)

    def _recompute_from_maps(
        self,
        side: str,
        event_type: str,
        now_ts: Optional[float] = None,
        override_bid: Optional[float] = None,
        override_ask: Optional[float] = None,
        has_override: bool = False,
    ):
        bmap = self._bid_maps[side]
        amap = self._ask_maps[side]
        top_bids = heapq.nlargest(5, bmap.items(), key=lambda x: x[0]) if bmap else []
        top_asks = heapq.nsmallest(5, amap.items(), key=lambda x: x[0]) if amap else []

        if has_override:
            best_bid = override_bid if override_bid is not None else 0.0
            best_ask = override_ask if override_ask is not None else 0.0
        else:
            best_bid = top_bids[0][0] if top_bids else 0.0
            best_ask = top_asks[0][0] if top_asks else 0.0

        if best_bid > 0.0 and best_ask > 0.0 and best_bid > best_ask:
            return

        bk = self.books[side]
        if best_bid > 0.0 and best_ask > 0.0:
            mid = round((best_bid + best_ask) * 0.5, 4)
            spread = round(max(0.0, best_ask - best_bid), 4)
        elif best_bid > 0.0:
            mid = round(best_bid, 4)
            spread = 0.0
        elif best_ask > 0.0:
            mid = round(best_ask, 4)
            spread = 0.0
        elif bk["last_trade"] > 0.0:
            mid = round(bk["last_trade"], 4)
            spread = 0.0
        else:
            mid = bk["mid"]
            spread = 0.0

        top_bid_vol = sum(sz for _, sz in top_bids[:3])
        top_ask_vol = sum(sz for _, sz in top_asks[:3])
        denom = top_bid_vol + top_ask_vol
        imbalance = round(top_bid_vol / denom, 4) if denom > 0.0 else 0.5

        t = now_ts if now_ts is not None else time.time()
        hist = self._depth_hist[side]
        if not hist or (t - hist[-1][0]) >= 0.15:
            hist.append((t, top_bid_vol, top_ask_vol))

        pull_score = 0.0
        ref_b = ref_a = None
        for h_ts, h_b, h_a in hist:
            if t - h_ts <= 2.5:
                ref_b, ref_a = h_b, h_a
                break
        if ref_b is not None and ref_a is not None:
            ask_pull = max(0.0, (ref_a - top_ask_vol) / max(10.0, ref_a)) if ref_a >= 20.0 else 0.0
            bid_pull = max(0.0, (ref_b - top_bid_vol) / max(10.0, ref_b)) if ref_b >= 20.0 else 0.0
            pull_score = round(max(-1.0, min(1.0, ask_pull - bid_pull)), 3)

        bk["bids"] = top_bids
        bk["asks"] = top_asks
        bk["best_bid"] = round(best_bid, 4)
        bk["best_ask"] = round(best_ask, 4)
        bk["mid"] = mid
        bk["spread"] = spread
        bk["imbalance"] = imbalance
        bk["liquidity_pull"] = pull_score
        bk["updated_at"] = t
        self._emit_update(side, event_type)

    def _apply_levels_to_maps(self, bids: list, asks: list) -> tuple[dict[float, float], dict[float, float]]:
        new_bids: dict[float, float] = {}
        new_asks: dict[float, float] = {}
        for b in bids or ():
            try:
                p, sz = float(b["price"]), float(b["size"])
                if 0.0 < p < 1.0 and sz > 0.0:
                    new_bids[round(p, 4)] = sz
            except (KeyError, TypeError, ValueError):
                continue
        for a in asks or ():
            try:
                p, sz = float(a["price"]), float(a["size"])
                if 0.0 < p < 1.0 and sz > 0.0:
                    new_asks[round(p, 4)] = sz
            except (KeyError, TypeError, ValueError):
                continue
        return new_bids, new_asks

    def _apply_levels(self, side: str, bids: list, asks: list, event_type: str = "book"):
        new_bids, new_asks = self._apply_levels_to_maps(bids, asks)
        if not new_bids and not new_asks:
            return
        self._bid_maps[side] = new_bids
        self._ask_maps[side] = new_asks
        self._recompute_from_maps(side, event_type)

    def _apply_presub_levels(self, asset_id: str, bids: list, asks: list):
        new_bids, new_asks = self._apply_levels_to_maps(bids, asks)
        if new_bids or new_asks:
            self._pre_bid_maps[asset_id] = new_bids
            self._pre_ask_maps[asset_id] = new_asks

    def _apply_price_change_batch(self, changes: list[dict], now: float, parent_ts_ms: int = 0):
        dirty_sides: set[str] = set()
        bbo_overrides: dict[str, tuple[Optional[float], Optional[float]]] = {}

        for ch in changes:
            aid = str(ch.get("asset_id", ""))
            ts_ms = self._parse_ev_ts_ms(ch) or parent_ts_ms
            if ts_ms > 0:
                lat = self.clock.observe(ts_ms)
                if lat is not None:
                    self.status["last_delay_ms"] = lat
            if self._is_stale(aid, ts_ms):
                continue

            s2 = self._side_for_token(aid)
            is_pre = (s2 is None) and self._is_presub_token(aid)
            if s2 is None and not is_pre:
                continue

            bmap = self._bid_maps[s2] if s2 else self._pre_bid_maps.setdefault(aid, {})
            amap = self._ask_maps[s2] if s2 else self._pre_ask_maps.setdefault(aid, {})

            raw_p = ch.get("price")
            raw_sz = ch.get("size")
            raw_side = str(ch.get("side") or "").strip().upper()
            if raw_p not in (None, "") and raw_sz not in (None, "") and raw_side in ("BUY", "SELL", "BID", "ASK"):
                try:
                    p_f = round(float(raw_p), 4)
                    sz_f = float(raw_sz)
                    if 0.0 < p_f < 1.0:
                        target_map = bmap if raw_side in ("BUY", "BID") else amap
                        if sz_f <= 0.0:
                            target_map.pop(p_f, None)
                        else:
                            target_map[p_f] = sz_f
                        if s2:
                            dirty_sides.add(s2)
                except (TypeError, ValueError):
                    pass

            bb = ch.get("best_bid")
            ba = ch.get("best_ask")
            if bb not in (None, "") or ba not in (None, ""):
                exp_bid, exp_ask, ok = self._normalize_bbo(bb, ba)
                if ok:
                    matched = self._verify_top_maps(bmap, amap, exp_bid, exp_ask)
                    if s2:
                        if not matched and (bmap or amap):
                            self.status["mismatches"] = int(self.status.get("mismatches", 0)) + 1
                        bbo_overrides[s2] = (exp_bid, exp_ask)
                        dirty_sides.add(s2)

        for s2 in dirty_sides:
            if s2 in bbo_overrides:
                ob, oa = bbo_overrides[s2]
                self._recompute_from_maps(s2, "price_change", now_ts=now, override_bid=ob, override_ask=oa, has_override=True)
            else:
                self._recompute_from_maps(s2, "price_change", now_ts=now)

    async def fetch_rest_snapshot(self) -> bool:
        up_tid, dn_tid = self.up_token_id, self.down_token_id
        if not up_tid or not dn_tid:
            return False

        ok_any = False
        session = await self._get_session()

        async def _fetch_one(side: str, tid: str):
            nonlocal ok_any
            try:
                t0 = time.time()
                async with session.get(
                    f"{CLOB_API_BASE}/book",
                    params={"token_id": tid},
                    proxy=POLY_PROXY or None,
                    timeout=aiohttp.ClientTimeout(total=3.5),
                ) as r:
                    self.clock.observe_http_date(r.headers.get("Date"), rtt_ms=(time.time() - t0) * 1000.0)
                    if r.status == 200:
                        d = await r.json()
                        if (side == "UP" and tid == self.up_token_id) or (side == "DOWN" and tid == self.down_token_id):
                            self._apply_levels(side, d.get("bids", []), d.get("asks", []), event_type="book")
                            if self.books[side]["best_bid"] > 0 or self.books[side]["best_ask"] > 0:
                                ok_any = True
            except Exception:
                pass

        await asyncio.gather(_fetch_one("UP", up_tid), _fetch_one("DOWN", dn_tid))
        self.refresh_status_metrics()
        return ok_any

    async def verify_fresh_book_rest(self, side: str, limit_price: float, is_buy: bool = True, is_taker: bool = False) -> tuple[bool, str, dict]:
        """Pre-order REST book check when local book age or WS delay > 1.0s."""
        tid = self.up_token_id if side == "UP" else self.down_token_id
        if not tid:
            return False, "缺少有效 Token ID", {}
        bk = self.books[side]
        age = time.time() - float(bk.get("updated_at") or 0.0)
        delay_ms = float(self.status.get("last_delay_ms") or 0.0)
        if age <= 1.0 and delay_ms <= 1000.0 and bk["best_bid"] > 0 and bk["best_ask"] > 0:
            return True, "", bk

        try:
            session = await self._get_session()
            async with session.get(
                f"{CLOB_API_BASE}/book",
                params={"token_id": tid},
                proxy=POLY_PROXY or None,
                timeout=aiohttp.ClientTimeout(total=2.5),
            ) as r:
                if r.status != 200:
                    return False, f"REST 盘口复核失败(HTTP {r.status})", bk
                d = await r.json()
                self._apply_levels(side, d.get("bids", []), d.get("asks", []), event_type="book")
        except Exception as e:
            return False, f"REST 盘口复核超时({type(e).__name__})", bk

        fresh = self.books[side]
        bb, ba = float(fresh["best_bid"]), float(fresh["best_ask"])
        if is_buy:
            if is_taker:
                if ba <= 0 or ba > limit_price + 1e-6:
                    return False, f"保护价{limit_price:.2f}内已无卖盘(现卖一{ba:.2f})", fresh
            else:
                if ba > 0 and limit_price >= ba:
                    return False, f"Maker挂买{limit_price:.2f}已穿越最新卖一{ba:.2f}", fresh
        else:
            if is_taker:
                if bb <= 0 or bb < limit_price - 1e-6:
                    return False, f"保护价{limit_price:.2f}内已无买盘(现买一{bb:.2f})", fresh
            else:
                if bb > 0 and limit_price <= bb:
                    return False, f"Maker挂卖{limit_price:.2f}已穿越最新买一{bb:.2f}", fresh
        return True, "", fresh

    async def _interruptible_sleep(self, seconds: float):
        try:
            await asyncio.wait_for(self._sub_change_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def _ws_guardian(self, ws, connected_at: float, subscribed: set[str], zombie_flag: list[bool]):
        last_ping_ts = time.time()
        last_rest_retry_ts = last_ping_ts
        try:
            while not self._stop:
                try:
                    await asyncio.wait_for(self._sub_change_event.wait(), timeout=1.5)
                    self._sub_change_event.clear()
                    if self._stop:
                        break
                    want = self._wanted_tokens()
                    add = sorted(want - subscribed)
                    rem = sorted(subscribed - want)
                    if add:
                        await ws.send(json.dumps({
                            "assets_ids": add,
                            "operation": "subscribe",
                            "custom_feature_enabled": True,
                        }))
                        subscribed.update(add)
                    if rem:
                        await ws.send(json.dumps({
                            "assets_ids": rem,
                            "operation": "unsubscribe",
                        }))
                        subscribed.difference_update(rem)
                    if add and not self.status["live_quotes"]:
                        await self.fetch_rest_snapshot()
                    continue
                except asyncio.TimeoutError:
                    pass

                now = time.time()
                if now - last_ping_ts >= 10.0:
                    try:
                        await ws.send("PING")
                        last_ping_ts = now
                    except Exception:
                        break

                self.refresh_status_metrics()
                if not self.status["live_quotes"]:
                    retry_gap = 1.5 if (now - connected_at) < 15.0 else 4.0
                    if now - last_rest_retry_ts >= retry_gap:
                        last_rest_retry_ts = now
                        await self.fetch_rest_snapshot()

                delay = self.status.get("last_delay_ms")
                if delay is not None and delay > self.BACKLOG_DELAY_MS:
                    self._backlog_ticks += 1
                    if (
                        self._backlog_ticks >= self.BACKLOG_CONSEC_TICKS
                        and (now - self._last_backlog_reconnect_ts) >= self.BACKLOG_RECONNECT_COOLDOWN
                    ):
                        self._backlog_ticks = 0
                        self._last_backlog_reconnect_ts = now
                        zombie_flag[0] = True
                        self.status["last_error"] = f"盘口延迟积压({int(delay)}ms)，自动重连同步"
                        break
                else:
                    self._backlog_ticks = 0

                if (now - connected_at) >= 15.0 and self.status["book_age_sec"] >= 15.0 and self.up_token_id:
                    await self.fetch_rest_snapshot()
                    if self.status["book_age_sec"] >= 15.0:
                        zombie_flag[0] = True
                        break
        except asyncio.CancelledError:
            return
        try:
            await ws.close()
        except Exception:
            pass

    async def run(self):
        backoff = 1.0
        while not self._stop:
            self._sub_change_event.clear()
            want = self._wanted_tokens()
            if not want:
                self.status["connected"] = False
                self.status["live_quotes"] = False
                await self._interruptible_sleep(0.5)
                continue

            await self.fetch_rest_snapshot()
            if self._stop:
                break

            subscribed = set(self._wanted_tokens())
            zombie_flag = [False]
            guardian = None
            try:
                async with websockets.connect(
                    CLOB_WS_URL,
                    ping_interval=None,
                    close_timeout=4,
                    **ws_headers_kwarg(websockets.connect),
                ) as ws:
                    await ws.send(json.dumps({
                        "assets_ids": sorted(subscribed),
                        "type": "market",
                        "initial_dump": True,
                        "custom_feature_enabled": True,
                    }))
                    self.status["connected"] = True
                    backoff = 1.0
                    connected_at = time.time()
                    guardian = asyncio.create_task(self._ws_guardian(ws, connected_at, subscribed, zombie_flag))

                    async for raw in ws:
                        if self._stop:
                            break
                        if not raw or raw in ("PONG", "pong"):
                            continue

                        now = time.time()
                        self.status["last_msg_ts"] = now
                        try:
                            data = fast_loads(raw)
                        except Exception:
                            continue
                        events = data if isinstance(data, list) else (data,)
                        for ev in events:
                            if not isinstance(ev, dict):
                                continue
                            ev_type = ev.get("event_type")
                            ev_ts_ms = self._parse_ev_ts_ms(ev)
                            if ev_ts_ms > 0:
                                lat = self.clock.observe(ev_ts_ms)
                                if lat is not None:
                                    self.status["last_delay_ms"] = lat

                            if ev_type == "price_change":
                                changes = ev.get("price_changes")
                                if isinstance(changes, list):
                                    self._apply_price_change_batch(changes, now, parent_ts_ms=ev_ts_ms)
                                else:
                                    self._apply_price_change_batch([ev], now, parent_ts_ms=ev_ts_ms)
                                continue

                            aid = str(ev.get("asset_id", ""))
                            if ev_type == "tick_size_change":
                                try:
                                    nt = float(ev.get("new_tick_size") or ev.get("tick_size") or 0)
                                    if nt > 0:
                                        self.set_tick_size(aid, nt)
                                except (TypeError, ValueError):
                                    pass
                                continue

                            side = self._side_for_token(aid)
                            if not side:
                                if ev_type == "book" and self._is_presub_token(aid):
                                    self._apply_presub_levels(aid, ev.get("bids", []), ev.get("asks", []))
                                continue

                            if self._is_stale(aid, ev_ts_ms):
                                continue

                            if ev_type == "book":
                                self._apply_levels(side, ev.get("bids", []), ev.get("asks", []), event_type="book")

                            elif ev_type == "best_bid_ask":
                                exp_bid, exp_ask, ok = self._normalize_bbo(ev.get("best_bid"), ev.get("best_ask"))
                                if ok:
                                    matched = self._verify_top_maps(self._bid_maps[side], self._ask_maps[side], exp_bid, exp_ask)
                                    if not matched and (self._bid_maps[side] or self._ask_maps[side]):
                                        self.status["mismatches"] = int(self.status.get("mismatches", 0)) + 1
                                    self._recompute_from_maps(
                                        side, "price_change", now_ts=now,
                                        override_bid=exp_bid, override_ask=exp_ask, has_override=True,
                                    )

                            elif ev_type == "last_trade_price":
                                try:
                                    p = float(ev.get("price", 0))
                                except (TypeError, ValueError):
                                    p = 0.0
                                if 0.0 < p < 1.0:
                                    bk = self.books[side]
                                    bk["last_trade"] = round(p, 4)
                                    bk["last_trade_ts"] = now
                                    bk["last_trade_side"] = str(ev.get("side") or "").strip().upper()
                                    if bk["best_bid"] > 0.0 or bk["best_ask"] > 0.0:
                                        bk["updated_at"] = now
                                    self._emit_update(side, "trade")

                    if zombie_flag[0]:
                        raise RuntimeError(self.status.get("last_error") or "CLOB WS 静默或积压，主动重连")
            except Exception as e:
                if not self._stop:
                    self.status["connected"] = False
                    self.status["live_quotes"] = False
                    self.status["errors"] += 1
                    msg = str(e).strip().replace("\n", " ")[:52]
                    if "429" in msg:
                        self.status["last_error"] = "HTTP 429 盘口限流"
                    elif "403" in msg or "451" in msg:
                        self.status["last_error"] = "盘口接口访问受限"
                    elif msg:
                        self.status["last_error"] = msg
                    else:
                        self.status["last_error"] = f"盘口连接异常({type(e).__name__})"
                    await self._interruptible_sleep(backoff)
                    backoff = min(15.0, backoff * 1.6)
            finally:
                if guardian:
                    guardian.cancel()
