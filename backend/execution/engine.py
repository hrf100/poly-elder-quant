import asyncio
import json
import time
from collections import deque
from decimal import Decimal
from typing import Optional
import aiohttp

from backend.config import HARD_LIMITS, MODELS_DIR
from backend.execution.live_clob import PolymarketAccountManager
from backend.execution.paper_engine import PaperExecutionEngine
from backend.feeds.cex_feeds import CexPriceFeed
from backend.feeds.polymarket_clob import PolymarketClobFeed
from backend.feeds.polymarket_oracle import PolymarketOracleSync
from backend.monitor.system_stats import SystemMonitor
from backend.storage.db import Database
from backend.strategy.offline_trainer import load_model_artifact, run_walk_forward_training
from backend.strategy.online_learner import SelfLearningCoordinator
from backend.strategy.quant_elder import QuantElderFeatureEngine


class QuantTradingEngine:
    """
    Main asynchronous orchestrator:
    - Keeps Gamma API Price-to-Beat & PolyBolt 60s TWAP strictly synchronized with Polymarket.
    - Runs 1s Quant Elder feature engine & 5s full-episode trajectory recorder.
    - Drives Online Self-Learning + Paper/Shadow/Live execution + 12h Low-Priority Offline Training.
    """

    def __init__(self, db: Database):
        self.db = db
        self.monitor = SystemMonitor(db)
        self.account = PolymarketAccountManager(db)
        self.paper = PaperExecutionEngine(db, on_order_cancelled=self._on_paper_order_cancelled)
        self.learner = SelfLearningCoordinator(db)
        self.features = QuantElderFeatureEngine(
            ema_period=self.learner.state.get("ema_period", 13),
            hl_window=self.learner.state.get("hl_window", 5),
        )

        self.cex = CexPriceFeed(on_price=self._on_cex_price)
        self.oracle = PolymarketOracleSync(on_oracle_tick=self._on_oracle_tick)
        self.clob = PolymarketClobFeed(on_book_update=self._on_book_update)

        # Active round state
        self.current_window_ts: int = 0
        self.current_slug: str = ""
        self.current_ptb: Optional[Decimal] = None
        self.current_ptb_source: str = "同步中"
        self.current_ptb_official: bool = False
        self.current_ptb_boundary_dt: float = 999.0
        self.current_condition_id: Optional[str] = None
        self.current_up_token: Optional[str] = None
        self.current_down_token: Optional[str] = None
        self.prefetched_rounds: dict[int, dict] = {}
        self._prefetch_in_flight: set[int] = set()
        self._settling_windows: set[int] = set()
        self.pending_settlements: dict[int, dict] = {}
        self.last_settled_round: Optional[dict] = None

        # Real-time pricing & signal state
        self.basis_ema: float = 0.0
        self._last_cex_basis_ts: float = 0.0
        self.last_signal_ts: float = 0.0
        self.last_cancel_ts: float = 0.0
        self.last_cancel_side: str = ""
        self.consecutive_losses: int = 0
        self.circuit_breaker_until: float = 0.0
        self.round_had_outlier: bool = False
        self.round_episode_samples: list[tuple[list[float], float]] = []
        self.round_orders_count: int = 0
        self.last_arb_attempt_ts: float = 0.0
        self.live_chart_buffer: deque[dict] = deque(maxlen=320)
        self.latest_pricing: dict = {
            "model_up_prob": 0.5,
            "model_down_prob": 0.5,
            "up_edge": 0.0,
            "down_edge": 0.0,
            "signal": "NONE",
            "last_decision": "初始化中",
            "last_decision_ts": 0.0,
            "last_block_reason": "",
            "last_block_ts": 0.0,
            "round_block_count": 0,
            "round_block_reasons": {},
        }
        self._decision_ts: float = 0.0
        self._decision_window_ts: int = 0
        self._decision_sticky: bool = False
        self._last_block_dedup_key: str = ""
        self._last_block_dedup_ts: float = 0.0
        auto_cfg = self.db.get_json("auto_train_config", {"enabled": True, "interval_hours": 12, "min_rounds": 16})
        self._last_auto_train_ts: float = time.time()
        self.training_status: dict = {
            "running": False,
            "last_run_ts": 0.0,
            "last_result": None,
            "auto_enabled": bool(auto_cfg.get("enabled", True)),
            "interval_hours": int(auto_cfg.get("interval_hours", 12)),
            "min_rounds": int(auto_cfg.get("min_rounds", 16)),
            "next_auto_train_ts": self._last_auto_train_ts + int(auto_cfg.get("interval_hours", 12)) * 3600,
        }
        self._tasks: list[asyncio.Task] = []
        self._stop = False

    def _load_champion_model(self):
        versions = self.db.list_model_versions(limit=10)
        for v in versions:
            if v["role"] == "CHAMPION":
                model_obj = load_model_artifact(v.get("artifact_path"))
                if model_obj:
                    params = json.loads(v["params_json"])
                    self.learner.apply_offline_params(params, v["version_id"], model_obj)
                    self.features.set_hyperparams(params.get("ema_period", 13), params.get("hl_window", 5))
                break

    def sync_oracle_credentials(self):
        creds = self.account.get_decrypted_api_creds()
        if creds:
            self.oracle.set_credentials(creds["apiKey"], creds["secret"], creds["passphrase"])

    async def start(self):
        self._load_champion_model()
        self.sync_oracle_credentials()
        # Hydrate latest settled round from DB so UI immediately displays last settlement & round time after restart
        recent_settled = self.db.get_recent_settled_rounds(limit=1)
        if recent_settled:
            r0 = recent_settled[0]
            if r0.get("official_outcome") in ("UP", "DOWN"):
                self.last_settled_round = {
                    "window_ts": int(r0["window_ts"]),
                    "price_to_beat": r0.get("price_to_beat"),
                    "settlement_price": r0.get("settlement_price") or "--",
                    "official_outcome": r0["official_outcome"],
                    "verified": True,
                }
        await self.oracle.check_geo_and_clock()

        self._tasks = [
            asyncio.create_task(self.cex.run_binance()),
            asyncio.create_task(self.cex.run_binance_perp()),
            asyncio.create_task(self.oracle.run_rtds()),
            asyncio.create_task(self.oracle.run_polybolt()),
            asyncio.create_task(self.clob.run()),
            asyncio.create_task(self._main_1s_loop()),
            asyncio.create_task(self._periodic_maintenance_loop()),
            asyncio.create_task(self._startup_recover_orphans()),
        ]

    async def _startup_recover_orphans(self):
        await asyncio.sleep(2.5)
        if not self._stop:
            await self.recover_unsettled_windows()

    async def stop(self):
        self._stop = True
        self.cex.stop()
        self.oracle.stop()
        self.clob.stop()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.oracle.close()
        await self.clob.close()

    def _on_cex_price(self, exchange: str, price: float, ts: float):
        if self.oracle.latest_spot_float > 0.0 and (self.basis_ema == 0.0 or (ts - self._last_cex_basis_ts) >= 0.25):
            self._last_cex_basis_ts = ts
            diff = price - self.oracle.latest_spot_float
            self.basis_ema = diff if self.basis_ema == 0.0 else (0.95 * self.basis_ema + 0.05 * diff)

    def _on_oracle_tick(self, kind: str, price: Decimal, ts: float, is_snapshot: bool = False, source_name: str = "RTDS"):
        spot = self.cex.get_best_spot()
        if kind == "spot" and spot > 0 and not is_snapshot:
            diff = spot - float(price)
            self.basis_ema = diff if self.basis_ema == 0.0 else (0.95 * self.basis_ema + 0.05 * diff)

        # Continuously refine tentative Price-to-Beat to exact T=0s boundary tick until official Gamma/settlement locks it
        if kind == "twap60" and self.current_window_ts > 0 and not self.current_ptb_official:
            boundary_px, dt = self.oracle.get_twap_at_boundary_detail(self.current_window_ts, max_delta_sec=15.0)
            if boundary_px is not None and (self.current_ptb is None or dt + 0.05 < self.current_ptb_boundary_dt):
                self.current_ptb = boundary_px
                self.current_ptb_boundary_dt = dt
                self.current_ptb_source = f"{source_name} 整点精确对齐" if dt <= 1.5 else f"{source_name} 整点预锁"
            elif self.current_ptb is None and not is_snapshot and (time.time() - self.current_window_ts) <= 15:
                self.current_ptb = price
                self.current_ptb_boundary_dt = abs(ts - self.current_window_ts)
                self.current_ptb_source = f"{source_name} 整点预锁"

    def _on_book_update(self, side: str, book: dict, event_type: str = "book"):
        self.paper.on_orderbook_tick(side, book, event_type=event_type)

    def _set_decision(self, text: str, sticky: bool = False, now: Optional[float] = None):
        t = float(now if now is not None else time.time())
        self.latest_pricing["last_decision"] = text
        self.latest_pricing["last_decision_ts"] = round(t, 1)
        self._decision_ts = t
        self._decision_window_ts = self.current_window_ts
        self._decision_sticky = bool(sticky)

    def _record_block(self, reason: str, category: str, now: Optional[float] = None):
        """Record a real trade/signal rejection without letting 15s idle fallback erase diagnostic history."""
        t = float(now if now is not None else time.time())
        self._set_decision(reason, sticky=False, now=t)
        self.latest_pricing["last_block_reason"] = reason
        self.latest_pricing["last_block_ts"] = round(t, 1)
        dedup_key = f"{self.current_window_ts}:{category}:{reason[:28]}"
        if dedup_key != self._last_block_dedup_key or (t - self._last_block_dedup_ts) >= 5.0:
            self._last_block_dedup_key = dedup_key
            self._last_block_dedup_ts = t
            self.latest_pricing["round_block_count"] = int(self.latest_pricing.get("round_block_count") or 0) + 1
            counts = dict(self.latest_pricing.get("round_block_reasons") or {})
            counts[category] = int(counts.get(category) or 0) + 1
            self.latest_pricing["round_block_reasons"] = counts

    def _on_paper_order_cancelled(self, order: dict):
        """Cancelled orders NEVER count as opened trades and immediately release the 15s cooldown."""
        if order.get("arm_id") == "ARB_LOCK_MAKER":
            return
        self.last_signal_ts = 0.0
        self.last_cancel_ts = time.time()
        self.last_cancel_side = str(order.get("side") or "")
        self._set_decision(f"{order.get('side')}挂单超时撤单（不计已开单），继续寻机", sticky=False)

    async def _send_webhook(self, title: str, text: str):
        cfg = self.db.get_json("webhook_config", {})
        if not cfg.get("enabled"):
            return
        try:
            async with aiohttp.ClientSession() as session:
                if cfg.get("feishu_url"):
                    await session.post(
                        cfg["feishu_url"],
                        json={"msg_type": "text", "content": {"text": f"[PolyQuant] {title}\n{text}"}},
                        timeout=aiohttp.ClientTimeout(total=5),
                    )
                if cfg.get("telegram_bot_token") and cfg.get("telegram_chat_id"):
                    tg_url = f"https://api.telegram.org/bot{cfg['telegram_bot_token']}/sendMessage"
                    await session.post(
                        tg_url,
                        json={"chat_id": cfg["telegram_chat_id"], "text": f"<b>{title}</b>\n{text}", "parse_mode": "HTML"},
                        timeout=aiohttp.ClientTimeout(total=5),
                    )
        except Exception:
            pass

    async def _switch_to_round(self, new_window_ts: int):
        prev_window_ts = self.current_window_ts
        prev_samples = list(self.round_episode_samples)
        prev_had_outlier = self.round_had_outlier
        prev_ptb = self.current_ptb
        prev_twap = self.oracle.latest_twap60
        prev_cond_id = self.current_condition_id

        # Schedule official settlement for previous round asynchronously (never blocks new round)
        if prev_window_ts > 0:
            prov_out = None
            if prev_ptb is not None and prev_twap > 0:
                prov_out = "UP" if prev_twap >= prev_ptb else "DOWN"
            self.pending_settlements[prev_window_ts] = {
                "provisional_outcome": prov_out,
                "provisional_price": str(prev_twap.quantize(Decimal("0.01"))) if prev_twap > 0 else "--",
                "started_at": time.time(),
            }
            asyncio.create_task(
                self._settle_previous_round(
                    prev_window_ts, prev_twap, prev_ptb, prev_samples, prev_had_outlier,
                    known_condition_id=prev_cond_id,
                )
            )

        self.current_window_ts = new_window_ts
        self.current_slug = f"btc-updown-5m-{new_window_ts}"
        self.round_episode_samples = []
        self.live_chart_buffer.clear()
        self.round_had_outlier = False
        self.round_orders_count = 0
        self.last_arb_attempt_ts = 0.0
        self.last_cancel_ts = 0.0
        self.last_cancel_side = ""
        self.latest_pricing["last_block_reason"] = ""
        self.latest_pricing["last_block_ts"] = 0.0
        self.latest_pricing["round_block_count"] = 0
        self.latest_pricing["round_block_reasons"] = {}
        self._last_block_dedup_key = ""
        self._last_block_dedup_ts = 0.0
        self._set_decision("新场次已同步，等待有效信号", sticky=False)
        self.current_ptb = None
        self.current_ptb_source = "同步中"
        self.current_ptb_official = False
        self.current_ptb_boundary_dt = 999.0
        # CRITICAL: Immediately switch CLOB to pre-fetched next-round tokens (0ms delay!) or clear stale tokens
        prefetched = self.prefetched_rounds.pop(new_window_ts, None)
        if prefetched and prefetched.get("up_token_id") and prefetched.get("down_token_id"):
            self.current_condition_id = prefetched.get("condition_id")
            self.current_up_token = prefetched["up_token_id"]
            self.current_down_token = prefetched["down_token_id"]
            self.clob.set_tokens(self.current_up_token, self.current_down_token)
        else:
            self.current_condition_id = None
            self.current_up_token = None
            self.current_down_token = None
            self.clob.set_tokens(None, None)

        # 1. Instant tentative Price-to-Beat from closest boundary TWAP (will be refined at T+1s & overwritten by official settlement)
        boundary_px, dt = self.oracle.get_twap_at_boundary_detail(new_window_ts, max_delta_sec=10.0)
        if boundary_px is not None:
            self.current_ptb = boundary_px
            self.current_ptb_boundary_dt = dt
            self.current_ptb_source = f"{self.oracle.active_oracle_source} 整点预锁"
        elif self.oracle.latest_twap60 > 0:
            self.current_ptb = self.oracle.latest_twap60
            self.current_ptb_boundary_dt = 5.0
            self.current_ptb_source = f"{self.oracle.active_oracle_source} 整点预锁"

        open_spot = self.cex.get_best_spot()
        self.features.on_new_round(new_window_ts, open_spot)

        # 2. Fetch official Gamma metadata & Price to Beat (retry in background until Gamma locks priceToBeat)
        asyncio.create_task(self._sync_gamma_round_metadata(new_window_ts))

    async def _prefetch_next_round(self, next_window_ts: int):
        """Pre-fetch next 5m round token IDs 25-30s early so CLOB orderbook switches at T=0.0s with zero gap."""
        if next_window_ts in self.prefetched_rounds or next_window_ts in self._prefetch_in_flight:
            return
        self._prefetch_in_flight.add(next_window_ts)
        try:
            info = await self.oracle.fetch_gamma_round(next_window_ts)
            if info and info.get("up_token_id") and info.get("down_token_id"):
                self.prefetched_rounds[next_window_ts] = info
                # Keep cache bounded
                for old_ts in [k for k in self.prefetched_rounds if k < self.current_window_ts]:
                    self.prefetched_rounds.pop(old_ts, None)
        finally:
            self._prefetch_in_flight.discard(next_window_ts)

    async def _sync_gamma_round_metadata(self, window_ts: int):
        # Poll gently across the entire 300s round until official Gamma priceToBeat or previous finalPrice locks in
        for attempt in range(45):
            if self._stop or window_ts != self.current_window_ts:
                return
            if self.current_ptb_official and self.current_up_token and self.current_down_token:
                return

            cur_info, prev_info = await asyncio.gather(
                self.oracle.fetch_gamma_round(window_ts),
                self.oracle.fetch_gamma_round(window_ts - 300),
            )
            if cur_info:
                if cur_info.get("condition_id"):
                    self.current_condition_id = cur_info["condition_id"]
                if cur_info["up_token_id"] and cur_info["down_token_id"]:
                    self.current_up_token = cur_info["up_token_id"]
                    self.current_down_token = cur_info["down_token_id"]
                    self.clob.set_tokens(cur_info["up_token_id"], cur_info["down_token_id"])

                if cur_info["price_to_beat"] is not None:
                    self.current_ptb = cur_info["price_to_beat"]
                    self.current_ptb_source = "Gamma 官方已锁定"
                    self.current_ptb_official = True
                    self.current_ptb_boundary_dt = 0.0
                elif prev_info and prev_info.get("final_price") is not None:
                    self.current_ptb = prev_info["final_price"]
                    self.current_ptb_source = "上场官方结算价锁定"
                    self.current_ptb_official = True
                    self.current_ptb_boundary_dt = 0.0
                elif not self.current_ptb_official:
                    boundary_px, dt = self.oracle.get_twap_at_boundary_detail(window_ts, max_delta_sec=15.0)
                    if boundary_px is not None and (self.current_ptb is None or dt + 0.05 < self.current_ptb_boundary_dt):
                        self.current_ptb = boundary_px
                        self.current_ptb_boundary_dt = dt
                        self.current_ptb_source = f"{self.oracle.active_oracle_source} 整点精确对齐" if dt <= 1.5 else f"{self.oracle.active_oracle_source} 整点预锁"

                self.db.upsert_round({
                    "window_ts": window_ts,
                    "slug": cur_info["slug"],
                    "condition_id": cur_info["condition_id"],
                    "up_token_id": cur_info["up_token_id"],
                    "down_token_id": cur_info["down_token_id"],
                    "price_to_beat": self.current_ptb,
                    "ptb_source": self.current_ptb_source,
                    "status": "ACTIVE",
                })
                if self.current_ptb_official and self.current_up_token:
                    return
            await asyncio.sleep(3.0 if attempt < 15 else 8.0)

    async def recover_unsettled_windows(self):
        """Auto-discover & settle any past windows left unsettled across restarts or network blips."""
        cur_win = self.current_window_ts or self.oracle.get_window_ts()
        if cur_win <= 0:
            return
        past_windows = self.db.get_unsettled_past_windows(cur_win, limit=20)
        for w_ts in past_windows:
            if w_ts in self._settling_windows:
                continue
            r_row = self.db.get_round(w_ts)
            cond_id = r_row.get("condition_id") if r_row else None
            ptb_dec = None
            if r_row and r_row.get("price_to_beat"):
                try:
                    ptb_dec = Decimal(str(r_row["price_to_beat"]))
                except Exception:
                    ptb_dec = None
            age_sec = time.time() - (w_ts + 300)
            # For older recovered windows, shorter timeout (60s) since Polymarket already resolved them
            timeout_s = 60 if age_sec > 360 else 420
            self.pending_settlements.setdefault(w_ts, {
                "provisional_outcome": None,
                "provisional_price": "--",
                "started_at": time.time(),
            })
            asyncio.create_task(
                self._settle_previous_round(
                    w_ts, Decimal("0"), ptb_dec, [], False,
                    known_condition_id=cond_id, timeout_sec=timeout_s, is_recovery=True,
                )
            )

    def get_enriched_orders(self, mode: Optional[str] = None, limit: int = 100) -> list[dict]:
        """Attach awaiting_official & provisional_outcome metadata for orders in ended rounds awaiting Polymarket confirmation."""
        rows = self.db.get_orders(mode=mode, limit=limit)
        now = time.time()
        for o in rows:
            ended = (int(o["window_ts"]) + 300) <= now
            awaiting = bool(ended and o["status"] == "FILLED" and not o.get("official_outcome"))
            o["awaiting_official"] = awaiting
            if awaiting:
                ps = self.pending_settlements.get(int(o["window_ts"]))
                o["provisional_outcome"] = ps.get("provisional_outcome") if ps else None
            else:
                o["provisional_outcome"] = None
        return rows

    async def _settle_previous_round(
        self,
        window_ts: int,
        fallback_twap: Decimal,
        fallback_ptb: Optional[Decimal],
        episode_samples: list[tuple[list[float], float]],
        had_outlier: bool,
        known_condition_id: Optional[str] = None,
        timeout_sec: int = 420,
        is_recovery: bool = False,
    ):
        if window_ts in self._settling_windows:
            return
        self._settling_windows.add(window_ts)
        try:
            res = await self.oracle.wait_official_settlement(
                window_ts, fallback_twap, fallback_ptb,
                known_condition_id=known_condition_id, timeout_sec=timeout_sec,
            )
            outcome = res["official_outcome"]
            if not res.get("verified") or outcome not in ("UP", "DOWN"):
                self.paper.void_unverified_window(window_ts)
                if not is_recovery:
                    self._set_decision(f"场次 {window_ts} 官方裁定超时，订单已作废且跳过本场学习", sticky=False)
                return

            self.db.settle_round(window_ts, res["settlement_price"], outcome, res["price_to_beat"])
            if self.last_settled_round is None or window_ts >= int(self.last_settled_round.get("window_ts", 0)):
                self.last_settled_round = res

            # Exact official handoff: previous round's official settlement price == current round's exact Price to Beat!
            if window_ts + 300 == self.current_window_ts and res.get("settlement_price") not in (None, "", "--"):
                try:
                    exact_ptb = Decimal(str(res["settlement_price"]))
                    if exact_ptb > 0 and not self.current_ptb_official:
                        self.current_ptb = exact_ptb
                        self.current_ptb_source = "上场官方结算价锁定"
                        self.current_ptb_official = True
                        self.current_ptb_boundary_dt = 0.0
                        self.db.upsert_round({
                            "window_ts": self.current_window_ts,
                            "slug": self.current_slug,
                            "price_to_beat": self.current_ptb,
                            "ptb_source": self.current_ptb_source,
                            "status": "ACTIVE",
                        })
                except Exception:
                    pass

            settled_orders = self.paper.settle_window(window_ts, outcome)

            # Update consecutive loss circuit breaker using NET round PnL
            if not is_recovery:
                risk_cfg = self.db.get_json("risk_config", {})
                mode = self.db.get_kv("trading_mode", "PAPER")
                mode_settled = [o for o in settled_orders if o["mode"] == mode and o["status"] in ("SETTLED", "CLOSED")]
                if mode_settled:
                    net_round_pnl = sum(float(o["pnl_usdc"] or 0.0) for o in mode_settled)
                    if net_round_pnl < 0:
                        self.consecutive_losses += 1
                        if self.consecutive_losses >= int(risk_cfg.get("consecutive_loss_limit", 3)):
                            self.circuit_breaker_until = time.time() + HARD_LIMITS["circuit_breaker_minutes"] * 60
                            await self._send_webhook(
                                "风控熔断触发",
                                f"连续亏损 {self.consecutive_losses} 场，暂停开单 {HARD_LIMITS['circuit_breaker_minutes']} 分钟。",
                            )
                    elif net_round_pnl > 0:
                        self.consecutive_losses = 0

            # Online Self-Learning & Counterfactual Exit-Arm Update using official outcome + 5s slices
            round_slices = self.db.get_slices_by_window(window_ts)
            self.learner.update_on_round_settlement(
                official_outcome=outcome,
                episode_samples=episode_samples,
                settled_orders=settled_orders,
                freeze_updates=had_outlier,
                round_slices=round_slices,
            )
        finally:
            self._settling_windows.discard(window_ts)
            self.pending_settlements.pop(window_ts, None)

    async def _main_1s_loop(self):
        last_slice_sec = -1
        while not self._stop:
            loop_start = time.time()
            try:
                now = time.time()
                win_ts = self.oracle.get_window_ts(now)
                if win_ts != self.current_window_ts:
                    await self._switch_to_round(win_ts)
                    last_slice_sec = -1

                elapsed_sec = int(now - self.current_window_ts)
                spot = self.cex.get_best_spot()
                strike = float(self.current_ptb) if self.current_ptb else 0.0
                twap60 = float(self.oracle.latest_twap60) if self.oracle.latest_twap60 > 0 else (spot - self.basis_ema)

                up_book = self.clob.books["UP"]
                dn_book = self.clob.books["DOWN"]
                orderflow = self.cex.get_orderflow_metrics(now)
                oracle_spot_live = float(self.oracle.latest_spot) if self.oracle.latest_spot > 0 else 0.0
                clob_pull_up = float(up_book.get("liquidity_pull") or 0.0)
                snap = self.features.update_1s(
                    spot, strike, twap60, self.basis_ema, elapsed_sec,
                    up_bid=up_book["best_bid"], up_ask=up_book["best_ask"],
                    orderflow=orderflow, oracle_spot=oracle_spot_live, clob_pull=clob_pull_up,
                )
                if snap["is_outlier_vol"]:
                    self.round_had_outlier = True
                model_up_p, feats = self.learner.predict_up_prob(
                    snap, up_book["imbalance"], elapsed_sec=elapsed_sec, pm_spread=up_book["spread"]
                )
                model_dn_p = round(1.0 - model_up_p, 4)

                up_ref_px = up_book["best_bid"] or up_book["mid"] or 0.5
                dn_ref_px = dn_book["best_bid"] or dn_book["mid"] or 0.5
                up_edge = round(model_up_p - up_ref_px, 4)
                dn_edge = round(model_dn_p - dn_ref_px, 4)

                # Record chart point
                # Sweep expired paper orders every second & verify against live books
                self.paper.sweep_expired_orders(self.clob.books)

                if spot > 0 and self.current_window_ts > 0:
                    oracle_spot = round(spot - self.basis_ema, 2)
                    prev_pt = self.live_chart_buffer[-1] if self.live_chart_buffer else None
                    pm_up_c = self._resolve_chart_book_cents(up_book, dn_book, prev_pt.get("pm_up_mid") if prev_pt else None)
                    pm_dn_c = self._resolve_chart_book_cents(dn_book, up_book, prev_pt.get("pm_down_mid") if prev_pt else None)
                    sec_s = max(0, min(300, int(elapsed_sec)))
                    pt = {
                        "s": sec_s,
                        "t": time.strftime("%H:%M:%S", time.gmtime(now)),
                        "spot": round(spot, 2),
                        "oracle_spot": oracle_spot,
                        "twap60": round(twap60, 2),
                        "strike": round(strike, 2) if strike > 0 else None,
                        "net_power_bps": snap["net_power_bps"],
                        "model_up_prob": round(model_up_p * 100, 1),
                        "pm_up_mid": pm_up_c,
                        "pm_down_mid": pm_dn_c,
                    }
                    if self.live_chart_buffer and self.live_chart_buffer[-1].get("s") == sec_s:
                        self.live_chart_buffer[-1] = pt
                    else:
                        self.live_chart_buffer.append(pt)

                # Pre-fetch next round's Token IDs 30s before rollover so new round starts with live quotes immediately
                if elapsed_sec >= 270 and (self.current_window_ts + 300) not in self.prefetched_rounds:
                    asyncio.create_task(self._prefetch_next_round(self.current_window_ts + 300))

                # Record full-episode slice every 5 seconds ONLY when real orderbook quotes are live (no 0/0 pollution)
                has_live_book = up_book["best_bid"] > 0 and up_book["best_ask"] > 0
                if elapsed_sec % 5 == 0 and elapsed_sec != last_slice_sec and spot > 0 and has_live_book:
                    last_slice_sec = elapsed_sec
                    self.round_episode_samples.append((feats, model_up_p))
                    self.db.insert_slice({
                        "window_ts": self.current_window_ts,
                        "elapsed_sec": elapsed_sec,
                        "ts": now,
                        "spot_price": spot,
                        "twap60_price": twap60,
                        "dist_to_strike_bps": snap["dist_to_strike_bps"],
                        "ema13_slope_bps": snap["ema13_slope_bps"],
                        "net_power_bps": snap["net_power_bps"],
                        "bull_power_bps": snap["bull_power_bps"],
                        "bear_power_bps": snap["bear_power_bps"],
                        "atr_bps": snap["atr_bps"],
                        "path_efficiency": snap["path_efficiency"],
                        "mfe_bps": snap["mfe_bps"],
                        "mae_bps": snap["mae_bps"],
                        "locked_twap_ratio": snap["locked_twap_ratio"],
                        "pm_up_bid": up_book["best_bid"],
                        "pm_up_ask": up_book["best_ask"],
                        "pm_down_bid": dn_book["best_bid"],
                        "pm_down_ask": dn_book["best_ask"],
                        "pm_spread": up_book["spread"],
                        "book_imbalance": up_book["imbalance"],
                        "regime": snap["regime"],
                        "moneyness_z": snap["moneyness_z"],
                        "bs_up_prob": snap["bs_up_prob"],
                        "sigma_rem_bps": snap["sigma_rem_bps"],
                        "iv_1s_bps": snap["iv_1s_bps"],
                        "iv_rv_ratio": snap["iv_rv_ratio"],
                        "log_iv_rv": snap["log_iv_rv"],
                        "iv_state": snap["iv_state"],
                        "cvd_3s_imbalance": snap["cvd_3s_imbalance"],
                        "perp_basis_jump_bps": snap["perp_basis_jump_bps"],
                        "orderflow_score": snap["orderflow_score"],
                        "clob_liquidity_pull": snap["clob_liquidity_pull"],
                        "energy_accum_score": snap["energy_accum_score"],
                        "energy_consistency": snap["energy_consistency"],
                    })

                # Evaluate trading signal & execution
                await self._evaluate_and_trade(now, elapsed_sec, snap, model_up_p, model_dn_p, up_book, dn_book, up_edge, dn_edge)

            except Exception as e:
                self._set_decision(f"引擎异常: {str(e)[:60]}", sticky=False)

            elapsed_loop = time.time() - loop_start
            await asyncio.sleep(max(0.1, 1.0 - elapsed_loop))

    def _get_round_position_state(self, window_ts: int, mode: str) -> dict:
        """Inspect current round orders for the given mode to enforce strict 1-primary-fill + optional equal-share arb lock."""
        orders = [o for o in self.db.get_orders_by_window(window_ts) if o["mode"] == mode]
        pending_orders = [o for o in orders if o["status"] == "PENDING"]
        primary_filled = [
            o for o in orders
            if o["status"] in ("FILLED", "SETTLED", "CLOSED") and o["arm_id"] != "ARB_LOCK_MAKER"
        ]
        arb_filled = [
            o for o in orders
            if o["status"] in ("FILLED", "SETTLED", "CLOSED") and o["arm_id"] == "ARB_LOCK_MAKER"
        ]
        cancelled_primary = [
            o for o in orders
            if o["status"] == "CANCELLED" and o["arm_id"] != "ARB_LOCK_MAKER"
        ]
        return {
            "has_pending": len(pending_orders) > 0,
            "primary_filled": primary_filled[0] if primary_filled else None,
            "arb_filled": arb_filled[0] if arb_filled else None,
            "cancelled_primary_count": len(cancelled_primary),
        }

    async def _try_arb_lock_hedge(
        self,
        now: float,
        elapsed_sec: int,
        mode: str,
        primary_ord: dict,
        up_book: dict,
        dn_book: dict,
        snap: dict,
    ) -> bool:
        """
        Autonomous ARB_LOCK_AUTO Exit Arm:
        Active whenever the filled primary order was assigned exit_arm == 'ARB_LOCK_AUTO'.
        Places 0%-fee Post-Only equal-shares opposite hedge if combined <= evolved arb_max_combined_price.
        """
        if str(primary_ord.get("exit_arm") or "") != "ARB_LOCK_AUTO":
            return False
        if elapsed_sec >= 285 or mode not in ("PAPER", "LIVE"):
            return False
        if now - self.last_arb_attempt_ts < 6.0:
            return False
        filled_at = float(primary_ord.get("filled_at") or primary_ord.get("created_at") or 0.0)
        if now - filled_at < 3.0:
            return False

        primary_side = primary_ord["side"]
        primary_px = float(primary_ord.get("fill_price") or primary_ord["limit_price"])
        primary_shares = float(primary_ord.get("shares") or (float(primary_ord["size_usdc"]) / max(0.01, primary_px)))

        opp_side = "DOWN" if primary_side == "UP" else "UP"
        opp_book = dn_book if opp_side == "DOWN" else up_book
        if not self.clob.is_book_fresh(opp_side, max_age_sec=8.0) or not self.clob.is_parity_ok():
            return False
        opp_bid = float(opp_book.get("best_bid") or 0.0)
        opp_ask = float(opp_book.get("best_ask") or 0.0)
        if opp_bid <= 0.02 or opp_ask <= 0.02:
            return False

        opp_limit = round(opp_bid + 0.01, 2) if (opp_bid + 0.01 < opp_ask) else round(opp_bid, 2)
        opp_limit = max(0.02, min(0.90, opp_limit))

        auto_params = self.learner.get_auto_params()
        max_combined = max(0.92, min(0.96, float(auto_params.get("arb_max_combined_price", 0.95))))
        combined_px = round(primary_px + opp_limit, 4)
        if combined_px > max_combined:
            return False

        arb_size_usdc = round(primary_shares * opp_limit, 2)
        if arb_size_usdc < 1.0:
            return False

        guaranteed_margin = round(1.0 - combined_px, 4)
        arb_arm = {"id": "ARB_LOCK_MAKER", "offset_cents": 0.00, "ttl_ms": 5000, "is_taker": False}
        self.last_arb_attempt_ts = now

        if mode == "PAPER":
            self.paper.submit_order(
                self.current_window_ts, opp_side, arb_arm, opp_limit, arb_size_usdc,
                round(1.0 - primary_px, 4), opp_book["mid"], guaranteed_margin,
                snap["net_power_bps"], elapsed_sec, snap["regime"], mode="PAPER",
                book_bid=opp_bid, book_ask=opp_ask, exit_arm="ARB_LOCK_AUTO", entry_strategy="arb_lock",
            )
            self._set_decision(
                f"[AI双边锁利] 挂反手 {opp_side}@{opp_limit:.2f} 等份锁利 (合计{combined_px:.2f}, +{guaranteed_margin*100:.1f}%)",
                sticky=True, now=now,
            )
            return True
        elif mode == "LIVE":
            token_id = self.current_up_token if opp_side == "UP" else self.current_down_token
            try:
                res = await self.account.place_live_order(token_id, opp_limit, arb_size_usdc, False, 5000)
                self.db.save_order({
                    "order_id": res["order_id"] or f"live_arb_{int(now*1000)}",
                    "window_ts": self.current_window_ts,
                    "mode": "LIVE",
                    "side": opp_side,
                    "arm_id": "ARB_LOCK_MAKER",
                    "limit_price": opp_limit,
                    "fill_price": opp_limit,
                    "size_usdc": arb_size_usdc,
                    "shares": res["shares"],
                    "fee_usdc": 0.0,
                    "status": "FILLED",
                    "model_win_prob": round(1.0 - primary_px, 4),
                    "market_implied_prob": opp_book["mid"],
                    "edge": guaranteed_margin,
                    "net_power_bps": snap["net_power_bps"],
                    "elapsed_sec": elapsed_sec,
                    "regime": snap["regime"],
                    "official_outcome": None,
                    "pnl_usdc": 0.0,
                    "created_at": now,
                    "filled_at": now,
                    "settled_at": None,
                    "book_bid": opp_bid,
                    "book_ask": opp_ask,
                    "exit_arm": "ARB_LOCK_AUTO",
                    "entry_strategy": "arb_lock",
                    "notes": f"实盘AI双边锁利 合计{combined_px:.2f} | 盘口 {opp_bid:.2f}/{opp_ask:.2f}",
                })
                self._set_decision(f"[实盘AI锁利] {opp_side}@{opp_limit:.2f} (合计{combined_px:.2f})", sticky=True, now=now)
                return True
            except Exception as e:
                self._set_decision(f"[实盘锁利失败] {str(e)[:50]}", sticky=False, now=now)
                return False
        return False

    async def _try_early_exit(
        self,
        now: float,
        elapsed_sec: int,
        mode: str,
        primary_ord: dict,
        model_up_p: float,
        model_dn_p: float,
        up_book: dict,
        dn_book: dict,
        risk_cfg: dict,
    ) -> bool:
        """Execute 0%-fee Post-Only Maker TP OR emergency Taker SL governed by assigned exit_arm."""
        if primary_ord.get("status") != "FILLED" or mode not in ("PAPER", "LIVE"):
            return False
        side = primary_ord["side"]
        if not self.clob.is_book_fresh(side, max_age_sec=8.0) or not self.clob.is_parity_ok():
            return False

        book = up_book if side == "UP" else dn_book
        model_p = model_up_p if side == "UP" else model_dn_p
        exit_sig = self.learner.evaluate_early_exit(primary_ord, book, model_p, elapsed_sec, risk_cfg, now)
        if not exit_sig:
            return False

        is_maker = bool(exit_sig.get("is_maker", False))
        trigger_px = float(exit_sig["trigger_price"])
        ttl_ms = int(exit_sig.get("ttl_ms", 8000))

        if mode == "PAPER":
            queued = self.paper.submit_early_exit(
                primary_ord,
                exit_sig["exit_reason"],
                trigger_px,
                exit_sig["detail"],
                is_maker=is_maker,
                ttl_ms=ttl_ms,
            )
            if queued:
                self._set_decision(f"[{exit_sig['exit_reason']}] {exit_sig['detail']}", sticky=True, now=now)
                return True
        elif mode == "LIVE":
            token_id = self.current_up_token if side == "UP" else self.current_down_token
            shares = float(primary_ord.get("shares") or 0.0)
            if token_id and shares > 0:
                try:
                    res = await self.account.place_live_sell_order(
                        token_id, trigger_px, shares, is_taker=(not is_maker), ttl_ms=ttl_ms
                    )
                    exec_px = float(res.get("exec_price") or trigger_px)
                    entry_fee = float(primary_ord.get("fee_usdc") or 0.0)
                    exit_fee = (shares * HARD_LIMITS["taker_fee_rate"] * exec_px * (1.0 - exec_px)) if (not is_maker) else 0.0
                    pnl = round(shares * exec_px - float(primary_ord["size_usdc"]) - entry_fee - exit_fee, 4)
                    ord_upd = dict(primary_ord)
                    ord_upd.update({
                        "status": "CLOSED",
                        "exit_price": round(exec_px, 4),
                        "exit_reason": exit_sig["exit_reason"],
                        "fee_usdc": round(entry_fee + exit_fee, 4),
                        "pnl_usdc": pnl,
                        "settled_at": now,
                        "notes": (ord_upd.get("notes") or "") + f" | 实盘{exit_sig['exit_reason']}@{exec_px:.2f} (净{pnl:+.2f})",
                    })
                    self.db.save_order(ord_upd)
                    self._set_decision(f"[实盘{exit_sig['exit_reason']}] @{exec_px:.2f} (净{pnl:+.2f})", sticky=True, now=now)
                    return True
                except Exception as e:
                    self._set_decision(f"[实盘平仓失败] {str(e)[:55]}", sticky=False, now=now)
                    await self._send_webhook("实盘平仓失败", str(e))
        return False

    async def _submit_entry_order(
        self,
        now: float,
        elapsed_sec: int,
        effective_mode: str,
        raw_mode: str,
        in_breaker: bool,
        sig: str,
        arm: dict,
        limit_px: float,
        size_usdc: float,
        model_prob: float,
        book: dict,
        snap: dict,
        extra_note: str,
        exit_arm: str,
        entry_strategy: str,
        shadow_reason: str = "",
    ):
        if effective_mode == "SHADOW":
            self.paper.submit_order(
                self.current_window_ts, sig, arm, limit_px, size_usdc,
                model_prob, book["mid"], model_prob - limit_px, snap["net_power_bps"], elapsed_sec, snap["regime"],
                mode="SHADOW", book_bid=book["best_bid"], book_ask=book["best_ask"],
                extra_note=extra_note, exit_arm=exit_arm, entry_strategy=entry_strategy,
            )
            if not shadow_reason:
                self.last_signal_ts = now
            reason = shadow_reason or ("暂停" if raw_mode == "PAUSED" else ("熔断" if in_breaker else "日亏上限"))
            self._set_decision(f"[{reason}] 影子单({extra_note}) {sig}@{limit_px:.2f}", sticky=False, now=now)
            return

        if effective_mode == "PAPER":
            self.paper.submit_order(
                self.current_window_ts, sig, arm, limit_px, size_usdc,
                model_prob, book["mid"], model_prob - limit_px, snap["net_power_bps"], elapsed_sec, snap["regime"],
                mode="PAPER", book_bid=book["best_bid"], book_ask=book["best_ask"],
                extra_note=extra_note, exit_arm=exit_arm, entry_strategy=entry_strategy,
            )
            self.last_signal_ts = now
            self.round_orders_count += 1
            self._set_decision(f"[{extra_note}] {sig} @{limit_px:.2f} (${size_usdc})", sticky=True, now=now)
        elif effective_mode == "LIVE":
            token_id = self.current_up_token if sig == "UP" else self.current_down_token
            try:
                res = await self.account.place_live_order(token_id, limit_px, size_usdc, bool(arm.get("is_taker")), int(arm["ttl_ms"]))
                self.db.save_order({
                    "order_id": res["order_id"] or f"live_{int(now*1000)}",
                    "window_ts": self.current_window_ts,
                    "mode": "LIVE",
                    "side": sig,
                    "arm_id": arm["id"],
                    "limit_price": limit_px,
                    "fill_price": limit_px,
                    "size_usdc": size_usdc,
                    "shares": res["shares"],
                    "fee_usdc": 0.0,
                    "status": "FILLED",
                    "model_win_prob": model_prob,
                    "market_implied_prob": book["mid"],
                    "edge": round(model_prob - limit_px, 4),
                    "net_power_bps": snap["net_power_bps"],
                    "elapsed_sec": elapsed_sec,
                    "regime": snap["regime"],
                    "official_outcome": None,
                    "pnl_usdc": 0.0,
                    "created_at": now,
                    "filled_at": now,
                    "settled_at": None,
                    "book_bid": book["best_bid"],
                    "book_ask": book["best_ask"],
                    "exit_arm": exit_arm,
                    "entry_strategy": entry_strategy,
                    "notes": f"实盘[{extra_note}] 离场:{exit_arm} | 盘口 {book['best_bid']:.2f}/{book['best_ask']:.2f}",
                })
                self.last_signal_ts = now
                self.round_orders_count += 1
                self._set_decision(f"[实盘·{extra_note}] {sig} @{limit_px:.2f} (${size_usdc})", sticky=True, now=now)
            except Exception as e:
                self._set_decision(f"[实盘失败] {str(e)[:60]}", sticky=False, now=now)
                await self._send_webhook("实盘下单失败", str(e))

    async def _evaluate_and_trade(self, now, elapsed_sec, snap, model_up_p, model_dn_p, up_book, dn_book, up_edge, dn_edge):
        risk_cfg = self.db.get_json("risk_config", {})
        auto_params = self.learner.get_auto_params()

        sig, block_reason, trigger_src, entry_strat = self.learner.evaluate_signal_with_reason(
            snap, elapsed_sec,
            up_mid=float(up_book.get("mid") or 0.5),
            dn_mid=float(dn_book.get("mid") or 0.5),
            enable_orderflow_lead=True,
        )
        self.latest_pricing.update({
            "model_up_prob": model_up_p,
            "model_down_prob": model_dn_p,
            "bs_up_prob": snap.get("bs_up_prob", 0.5),
            "moneyness_z": snap.get("moneyness_z", 0.0),
            "iv_rv_ratio": snap.get("iv_rv_ratio", 1.0),
            "iv_state": snap.get("iv_state", "FAIR"),
            "orderflow_score": snap.get("orderflow_score", 0.0),
            "up_edge": up_edge,
            "down_edge": dn_edge,
            "signal": sig or "NONE",
            "trigger_source": trigger_src,
        })
        if block_reason:
            b_cat = "尾盘跳过" if ("尾盘" in block_reason or "胜负已定价" in block_reason) else (
                "逆势拦截" if ("死猫跳" in block_reason or "反抽" in block_reason) else (
                    "波动保护" if "IV/RV" in block_reason else "订单流过滤"
                )
            )
            self._record_block(block_reason, b_cat, now=now)

        if elapsed_sec < 4 or self.current_ptb is None or not self.current_up_token:
            if sig:
                self._set_decision("等待新场次官方基准价与真实盘口同步", sticky=False, now=now)
            return

        mode = self.db.get_kv("trading_mode", "PAPER")
        in_breaker = now < self.circuit_breaker_until
        perf = self.db.get_performance_stats(mode if mode in ("PAPER", "LIVE") else "PAPER")
        daily_loss_hit = perf["daily_pnl"] <= -abs(float(risk_cfg.get("max_daily_loss_usdc", 100.0)))
        effective_mode = "SHADOW" if (mode == "PAUSED" or in_breaker or daily_loss_hit) else mode

        pos_state = self._get_round_position_state(self.current_window_ts, effective_mode)
        cooldown_now = max(10, int(auto_params.get("cooldown_sec", 15)))

        # Auto-expire stale transient decisions (>15s old or from a prior round) ONLY when no fresh signal/block this tick
        decision_stale = (
            self._decision_window_ts != self.current_window_ts
            or ((not self._decision_sticky) and (now - self._decision_ts >= 15.0))
        )
        if decision_stale and not sig and not block_reason:
            if pos_state["has_pending"]:
                live_msg = "挂单撮合中，等待盘口成交"
            elif pos_state["primary_filled"] is not None:
                pf0 = pos_state["primary_filled"]
                if pf0.get("status") == "CLOSED":
                    live_msg = f"本场已提前平仓（{pf0.get('exit_reason') or '已平仓'}），等待下场"
                elif pos_state["arb_filled"] is not None:
                    live_msg = "本场已完成双边无风险锁利，等待官方结算"
                else:
                    p0_px = float(pf0.get("fill_price") or pf0["limit_price"])
                    live_msg = f"本场已持主仓 {pf0['side']}@{p0_px:.2f}（实时监控离场中）"
            elif mode == "PAUSED":
                live_msg = "策略已暂停（仅影子学习中）"
            elif in_breaker:
                rem_brk = max(1, int(self.circuit_breaker_until - now))
                live_msg = f"连亏熔断保护中（剩{rem_brk}s，仅影子学习）"
            elif daily_loss_hit:
                live_msg = "已触及单日亏损上限（仅影子学习）"
            elif self.last_signal_ts > 0 and (now - self.last_signal_ts) < cooldown_now:
                rem_cd = max(1, int(cooldown_now - (now - self.last_signal_ts)))
                live_msg = f"信号冷却中（剩{rem_cd}s）"
            else:
                live_msg = "巡航监听中（暂无触发信号）"
            if live_msg != self.latest_pricing.get("last_decision") or self._decision_window_ts != self.current_window_ts:
                self.latest_pricing["last_decision"] = live_msg
                self.latest_pricing["last_decision_ts"] = round(now, 1)
                self._decision_ts = now
                self._decision_window_ts = self.current_window_ts

        # 1. Never overlap multiple active pending orders in the same round
        if pos_state["has_pending"]:
            return

        # 2. If primary order already filled or closed in this round:
        if pos_state["primary_filled"] is not None:
            pf = pos_state["primary_filled"]
            if pf.get("status") == "CLOSED":
                return
            if pos_state["arb_filled"] is not None:
                if sig:
                    self._set_decision("本场已完成双边无风险锁利，等待官方结算", sticky=False, now=now)
                return

            if effective_mode in ("PAPER", "LIVE"):
                # Priority 1: Check 0%-fee Maker TP or Emergency SL (governed by assigned exit_arm)
                exited = await self._try_early_exit(
                    now, elapsed_sec, effective_mode, pf, model_up_p, model_dn_p, up_book, dn_book, risk_cfg
                )
                if exited:
                    return
                # Priority 2: Check equal-shares risk-free Arb Lock (active when exit_arm == 'ARB_LOCK_AUTO')
                locked = await self._try_arb_lock_hedge(
                    now, elapsed_sec, effective_mode, pf, up_book, dn_book, snap
                )
                if locked:
                    return

            if sig:
                p_px = float(pf.get("fill_price") or pf["limit_price"])
                self._set_decision(f"本场已持主仓 {pf['side']}@{p_px:.2f}（单场限1单保护）", sticky=False, now=now)
            return

        # 3. No filled primary position yet -> cancelled orders NEVER block finding new opportunities!
        if now - self.last_cancel_ts < 2.0:
            if sig:
                self._set_decision(f"撤单防抖中，稍后重估 {sig}({trigger_src})", sticky=False, now=now)
            return
        cooldown = max(10, int(auto_params.get("cooldown_sec", 15)))
        if self.last_signal_ts > 0 and (now - self.last_signal_ts) < cooldown:
            if sig:
                rem_cd = max(1, int(cooldown - (now - self.last_signal_ts)))
                self._set_decision(f"信号冷却中（剩{rem_cd}s），暂缓 {sig}({trigger_src})", sticky=False, now=now)
            return

        # 3a. Final-60s TWAP Rule Mispricing Sniper (autonomous ACTIVE/SHADOW self-gating)
        sniper = self.learner.evaluate_twap_sniper(snap, elapsed_sec, up_book, dn_book, risk_cfg)
        if sniper:
            s_side = sniper["side"]
            if self.clob.is_book_fresh(s_side, max_age_sec=8.0) and self.clob.is_parity_ok():
                s_book = up_book if s_side == "UP" else dn_book
                bankroll = self.paper.get_balance() if mode != "LIVE" else max(10.0, float(self.account.cached_portfolio.get("usdc_balance", 0.0)))
                size_usdc = self.learner.calculate_position_size(
                    bankroll, sniper["true_prob"], sniper["limit_price"],
                    bool(sniper["arm"].get("is_taker")), risk_cfg,
                    remaining_sec=int(snap.get("remaining_sec", 30)),
                    is_twap_sniper=True,
                )
                if size_usdc > 0:
                    s_mode = "SHADOW" if sniper.get("shadow_only") else effective_mode
                    if s_mode == "SHADOW":
                        # Avoid spamming shadow sniper orders in the same window
                        sh_state = self._get_round_position_state(self.current_window_ts, "SHADOW")
                        if sh_state["has_pending"] or sh_state["primary_filled"] is not None:
                            s_mode = ""
                    if s_mode:
                        self.latest_pricing["signal"] = s_side
                        await self._submit_entry_order(
                            now, elapsed_sec, s_mode, mode, in_breaker,
                            s_side, sniper["arm"], sniper["limit_price"], size_usdc,
                            sniper["true_prob"], s_book, snap, extra_note="尾盘TWAP狙击",
                            exit_arm="HOLD_EXPIRY", entry_strategy="twap_sniper",
                            shadow_reason="尾盘子策略影子恢复中" if sniper.get("shadow_only") else "",
                        )
                        if not sniper.get("shadow_only"):
                            return

        # 4. Standard Path: Leading Perp Orderflow or Quant Elder Momentum Trigger
        if not sig:
            return
        if not self.clob.is_book_fresh(sig, max_age_sec=8.0) or not self.clob.is_parity_ok():
            self._record_block("实时订单簿延迟或双边未对齐，跳过下单", "盘口异常", now=now)
            return

        book = up_book if sig == "UP" else dn_book
        if book["best_bid"] <= 0 or book["best_ask"] <= 0 or book["best_bid"] > book["best_ask"]:
            return

        # Check whether this entry sub-strategy is autonomously hibernated in SHADOW mode
        sub_shadow = (self.learner.get_sub_strategy_status(entry_strat) == "SHADOW")
        target_mode = "SHADOW" if sub_shadow else effective_mode
        if sub_shadow:
            sh_state = self._get_round_position_state(self.current_window_ts, "SHADOW")
            if sh_state["has_pending"] or sh_state["primary_filled"] is not None:
                self._record_block(f"{trigger_src}处于影子恢复中（本场已录影子单）", "影子休眠", now=now)
                return

        model_prob = model_up_p if sig == "UP" else model_dn_p
        edge_est = model_prob - book["best_bid"]
        recent_cancel_same = (self.last_cancel_side == sig and (now - self.last_cancel_ts) <= 15.0)
        arm = self.learner.select_execution_arm(
            edge_est,
            snap["regime"] == "TRENDING" or entry_strat in ("orderflow_lead", "energy_accum"),
            recent_cancel_same_side=recent_cancel_same,
            best_ask=book["best_ask"],
        )
        max_px = float(auto_params.get("max_entry_price", 0.75))
        limit_px = self.learner.compute_order_price(arm, book["best_bid"], book["best_ask"], max_px)
        if limit_px is None:
            self._record_block(
                f"{sig}({trigger_src})盘口 {book['best_bid']:.2f}/{book['best_ask']:.2f} 超出AI赔率上限({max_px:.2f})",
                "赔率超限", now=now,
            )
            return

        bankroll = self.paper.get_balance() if mode != "LIVE" else max(10.0, float(self.account.cached_portfolio.get("usdc_balance", 0.0)))
        rem_sec = int(snap.get("remaining_sec", max(1, 300 - elapsed_sec)))
        z_now = float(snap.get("moneyness_z", 0.0))
        is_buying_itm = (sig == "UP" and z_now > 0.35) or (sig == "DOWN" and z_now < -0.35)
        size_usdc = self.learner.calculate_position_size(
            bankroll, model_prob, limit_px, bool(arm.get("is_taker")), risk_cfg,
            remaining_sec=rem_sec, iv_state=str(snap.get("iv_state", "FAIR")), is_buying_itm=is_buying_itm,
        )

        if size_usdc <= 0:
            req_edge, _ = self.learner.compute_dynamic_min_edge(
                limit_px, risk_cfg, remaining_sec=rem_sec,
                iv_state=str(snap.get("iv_state", "FAIR")), is_buying_itm=is_buying_itm,
            )
            fee_ps = HARD_LIMITS["taker_fee_rate"] * limit_px * (1.0 - limit_px) if bool(arm.get("is_taker")) else 0.0
            net_e = (model_prob - (limit_px + fee_ps)) * 100.0
            self._record_block(
                f"{sig}({trigger_src})未达动态门槛 (净{net_e:+.1f}% < {limit_px:.2f}价位需{req_edge*100:.1f}%)",
                "优势不足", now=now,
            )
            return

        chosen_exit_arm = self.learner.select_exit_arm(entry_strat, str(snap.get("regime", "CHOPPY")), elapsed_sec)
        await self._submit_entry_order(
            now, elapsed_sec, target_mode, mode, in_breaker,
            sig, arm, limit_px, size_usdc, model_prob, book, snap,
            extra_note=trigger_src or "动量突破",
            exit_arm=chosen_exit_arm,
            entry_strategy=entry_strat or "momentum_breakout",
            shadow_reason=(f"{trigger_src}影子恢复中" if sub_shadow else ""),
        )

    def update_auto_train_config(self, enabled: bool, interval_hours: int, min_rounds: int = 16) -> dict:
        interval_hours = max(1, min(168, int(interval_hours)))
        min_rounds = max(12, min(500, int(min_rounds)))
        cfg = {"enabled": bool(enabled), "interval_hours": interval_hours, "min_rounds": min_rounds}
        self.db.set_json("auto_train_config", cfg)
        self.training_status.update({
            "auto_enabled": cfg["enabled"],
            "interval_hours": cfg["interval_hours"],
            "min_rounds": cfg["min_rounds"],
            "next_auto_train_ts": self._last_auto_train_ts + cfg["interval_hours"] * 3600,
        })
        return cfg

    def reset_learning_dataset(self, clear_models: bool = True) -> dict:
        """Reset recorded full-round slices & rounds, optionally clearing offline models back to baseline."""
        res = self.db.reset_dataset(clear_models=clear_models)
        self.round_episode_samples = []
        self.last_settled_round = None
        if clear_models:
            self.learner.offline_model = None
            self.learner.active_version_id = "online_baseline_v0"
            try:
                for p in MODELS_DIR.glob("*.pkl"):
                    p.unlink(missing_ok=True)
            except Exception:
                pass
        self.training_status["last_result"] = None
        return res

    async def trigger_offline_training(self) -> dict:
        if self.training_status["running"]:
            return {"ok": False, "reason": "离线训练任务正在运行中"}
        self.training_status["running"] = True
        try:
            min_r = int(self.training_status.get("min_rounds", 16))
            res = await asyncio.to_thread(run_walk_forward_training, self.db, min_r)
            now = time.time()
            self._last_auto_train_ts = now
            self.training_status["last_run_ts"] = now
            self.training_status["last_result"] = res
            self.training_status["next_auto_train_ts"] = now + int(self.training_status.get("interval_hours", 12)) * 3600
            if res.get("ok") and res.get("role") == "CHAMPION":
                self._load_champion_model()
                await self._send_webhook(
                    "模型自迭代上线",
                    f"新主模型 {res['version_id']} ({res['model_type']}) 已热加载 | 验证误差={res['val_brier']}, 区分度={res['val_auc']}",
                )
            return res
        finally:
            self.training_status["running"] = False

    async def _periodic_maintenance_loop(self):
        last_clean = time.time()
        last_bal = 0.0
        last_recover = time.time()
        last_clock = time.time()
        while not self._stop:
            now = time.time()
            # Re-check UTC clock drift every 5 minutes so OS time fixes reflect automatically without restart
            if now - last_clock >= 300:
                last_clock = now
                await self.oracle.check_clock_drift()

            # Periodic orphan settlement sweep every 60s so no order ever stays stuck
            if now - last_recover >= 60:
                last_recover = now
                await self.recover_unsettled_windows()

            # Refresh wallet balances every 45s if wallet configured
            if now - last_bal >= 45:
                last_bal = now
                if self.account.get_public_wallet_info().get("has_private_key"):
                    await self.account.refresh_balances_and_portfolio()

            # Automatic scheduled offline Walk-Forward training
            if self.training_status.get("auto_enabled", True):
                interval_sec = int(self.training_status.get("interval_hours", 12)) * 3600
                self.training_status["next_auto_train_ts"] = self._last_auto_train_ts + interval_sec
                if now - self._last_auto_train_ts >= interval_sec:
                    self._last_auto_train_ts = now
                    await self.trigger_offline_training()

            # Cleanup >7d episode slices daily to keep VPS disk slim
            if now - last_clean >= 86400:
                last_clean = now
                self.db.cleanup_old_data(retain_days=7)
                await self.oracle.check_geo_and_clock()

            await asyncio.sleep(10)

    def _format_round_position(self, mode: str) -> str:
        if not self.current_window_ts:
            return "本场空仓"
        m = mode if mode in ("PAPER", "LIVE") else "PAPER"
        ps = self._get_round_position_state(self.current_window_ts, m)
        if ps["primary_filled"] and ps["arb_filled"]:
            p1 = float(ps["primary_filled"].get("fill_price") or ps["primary_filled"]["limit_price"])
            p2 = float(ps["arb_filled"].get("fill_price") or ps["arb_filled"]["limit_price"])
            return f"已双边锁利 (合计{p1+p2:.2f})"
        if ps["primary_filled"]:
            pf = ps["primary_filled"]
            if pf.get("status") == "CLOSED":
                return f"{pf.get('exit_reason') or '已提前平仓'} ({float(pf.get('pnl_usdc') or 0):+.2f})"
            px = float(pf.get("fill_price") or pf["limit_price"])
            cur_bid = float(self.clob.books[pf["side"]].get("best_bid") or 0.0)
            if cur_bid > 0:
                sh = float(pf.get("shares") or (float(pf["size_usdc"]) / max(0.01, px)))
                u_pnl = sh * cur_bid - float(pf["size_usdc"])
                return f"持 {pf['side']}@{px:.2f} (浮{u_pnl:+.2f})"
            return f"已持主仓 {pf['side']}@{px:.2f}"
        if ps["has_pending"]:
            return "挂单等待成交"
        return "本场空仓"

    @staticmethod
    def _resolve_chart_book_cents(book: dict, opp_book: dict, prev_val: Optional[float]) -> Optional[float]:
        """Resolve continuous 0-100c chart quote even when one side or both sides of the book are empty."""
        bb = float(book.get("best_bid") or 0.0)
        ba = float(book.get("best_ask") or 0.0)
        if bb > 0.0 and ba > 0.0:
            return round((bb + ba) * 50.0, 1)
        if bb > 0.0:
            return round(bb * 100.0, 1)
        if ba > 0.0:
            return round(ba * 100.0, 1)
        lt = float(book.get("last_trade") or 0.0)
        if 0.0 < lt < 1.0:
            return round(lt * 100.0, 1)
        obb = float(opp_book.get("best_bid") or 0.0)
        oba = float(opp_book.get("best_ask") or 0.0)
        if obb > 0.0 and oba > 0.0:
            return round((1.0 - 0.5 * (obb + oba)) * 100.0, 1)
        if obb > 0.0:
            return round((1.0 - obb) * 100.0, 1)
        if oba > 0.0:
            return round((1.0 - oba) * 100.0, 1)
        if prev_val is not None:
            return float(prev_val)
        return None

    def _build_round_chart_events(self, mode: str) -> list[dict]:
        """Build lightweight in-round order/fill/exit markers for the 0-300s orderbook chart."""
        if not self.current_window_ts:
            return []
        eff_mode = mode if mode in ("PAPER", "LIVE") else "PAPER"
        orders = [o for o in self.db.get_orders_by_window(self.current_window_ts) if o.get("mode") == eff_mode]
        events: list[dict] = []
        win_ts = float(self.current_window_ts)
        for o in orders:
            st = o.get("status")
            side = str(o.get("side") or "")
            if st in ("FILLED", "SETTLED", "CLOSED") and o.get("fill_price") is not None:
                fill_px = float(o["fill_price"])
                f_ts = float(o.get("filled_at") or o.get("created_at") or win_ts)
                f_sec = max(0, min(300, int(round(f_ts - win_ts))))
                is_arb = (o.get("arm_id") == "ARB_LOCK_MAKER")
                events.append({
                    "kind": "ARB" if is_arb else "ENTRY",
                    "side": side,
                    "sec": f_sec,
                    "price": round(fill_px * 100.0, 1),
                    "label": f"{'锁' if is_arb else '买'}{side} {round(fill_px * 100):.0f}¢",
                    "active_cost": (not is_arb) and (st == "FILLED"),
                })
            if st == "CLOSED" and o.get("exit_price") is not None:
                ex_px = float(o["exit_price"])
                ex_ts = float(o.get("settled_at") or o.get("filled_at") or win_ts)
                ex_sec = max(0, min(300, int(round(ex_ts - win_ts))))
                reason = str(o.get("exit_reason") or "")
                is_sl = ("止损" in reason)
                events.append({
                    "kind": "EXIT_SL" if is_sl else "EXIT_TP",
                    "side": side,
                    "sec": ex_sec,
                    "price": round(ex_px * 100.0, 1),
                    "label": f"{'止损' if is_sl else '止盈'} {round(ex_px * 100):.0f}¢",
                    "active_cost": False,
                })
        return events

    def get_live_state_payload(self) -> dict:
        now = time.time()
        elapsed = max(0, min(300, int(now - self.current_window_ts))) if self.current_window_ts else 0
        remaining = 300 - elapsed
        mode = self.db.get_kv("trading_mode", "PAPER")

        ptb_str = str(self.current_ptb.quantize(Decimal("0.01"))) if self.current_ptb is not None else "--"
        twap_str = str(self.oracle.latest_twap60.quantize(Decimal("0.01"))) if self.oracle.latest_twap60 > 0 else "--"
        twap_diff = None
        if self.current_ptb is not None and self.oracle.latest_twap60 > 0:
            twap_diff = str((self.oracle.latest_twap60 - self.current_ptb).quantize(Decimal("0.01")))
        best_spot = self.cex.get_best_spot()
        oracle_spot_val = round(best_spot - self.basis_ema, 2) if best_spot > 0 else (
            round(self.oracle.latest_spot_float, 2) if self.oracle.latest_spot_float > 0 else 0.0
        )
        spot_twap_diff = None
        if oracle_spot_val > 0 and self.oracle.latest_twap60 > 0:
            spot_twap_diff = f"{(oracle_spot_val - float(self.oracle.latest_twap60)):.2f}"

        return {
            "ts": now,
            "mode": mode,
            "system": self.monitor.sample(),
            "feeds": {
                "binance": self.cex.status["binance"],
                "binance_perp": self.cex.status["binance_perp"],
                "polybolt": self.oracle.status["polybolt"],
                "rtds": self.oracle.status["rtds"],
                "active_oracle": self.oracle.active_oracle_source,
                "gamma": self.oracle.status["gamma"],
                "clob": self.clob.refresh_status_metrics(),
                "geo_check": self.oracle.status["geo_check"],
            },
            "round": {
                "window_ts": self.current_window_ts,
                "slug": self.current_slug,
                "elapsed_sec": elapsed,
                "remaining_sec": remaining,
                "price_to_beat": ptb_str,
                "ptb_source": self.current_ptb_source,
                "oracle_source": self.oracle.active_oracle_source,
                "twap60": twap_str,
                "twap_diff_usd": twap_diff,
                "oracle_spot": oracle_spot_val,
                "spot_twap_diff_usd": spot_twap_diff,
                "binance_spot": round(best_spot, 2),
                "basis_usd": round(self.basis_ema, 2),
                "last_settled": self.last_settled_round,
                "pending_settlement_count": len(self.pending_settlements),
            },
            "orderbook": {
                "UP": self.clob.books["UP"],
                "DOWN": self.clob.books["DOWN"],
            },
            "strategy": {
                "features": self.features.latest_snapshot,
                "pricing": self.latest_pricing,
                "cooldown_remaining": max(0, int(self.learner.get_auto_params().get("cooldown_sec", 15) - (now - self.last_signal_ts))) if self.last_signal_ts > 0 else max(0, int(2.0 - (now - self.last_cancel_ts))),
                "consecutive_losses": self.consecutive_losses,
                "circuit_breaker_remaining": max(0, int(self.circuit_breaker_until - now)),
                "round_position": self._format_round_position(mode),
            },
            "paper_balance": self.paper.get_balance(),
            "stats": self.db.get_performance_stats(mode if mode in ("PAPER", "LIVE") else "PAPER"),
            "learning": self.learner.get_dashboard_summary(),
            "dataset_counts": self.db.get_slice_counts(),
            "training_status": self.training_status,
            "wallet": self.account.get_public_wallet_info(),
            "portfolio": self.account.cached_portfolio,
            "chart_points": list(self.live_chart_buffer),
            "round_events": self._build_round_chart_events(mode),
        }
