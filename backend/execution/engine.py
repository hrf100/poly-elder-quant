import asyncio
import json
import os
import time
from collections import deque
from decimal import Decimal
from typing import Optional
import aiohttp

try:
    import fcntl
except ImportError:  # Windows fallback
    fcntl = None

from backend.config import DATA_DIR, HARD_LIMITS, MODELS_DIR
from backend.execution.live_clob import PolymarketAccountManager
from backend.execution.paper_engine import PaperExecutionEngine
from backend.feeds.cex_feeds import CexPriceFeed
from backend.feeds.clock import ServerClock
from backend.feeds.polymarket_clob import PolymarketClobFeed
from backend.feeds.polymarket_oracle import PolymarketOracleSync
from backend.monitor.system_stats import SystemMonitor
from backend.storage.db import Database
from backend.strategy.offline_trainer import load_model_artifact, run_walk_forward_training
from backend.strategy.fv_value_strategy import FVDiscountLeaderStrategy
from backend.strategy.online_learner import SelfLearningCoordinator
from backend.strategy.quant_elder import QuantElderFeatureEngine


ARB_ARM_IDS = ("ARB_LOCK_MAKER", "ARB_LOCK_TAKER")


class QuantTradingEngine:
    """
    Main asynchronous orchestrator:
    - Keeps Gamma API Price-to-Beat & PolyBolt 60s TWAP strictly synchronized with Polymarket.
    - Runs 1s Quant Elder feature engine & 5s full-episode trajectory recorder.
    - Drives Online Self-Learning + Paper/Shadow/Live execution + 12h Low-Priority Offline Training.
    """

    def __init__(self, db: Database):
        self.db = db
        self.clock = ServerClock()
        self.monitor = SystemMonitor(db)
        self.account = PolymarketAccountManager(db, clock=self.clock, on_live_order_update=self._on_live_order_update)
        self.paper = PaperExecutionEngine(db, on_order_cancelled=self._on_paper_order_cancelled)
        self.learner = SelfLearningCoordinator(db)
        self.fv_strategy = FVDiscountLeaderStrategy()
        self.features = QuantElderFeatureEngine(
            ema_period=self.learner.state.get("ema_period", 13),
            hl_window=self.learner.state.get("hl_window", 5),
        )

        self.cex = CexPriceFeed(on_price=self._on_cex_price)
        self.oracle = PolymarketOracleSync(on_oracle_tick=self._on_oracle_tick, clock=self.clock)
        self.clob = PolymarketClobFeed(on_book_update=self._on_book_update, clock=self.clock)

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
        self.current_tick_size: float = 0.01
        self.current_min_size: float = 5.0
        self.current_taker_fee_rate: float = float(HARD_LIMITS["taker_fee_rate"])
        self.current_accepting_orders: bool = True
        self.current_neg_risk: bool = False
        self.current_market_version: int = 1
        self._pending_live_exits: dict[str, dict] = {}
        self._trade_lock = asyncio.Lock()
        self._instance_lock_fp = None
        self.is_duplicate_instance: bool = False
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
        self.round_arb_attempts: int = 0
        self.last_exit_attempt_ts: float = 0.0
        self.round_live_cancel_failed: bool = False
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

    def _acquire_single_instance_lock(self) -> bool:
        """Acquire non-blocking OS file lock so two server processes can NEVER trade on the same DB."""
        if fcntl is None:
            return True
        lock_path = DATA_DIR / ".engine.lock"
        try:
            fp = open(lock_path, "a+", encoding="utf-8")
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fp.seek(0)
            fp.truncate()
            fp.write(f"{os.getpid()}\n")
            fp.flush()
            self._instance_lock_fp = fp
            self.is_duplicate_instance = False
            return True
        except BlockingIOError:
            self.is_duplicate_instance = True
            return False
        except Exception:
            return True

    def _release_single_instance_lock(self):
        fp = self._instance_lock_fp
        self._instance_lock_fp = None
        if fp is not None:
            try:
                if fcntl is not None:
                    fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                fp.close()
            except Exception:
                pass

    async def start(self):
        self._load_champion_model()
        if not self._acquire_single_instance_lock():
            self._set_decision(
                "检测到已有另一进程正在运行交易引擎，本实例已自动锁定为只读保护模式（防止同场重复下单）",
                sticky=True,
            )
            return
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
        self.account.start_background(asyncio.get_running_loop())

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
        self.account.stop_background()
        self.cex.stop()
        self.oracle.stop()
        self.clob.stop()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.oracle.close()
        await self.clob.close()
        self._release_single_instance_lock()

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

    def _on_live_order_update(self, ev: dict):
        """
        Authoritative CLOB V2 real-time fill / cancel / TTL reconciliation callback
        (driven by UserSpec WS, TTL auto-cancel reconciliation, and 20s list_account_trades sync).
        """
        event_kind = str(ev.get("event") or "")
        oid = str(ev.get("order_id") or "")
        if not oid:
            return
        now = time.time()

        # Case 1: Pending LIVE early-exit SELL order
        if oid in self._pending_live_exits:
            ex_meta = self._pending_live_exits[oid]
            primary_id = ex_meta["primary_order_id"]
            primary_ord = self.db.get_order(primary_id)
            if not primary_ord or primary_ord.get("status") != "FILLED":
                self._pending_live_exits.pop(oid, None)
                return

            matched_sh = float(ev.get("shares") or ev.get("size_matched") or 0.0)
            if matched_sh > 0.0:
                exec_px = float(ev.get("fill_price") or ev.get("price") or ex_meta["trigger_px"])
                is_maker = bool(ex_meta.get("is_maker", True))
                entry_fee = float(primary_ord.get("fee_usdc") or 0.0)
                exit_fee = float(ev.get("fee_usdc") or 0.0)
                if exit_fee <= 0.0 and not is_maker:
                    exit_fee = matched_sh * self.current_taker_fee_rate * exec_px * (1.0 - exec_px)
                entry_shares = float(primary_ord.get("shares") or matched_sh)
                closed_cost = float(primary_ord["size_usdc"]) * min(1.0, matched_sh / max(0.01, entry_shares))
                pnl = round(matched_sh * exec_px - closed_cost - entry_fee - exit_fee, 4)

                upd = dict(primary_ord)
                upd.update({
                    "status": "CLOSED",
                    "exit_price": round(exec_px, 4),
                    "exit_reason": ex_meta["exit_reason"],
                    "fee_usdc": round(entry_fee + exit_fee, 4),
                    "pnl_usdc": pnl,
                    "settled_at": now,
                    "notes": (upd.get("notes") or "") + f" | 实盘确权{ex_meta['exit_reason']}@{exec_px:.2f} ({matched_sh:.2f}份, 净{pnl:+.2f})",
                })
                self.db.save_order(upd)
                self._pending_live_exits.pop(oid, None)
                self._set_decision(f"[实盘{ex_meta['exit_reason']}成交] @{exec_px:.2f} (净{pnl:+.2f})", sticky=True, now=now)
                try:
                    asyncio.get_running_loop().create_task(self._audit_live_settlement(int(upd["window_ts"]), [upd]))
                except RuntimeError:
                    pass
            elif event_kind == "trade_failed":
                self._pending_live_exits.pop(oid, None)
                self._set_decision(f"[实盘平仓链上失败] {ex_meta['exit_reason']} 未成交，恢复持仓监控", sticky=True, now=now)
            elif event_kind == "cancel_failed":
                err_txt = str(ev.get("error") or "撤卖单未确认")[:70]
                self.round_live_cancel_failed = True
                self._set_decision(f"[实盘撤卖失败·锁定] {err_txt}", sticky=True, now=now)
                try:
                    asyncio.get_running_loop().create_task(self._send_webhook("实盘撤卖单未确认", f"订单 {oid}: {err_txt}"))
                except RuntimeError:
                    pass
            elif event_kind in ("order_closed", "ttl_reconciled"):
                if ev.get("cancel_confirmed") is True:
                    self._pending_live_exits.pop(oid, None)
                    self._set_decision(f"[实盘挂卖已撤] {ex_meta['exit_reason']}超时确认撤单，继续监控持仓", sticky=False, now=now)
                else:
                    self.round_live_cancel_failed = True
                    self._set_decision(f"[实盘挂卖待核] {ex_meta['exit_reason']}撤单待交易所确认，暂锁定平仓", sticky=True, now=now)
            return

        # Case 2: LIVE entry or arb-lock BUY order
        ord_row = self.db.get_order(oid)
        if not ord_row or ord_row.get("mode") != "LIVE":
            return
        if ord_row.get("status") in ("SETTLED", "CLOSED"):
            return

        # Explicit on-chain FAILED / ghost fill revert -> roll back to FAILED immediately!
        if event_kind == "trade_failed":
            reason_f = str(ev.get("reason") or ev.get("status") or "链上结算失败")[:80]
            bal_b = ord_row.get("bal_before")
            bal_a = ev.get("bal_after_entry")
            upd = dict(ord_row)
            upd.update({
                "status": "FAILED",
                "fill_price": None,
                "shares": 0.0,
                "pnl_usdc": 0.0,
                "settled_at": now,
                "bal_after_entry": bal_a if bal_a is not None else upd.get("bal_after_entry"),
                "bal_after_settle": bal_a if bal_a is not None else upd.get("bal_after_settle"),
                "recon_status": "OK" if (bal_b is not None and bal_a is not None and abs(float(bal_a) - float(bal_b)) <= 0.15) else "MISMATCH",
                "recon_diff": round(float(bal_a) - float(bal_b), 4) if (bal_b is not None and bal_a is not None) else 0.0,
                "recon_note": f"链上失败未成交: {reason_f}",
                "notes": (upd.get("notes") or "") + f" | 实盘未成回滚({reason_f})",
            })
            self.db.save_order(upd)
            self.account._local_reserved_usdc = 0.0
            self._set_decision(f"[实盘链上失败·已清理] {upd['side']}@{upd['limit_price']:.2f} {reason_f}", sticky=True, now=now)
            try:
                asyncio.get_running_loop().create_task(self.account.refresh_balances_and_portfolio(force=True))
                asyncio.get_running_loop().create_task(self._send_webhook("实盘链上失败/未成交告警", f"订单 {oid} ({upd['side']}@{upd['limit_price']}): {reason_f}"))
            except RuntimeError:
                pass
            return

        matched_sh = float(ev.get("shares") or ev.get("size_matched") or 0.0)
        if matched_sh > 0.0:
            fill_px = float(ev.get("fill_price") or ev.get("price") or ord_row["limit_price"])
            real_cost = round(matched_sh * fill_px, 4)
            fee_usdc = float(ev.get("fee_usdc") or ord_row.get("fee_usdc") or 0.0)
            if fee_usdc <= 0.0 and ord_row.get("arm_id") in ("TAKER_FAST", "ARB_LOCK_TAKER", "TWAP_SNIPER_TAKER"):
                fee_usdc = round(matched_sh * self.current_taker_fee_rate * fill_px * (1.0 - fill_px), 4)

            is_chain_confirmed = (event_kind == "settlement_confirmed") or bool(ev.get("confirmed"))
            bal_b = ord_row.get("bal_before")
            bal_a = ev.get("bal_after_entry")
            recon_st = ord_row.get("recon_status") or "PENDING"
            recon_diff = ord_row.get("recon_diff")
            recon_note = ord_row.get("recon_note")
            if bal_b is not None and bal_a is not None:
                expected_spend = real_cost + fee_usdc
                actual_spend = float(bal_b) - float(bal_a)
                recon_diff = round(actual_spend - expected_spend, 4)
                if abs(recon_diff) <= 0.20:
                    recon_st = "OK"
                elif actual_spend < 0.25 and expected_spend >= 1.0:
                    recon_st = "MISMATCH"
                else:
                    recon_st = "WARN"
                recon_note = f"入场核对: 前${float(bal_b):.2f}→后${float(bal_a):.2f} (实扣${actual_spend:.2f}/理论${expected_spend:.2f}, 差{recon_diff:+.2f})"

            upd = dict(ord_row)
            upd.update({
                "status": "FILLED" if (is_chain_confirmed or ord_row.get("status") != "CONFIRMING") else "CONFIRMING",
                "fill_price": round(fill_px, 4),
                "shares": round(matched_sh, 4),
                "size_usdc": real_cost,
                "fee_usdc": round(fee_usdc, 4),
                "filled_at": upd.get("filled_at") or now,
                "bal_after_entry": bal_a if bal_a is not None else upd.get("bal_after_entry"),
                "recon_status": recon_st,
                "recon_diff": recon_diff,
                "recon_note": recon_note,
                "notes": (upd.get("notes") or "") + f" | 实盘{'链上确权' if is_chain_confirmed else '成交'} {matched_sh:.2f}份@{fill_px:.2f}",
            })
            self.db.save_order(upd)
            self._set_decision(
                f"[实盘{'确权持仓' if upd['status'] == 'FILLED' else '链上确权中'}] {upd['side']} {matched_sh:.2f}份@{fill_px:.2f} (${real_cost:.2f})",
                sticky=True, now=now,
            )
            try:
                asyncio.get_running_loop().create_task(self.account.refresh_balances_and_portfolio(force=True))
            except RuntimeError:
                pass
        elif event_kind == "cancel_failed":
            err_txt = str(ev.get("error") or "撤单失败")[:80]
            self.round_live_cancel_failed = True
            upd = dict(ord_row)
            upd["notes"] = (upd.get("notes") or "") + f" | 撤单未确认({err_txt})"
            self.db.save_order(upd)
            self._set_decision(f"[实盘撤单失败·锁定本场] {err_txt}", sticky=True, now=now)
            try:
                asyncio.get_running_loop().create_task(self._send_webhook("实盘撤单失败告警", f"订单 {oid} ({upd['side']}@{upd['limit_price']}): {err_txt}"))
            except RuntimeError:
                pass
        elif event_kind in ("order_closed", "ttl_reconciled") and ord_row.get("status") == "PENDING":
            if ev.get("cancel_confirmed") is not True:
                self.round_live_cancel_failed = True
                self._set_decision(f"[实盘撤单待确认·锁定本场] {ord_row['side']}@{ord_row['limit_price']:.2f}", sticky=True, now=now)
                return
            upd = dict(ord_row)
            avail_c = self.account.get_available_cash()
            bal_b_c = upd.get("bal_before")
            rd_c = round(avail_c - float(bal_b_c), 4) if bal_b_c is not None else 0.0
            upd.update({
                "status": "CANCELLED",
                "settled_at": now,
                "bal_after_settle": avail_c,
                "recon_status": "OK" if abs(rd_c) <= 0.20 else "WARN",
                "recon_diff": rd_c,
                "recon_note": f"实盘撤单释放 (前${float(bal_b_c):.2f}→后${avail_c:.2f})" if bal_b_c is not None else "实盘撤单释放",
                "notes": (upd.get("notes") or "") + f" | 实盘确认撤单({ev.get('exchange_status') or 'TTL'})",
            })
            self.db.save_order(upd)
            if upd.get("arm_id") not in ARB_ARM_IDS:
                self.last_cancel_ts = now
                self.last_cancel_side = str(upd.get("side") or "")
                ps = self._get_round_position_state(int(upd["window_ts"]), "LIVE")
                if not ps["has_pending"] and ps["primary_filled"] is None:
                    self.last_signal_ts = 0.0
            self._set_decision(f"[实盘已撤单] {upd['side']}@{upd['limit_price']:.2f} 确认未成交", sticky=False, now=now)
            try:
                asyncio.get_running_loop().create_task(self.account.refresh_balances_and_portfolio(force=True))
            except RuntimeError:
                pass

    async def _audit_live_settlement(self, window_ts: int, live_orders: list[dict]):
        """Record post-settlement / post-exit exchange balance & audit diff after a LIVE order finishes."""
        try:
            all_closed = all(o.get("status") == "CLOSED" for o in live_orders)
            await asyncio.sleep(1.8 if all_closed else 5.0)
            pf = await self.account.refresh_balances_and_portfolio(force=True)
            avail_after = float(pf.get("available_cash") or pf.get("pusd_balance") or 0.0)
            for o in live_orders:
                row = self.db.get_order(o["order_id"])
                if not row or row.get("mode") != "LIVE":
                    continue
                upd = dict(row)
                upd["bal_after_settle"] = round(avail_after, 4)
                if upd.get("bal_before") is not None and upd.get("recon_status") != "MISMATCH":
                    expected_net = float(upd.get("pnl_usdc") or 0.0)
                    actual_net = avail_after - float(upd["bal_before"])
                    rdiff = round(actual_net - expected_net, 4)
                    # Note: winning payouts may take 10-30s for Polymarket auto-redeem
                    won = expected_net > 0
                    if abs(rdiff) <= 0.25:
                        upd["recon_status"] = "OK"
                    elif won and actual_net < 0:
                        upd["recon_status"] = "PENDING"
                        upd["recon_note"] = f"赢单待链上自动赎回 (前${float(upd['bal_before']):.2f}→现${avail_after:.2f})"
                    else:
                        upd["recon_status"] = "WARN"
                        upd["recon_note"] = f"结算核对: 前${float(upd['bal_before']):.2f}→后${avail_after:.2f} (实变{actual_net:+.2f}/理论{expected_net:+.2f})"
                    upd["recon_diff"] = rdiff
                self.db.save_order(upd)
            await self._record_live_equity_snapshot(window_ts)
        except Exception:
            pass

    async def reconcile_live_orders(self) -> dict:
        """
        v46 Authoritative LIVE Order & Position Cross-Reconciler:
        1. Reconciles PENDING + CONFIRMING + stuck SUBMITTING orders against exchange open orders & trades.
        2. Cross-checks current-round FILLED LIVE orders against Polymarket `list_positions()` to detect
           ghost positions (0 shares on exchange + un-deducted cash) and auto-flag MISMATCH / FAILED.
        """
        pending_list: list[dict] = []
        seen_ids: set[str] = set()
        now_ts = time.time()

        for w_ts in (self.current_window_ts, self.current_window_ts - 300 if self.current_window_ts else 0):
            if not w_ts:
                continue
            for o in self.db.get_orders_by_window(w_ts):
                if o.get("mode") != "LIVE":
                    continue
                st = str(o.get("status") or "")
                oid = str(o.get("order_id") or "")
                if st in ("PENDING", "CONFIRMING", "SUBMITTING") and oid and oid not in seen_ids:
                    if st == "SUBMITTING" and (now_ts - float(o.get("created_at") or now_ts)) < 15.0:
                        continue
                    seen_ids.add(oid)
                    pending_list.append({
                        "order_id": oid,
                        "side": "BUY",
                        "limit_price": float(o.get("limit_price") or 0.5),
                        "created_at": float(o.get("created_at") or now_ts),
                        "ttl_ms": 6000,
                    })
        for sell_oid, ex_meta in list(self._pending_live_exits.items()):
            if sell_oid not in seen_ids:
                seen_ids.add(sell_oid)
                pending_list.append({
                    "order_id": sell_oid,
                    "side": "SELL",
                    "limit_price": float(ex_meta.get("trigger_px") or 0.5),
                    "created_at": float(ex_meta.get("created_at") or now_ts),
                    "ttl_ms": 8000,
                })

        if pending_list:
            res = await self.account.reconcile_pending_orders(pending_list, condition_id=self.current_condition_id)
            if res.get("ok") and res.get("still_open", 0) == 0:
                self.round_live_cancel_failed = False
        else:
            if self.db.get_trading_mode() == "LIVE":
                cids = [self.current_condition_id] if self.current_condition_id else None
                await self.account.sync_account_trades(condition_ids=cids)
            res = {"ok": True, "checked": 0, "filled": 0, "cancelled": 0, "still_open": 0}

        # Cross-verify active current-round FILLED LIVE orders against actual Polymarket positions!
        if self.db.get_trading_mode() == "LIVE" and self.current_window_ts:
            cur_filled = [
                o for o in self.db.get_orders_by_window(self.current_window_ts)
                if o.get("mode") == "LIVE" and o.get("status") == "FILLED"
                and (now_ts - float(o.get("filled_at") or o.get("created_at") or now_ts)) >= 15.0
            ]
            if cur_filled:
                pf = await self.account.refresh_balances_and_portfolio(force=True, cache_key=str(self.current_condition_id or ""))
                pos_by_token: dict[str, float] = {}
                for p in (pf.get("open_positions") or []):
                    aid = str(p.get("asset_id") or "")
                    if aid:
                        pos_by_token[aid] = pos_by_token.get(aid, 0.0) + float(p.get("current_size") or p.get("size") or 0.0)
                avail_now = float(pf.get("available_cash") or 0.0)
                for o in cur_filled:
                    tid = self.current_up_token if o["side"] == "UP" else self.current_down_token
                    ex_sh = pos_by_token.get(str(tid or ""), 0.0)
                    upd = dict(o)
                    if ex_sh >= 0.5:
                        if upd.get("recon_status") != "OK" and upd.get("bal_before") is not None:
                            bal_a = upd.get("bal_after_entry") if upd.get("bal_after_entry") is not None else avail_now
                            exp_sp = float(upd.get("size_usdc") or 0.0) + float(upd.get("fee_usdc") or 0.0)
                            act_sp = float(upd["bal_before"]) - float(bal_a)
                            rdiff = round(act_sp - exp_sp, 4)
                            upd["bal_after_entry"] = round(float(bal_a), 4)
                            upd["recon_diff"] = rdiff
                            upd["recon_status"] = "OK" if abs(rdiff) <= 0.20 else "WARN"
                            upd["recon_note"] = f"交易所持仓核验通过({ex_sh:.2f}份) | 实扣${act_sp:.2f}/理论${exp_sp:.2f}"
                            self.db.save_order(upd)
                    else:
                        # Exchange reports 0 shares for a local FILLED order > 15s old!
                        bal_b = upd.get("bal_before")
                        no_spend = (bal_b is not None and abs(avail_now - float(bal_b)) <= 0.25)
                        if no_spend and not upd.get("exit_price"):
                            upd["status"] = "FAILED"
                            upd["shares"] = 0.0
                            upd["pnl_usdc"] = 0.0
                            upd["settled_at"] = now_ts
                            upd["bal_after_entry"] = round(avail_now, 4)
                            upd["bal_after_settle"] = round(avail_now, 4)
                            upd["recon_status"] = "MISMATCH"
                            upd["recon_diff"] = round(avail_now - float(bal_b), 4)
                            upd["recon_note"] = f"核验异常: 交易所0持仓且余额未扣(${float(bal_b):.2f}→${avail_now:.2f})，已自动撤销假持仓"
                            upd["notes"] = (upd.get("notes") or "") + " | 交易所0持仓且余额未扣，自动修正为FAILED"
                            self.db.save_order(upd)
                            self._set_decision(f"[实盘核验修正] {upd['side']} 交易所0持仓且余额未扣，已清除假持仓", sticky=True, now=now_ts)
                        elif upd.get("recon_status") != "MISMATCH":
                            upd["recon_status"] = "MISMATCH"
                            upd["recon_note"] = f"警告: 本地显示持仓但交易所当前0份(请检查是否外部平仓或链上延迟)"
                            self.db.save_order(upd)
        return res

    async def _check_live_round_safety(
        self,
        size_usdc: float,
        risk_cfg: dict,
        is_arb: bool = False,
        opp_token_id: Optional[str] = None,
    ) -> tuple[bool, str, dict]:
        """v42 Per-Round Hard Capital & Exchange Exposure Circuit Breaker."""
        if self.round_live_cancel_failed:
            return False, "本场存在未确认撤单，硬熔断锁定不再发单", {}

        orders = [o for o in self.db.get_orders_by_window(self.current_window_ts) if o.get("mode") == "LIVE"]
        active_spent = sum(
            float(o.get("size_usdc") or 0.0)
            for o in orders
            if o.get("status") in ("PENDING", "FILLED", "CLOSED", "SETTLED")
        )
        unit = max(HARD_LIMITS["min_unit_usdc"], float(risk_cfg.get("unit_size_usdc", 10.0)))
        round_budget_cap = round(unit * (2.2 if is_arb else 1.85), 2)
        if active_spent + float(size_usdc) > round_budget_cap + 0.25:
            return False, f"触及单场资金硬上限 (${active_spent:.2f}+${size_usdc:.2f} > ${round_budget_cap:.2f})", {}

        exp = await self.account.check_round_exchange_exposure(
            self.current_condition_id,
            [t for t in (self.current_up_token, self.current_down_token) if t],
        )
        if exp.get("open_orders_count", 0) > 0:
            return False, f"交易所本场已有 {exp['open_orders_count']} 笔活跃挂单，硬熔断拦截", exp
        if not is_arb and exp.get("position_shares", 0.0) >= 0.5:
            return False, f"交易所本场已有 {exp['position_shares']:.2f} 份真实持仓，拦截重复开仓", exp
        if is_arb and opp_token_id:
            opp_sh = float((exp.get("position_shares_by_token") or {}).get(str(opp_token_id), 0.0))
            if opp_sh >= 0.5:
                return False, f"交易所反手方向已有 {opp_sh:.2f} 份持仓，禁止重复锁利", exp
        return True, "", exp

    async def _record_live_equity_snapshot(self, window_ts: int):
        try:
            pf = await self.account.refresh_balances_and_portfolio(force=True)
            eq = float(pf.get("total_equity") or pf.get("pusd_balance") or 0.0)
            stats = self.db.get_performance_stats("LIVE")
            self.db.record_equity("LIVE", window_ts, eq, stats["total_pnl"], stats["total_trades"])
        except Exception:
            pass

    def _on_paper_order_cancelled(self, order: dict):
        """Cancelled orders release cooldown ONLY when no other active primary order exists in this round."""
        if order.get("arm_id") in ARB_ARM_IDS:
            return
        m = str(order.get("mode") or "PAPER")
        ps = self._get_round_position_state(int(order.get("window_ts") or self.current_window_ts), m)
        if not ps["has_pending"] and ps["primary_filled"] is None:
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
        prev_twap = self.oracle.get_effective_twap60()
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
        self.round_arb_attempts = 0
        self.last_exit_attempt_ts = 0.0
        self.round_live_cancel_failed = False
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
        self.current_tick_size = 0.01
        self.current_min_size = 5.0
        self.current_taker_fee_rate = float(HARD_LIMITS["taker_fee_rate"])
        self.current_accepting_orders = True
        self.current_neg_risk = False
        self.current_market_version = 1
        self._pending_live_exits.clear()
        # CRITICAL v37: Promote pre-subscribed next-round tokens & books with 0ms gap!
        prefetched = self.prefetched_rounds.pop(new_window_ts, None)
        if prefetched and prefetched.get("up_token_id") and prefetched.get("down_token_id"):
            self.current_condition_id = prefetched.get("condition_id")
            self.current_up_token = prefetched["up_token_id"]
            self.current_down_token = prefetched["down_token_id"]
            self.current_tick_size = float(prefetched.get("tick_size") or 0.01)
            self.current_min_size = float(prefetched.get("min_size") or 5.0)
            self.current_taker_fee_rate = float(prefetched.get("taker_fee_rate", HARD_LIMITS["taker_fee_rate"]))
            self.current_accepting_orders = bool(prefetched.get("accepting_orders", True))
            self.current_neg_risk = bool(prefetched.get("neg_risk", False))
            self.current_market_version = int(prefetched.get("market_version", 1))
            self.clob.set_tokens(self.current_up_token, self.current_down_token, tick_size=self.current_tick_size)
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
        self.fv_strategy.on_new_round(new_window_ts)

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
                self.clob.set_presubscribe_tokens(info["up_token_id"], info["down_token_id"])
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

            cur_info, prev_info, cp_info = await asyncio.gather(
                self.oracle.fetch_gamma_round(window_ts),
                self.oracle.fetch_gamma_round(window_ts - 300),
                self.oracle.fetch_crypto_price(window_ts) if not self.current_ptb_official else asyncio.sleep(0, result=None),
            )
            if cur_info:
                if cur_info.get("condition_id"):
                    self.current_condition_id = cur_info["condition_id"]
                self.current_tick_size = float(cur_info.get("tick_size") or self.current_tick_size)
                self.current_min_size = float(cur_info.get("min_size") or self.current_min_size)
                self.current_taker_fee_rate = float(cur_info.get("taker_fee_rate", self.current_taker_fee_rate))
                self.current_accepting_orders = bool(cur_info.get("accepting_orders", True))
                self.current_neg_risk = bool(cur_info.get("neg_risk", self.current_neg_risk))
                self.current_market_version = int(cur_info.get("market_version", self.current_market_version))
                if cur_info["up_token_id"] and cur_info["down_token_id"]:
                    self.current_up_token = cur_info["up_token_id"]
                    self.current_down_token = cur_info["down_token_id"]
                    self.clob.set_tokens(cur_info["up_token_id"], cur_info["down_token_id"], tick_size=self.current_tick_size)

                if cur_info["price_to_beat"] is not None:
                    self.current_ptb = cur_info["price_to_beat"]
                    self.current_ptb_source = "Gamma 官方已锁定"
                    self.current_ptb_official = True
                    self.current_ptb_boundary_dt = 0.0
                elif isinstance(cp_info, dict) and cp_info.get("open_price") is not None:
                    self.current_ptb = cp_info["open_price"]
                    self.current_ptb_source = "官方 crypto-price 已锁定"
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
        """Attach awaiting_official, provisional_outcome & unified `settled_balance` for ledger display."""
        rows = self.db.get_orders(mode=mode, limit=limit)
        now = time.time()

        # Lazy lookup map from equity_curve for legacy SETTLED/CLOSED rows missing bal_after_settle
        eq_by_key: dict[tuple[str, int], float] = {}
        need_eq = any(
            o.get("status") in ("SETTLED", "CLOSED")
            and o.get("bal_after_settle") is None
            and o.get("balance_after") is None
            and o.get("mode") in ("PAPER", "LIVE")
            for o in rows
        )
        if need_eq:
            for m_key in ("PAPER", "LIVE"):
                for pt in self.db.get_equity_curve(mode=m_key, limit=1000):
                    wts = int(pt.get("window_ts") or 0)
                    if wts > 0:
                        eq_by_key[(m_key, wts)] = round(float(pt.get("balance_usdc") or 0.0), 4)

        for o in rows:
            ended = (int(o["window_ts"]) + 300) <= now
            awaiting = bool(ended and o["status"] == "FILLED" and not o.get("official_outcome"))
            o["awaiting_official"] = awaiting
            if awaiting:
                ps = self.pending_settlements.get(int(o["window_ts"]))
                o["provisional_outcome"] = ps.get("provisional_outcome") if ps else None
            else:
                o["provisional_outcome"] = None

            # Unified post-settlement / post-close balance (strictly for SETTLED/CLOSED orders)
            st = str(o.get("status") or "")
            if st in ("SETTLED", "CLOSED"):
                sb = o.get("bal_after_settle")
                if sb is None:
                    sb = o.get("balance_after")
                if sb is None and o.get("mode") in ("PAPER", "LIVE"):
                    sb = eq_by_key.get((str(o["mode"]), int(o["window_ts"])))
                o["settled_balance"] = round(float(sb), 4) if sb is not None else None
            else:
                o["settled_balance"] = None
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
            live_settled = [o for o in settled_orders if o.get("mode") == "LIVE" and o.get("status") in ("SETTLED", "CLOSED")]
            if live_settled:
                asyncio.create_task(self._audit_live_settlement(window_ts, live_settled))

            # Update consecutive loss circuit breaker using NET round PnL
            if not is_recovery:
                risk_cfg = self.db.get_json("risk_config", {})
                mode = self.db.get_trading_mode()
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
                now_srv = self.clock.server_now()
                win_ts = self.oracle.get_window_ts(now_srv)
                if win_ts != self.current_window_ts:
                    await self._switch_to_round(win_ts)
                    last_slice_sec = -1

                elapsed_sec = max(0, min(300, int(now_srv - self.current_window_ts)))
                spot = self.cex.get_best_spot()
                strike = float(self.current_ptb) if self.current_ptb else 0.0
                eff_twap_dec = self.oracle.get_effective_twap60(now_srv)
                twap60 = float(eff_twap_dec) if eff_twap_dec > 0 else (spot - self.basis_ema)

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
                rc_loop = self.db.get_json("risk_config", {})
                use_fv_engine = str(rc_loop.get("strategy_engine") or "FV_DISCOUNT_LEADER").upper() == "FV_DISCOUNT_LEADER"
                if use_fv_engine:
                    feats = self.learner.build_feature_vector(snap, up_book["imbalance"])
                    model_up_p, model_dn_p, _ = self.fv_strategy.compute_fair_probs(snap, elapsed_sec)
                    up_edge, dn_edge = self.fv_strategy.compute_fee_net_edges(
                        model_up_p, model_dn_p, up_book, dn_book, self.current_taker_fee_rate
                    )
                else:
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

                # Pre-fetch & pre-subscribe next round's Token IDs 90s before rollover (0ms gap!)
                if elapsed_sec >= 210 and (self.current_window_ts + 300) not in self.prefetched_rounds:
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
                async with self._trade_lock:
                    await self._evaluate_and_trade(now, elapsed_sec, snap, model_up_p, model_dn_p, up_book, dn_book, up_edge, dn_edge)

            except Exception as e:
                self._set_decision(f"引擎异常: {str(e)[:60]}", sticky=False)

            elapsed_loop = time.time() - loop_start
            await asyncio.sleep(max(0.1, 1.0 - elapsed_loop))

    def _get_round_position_state(self, window_ts: int, mode: str) -> dict:
        """Inspect current round orders for the given mode to enforce strict 1-primary-fill + optional equal-share arb lock."""
        orders = [o for o in self.db.get_orders_by_window(window_ts) if o["mode"] == mode]
        pending_orders = [o for o in orders if o["status"] in ("SUBMITTING", "CONFIRMING", "PENDING")]
        confirming_orders = [o for o in orders if o["status"] in ("SUBMITTING", "CONFIRMING")]
        primary_filled = [
            o for o in orders
            if o["status"] in ("FILLED", "SETTLED", "CLOSED") and o["arm_id"] not in ARB_ARM_IDS
        ]
        arb_filled = [
            o for o in orders
            if o["status"] in ("FILLED", "SETTLED", "CLOSED") and o["arm_id"] in ARB_ARM_IDS
        ]
        cancelled_primary = [
            o for o in orders
            if o["status"] == "CANCELLED" and o["arm_id"] not in ARB_ARM_IDS
        ]
        return {
            "has_pending": len(pending_orders) > 0,
            "confirming_order": confirming_orders[0] if confirming_orders else None,
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
        Autonomous ARB_LOCK_AUTO Exit Arm (v44 — supports FAK Instant Taker Lock + Post-Only Maker Lock):
        - Priority 1 (FAK Instant Lock `ARB_LOCK_TAKER`): whenever opposite best_ask satisfies
          `primary_px + primary_fee_ps + opp_ask + taker_fee_ps <= max_combined`, immediately locks
          risk-free profit via FAK with a profit-preserving max_price ceiling (zero resting orders!).
        - Priority 2 (Maker Lock `ARB_LOCK_MAKER`): 0%-fee Post-Only equal-share opposite hedge.
          In LIVE mode, Maker Lock is STRICTLY disabled when `live_taker_only=True` and hard-capped
          at 1 attempt/round when enabled.
        """
        if str(primary_ord.get("exit_arm") or "") != "ARB_LOCK_AUTO":
            return False
        if elapsed_sec >= 285 or mode not in ("PAPER", "LIVE"):
            return False

        risk_cfg_arb = self.db.get_json("risk_config", {})
        live_taker_only = bool(risk_cfg_arb.get("live_taker_only", True))

        prior_live_arbs: list[dict] = []
        if mode == "LIVE":
            if not bool(risk_cfg_arb.get("live_enable_arb_lock", False)):
                return False
            if self.round_live_cancel_failed:
                return False
            prior_live_arbs = [
                o for o in self.db.get_orders_by_window(self.current_window_ts)
                if o.get("mode") == "LIVE" and o.get("arm_id") in ARB_ARM_IDS
            ]
            # Never fire again if any arb order is already PENDING or FILLED/SETTLED
            if any(o.get("status") in ("PENDING", "FILLED", "SETTLED", "CLOSED") for o in prior_live_arbs):
                return False
            min_gap = 15.0 if live_taker_only else 25.0
            if now - self.last_arb_attempt_ts < min_gap:
                return False
        elif now - self.last_arb_attempt_ts < 6.0:
            return False

        filled_at = float(primary_ord.get("filled_at") or primary_ord.get("created_at") or 0.0)
        if now - filled_at < 3.0:
            return False

        primary_side = primary_ord["side"]
        primary_px = float(primary_ord.get("fill_price") or primary_ord["limit_price"])
        primary_shares = float(primary_ord.get("shares") or (float(primary_ord["size_usdc"]) / max(0.01, primary_px)))
        primary_fee_ps = float(primary_ord.get("fee_usdc") or 0.0) / max(0.01, primary_shares)

        opp_side = "DOWN" if primary_side == "UP" else "UP"
        opp_book = dn_book if opp_side == "DOWN" else up_book
        if not self.clob.is_book_fresh(opp_side, max_age_sec=8.0) or not self.clob.is_parity_ok():
            return False
        opp_bid = float(opp_book.get("best_bid") or 0.0)
        opp_ask = float(opp_book.get("best_ask") or 0.0)
        if opp_bid <= 0.02 or opp_ask <= 0.02:
            return False

        auto_params = self.learner.get_auto_params()
        max_combined = max(0.92, min(0.96, float(auto_params.get("arb_max_combined_price", 0.95))))
        tick = max(0.001, self.clob.get_tick_size(opp_side))

        # Check Priority 1: Instant FAK Taker Arb Lock (zero resting order, accounts for BOTH legs' fees!)
        taker_fee_ps = self.current_taker_fee_rate * opp_ask * (1.0 - opp_ask)
        all_in_taker = round(primary_px + primary_fee_ps + opp_ask + taker_fee_ps, 4)
        can_instant_fak = (0.02 <= opp_ask <= 0.90) and (all_in_taker <= max_combined)

        use_taker_arb = False
        if can_instant_fak:
            use_taker_arb = True
            opp_limit = self.clob.round_to_tick(opp_ask, side=opp_side, mode="nearest")
            combined_px = all_in_taker
            # Profit-preserving FAK max_price ceiling (at most +1 tick AND never breaches max_combined)
            max_profit_px = max_combined - primary_px - primary_fee_ps - taker_fee_ps
            fak_max_px = self.clob.round_to_tick(min(opp_limit + tick, max_profit_px), side=opp_side, mode="floor")
            fak_max_px = max(opp_limit, fak_max_px)
        else:
            # In LIVE with `live_taker_only=True`: strictly NEVER place resting Maker arb-lock orders!
            if mode == "LIVE" and live_taker_only:
                return False
            opp_limit = round(opp_bid + 0.01, 2) if (opp_bid + 0.01 < opp_ask) else round(opp_bid, 2)
            opp_limit = max(0.02, min(0.90, opp_limit))
            combined_px = round(primary_px + primary_fee_ps + opp_limit, 4)
            if combined_px > max_combined:
                return False
            fak_max_px = None

        arb_size_usdc = round(primary_shares * opp_limit, 2)
        if arb_size_usdc < 1.0:
            return False

        guaranteed_margin = round(1.0 - combined_px, 4)
        arb_arm = (
            {"id": "ARB_LOCK_TAKER", "offset_cents": 0.00, "ttl_ms": 1500, "is_taker": True}
            if use_taker_arb
            else {"id": "ARB_LOCK_MAKER", "offset_cents": 0.00, "ttl_ms": 5000, "is_taker": False}
        )

        if mode == "PAPER":
            self.last_arb_attempt_ts = now
            arb_ord = self.paper.submit_order(
                self.current_window_ts, opp_side, arb_arm, opp_limit, arb_size_usdc,
                round(1.0 - primary_px, 4), opp_book["mid"], guaranteed_margin,
                snap["net_power_bps"], elapsed_sec, snap["regime"], mode="PAPER",
                book_bid=opp_bid, book_ask=opp_ask, exit_arm="ARB_LOCK_AUTO", entry_strategy="arb_lock",
            )
            if arb_ord is None:
                return False
            tag_lbl = "FAK即时锁利" if use_taker_arb else "挂反手锁利"
            self._set_decision(
                f"[AI双边锁利] {tag_lbl} {opp_side}@{opp_limit:.2f} (含费合计{combined_px:.2f}, +{guaranteed_margin*100:.1f}%)",
                sticky=True, now=now,
            )
            return True

        elif mode == "LIVE":
            if self.oracle.status["geo_check"].get("blocked") or not self.current_accepting_orders:
                return False
            # Hard attempt limits: FAK allows up to 2 attempts/round (never leaves resting order); Maker strictly 1!
            prior_maker = [o for o in prior_live_arbs if o.get("arm_id") == "ARB_LOCK_MAKER"]
            prior_taker = [o for o in prior_live_arbs if o.get("arm_id") == "ARB_LOCK_TAKER"]
            if not use_taker_arb and (len(prior_maker) >= 1 or self.round_arb_attempts >= 1):
                return False
            if use_taker_arb and (len(prior_taker) >= 2 or self.round_arb_attempts >= 2):
                return False

            if primary_shares < float(self.current_min_size or 1.0):
                return False
            cash_info = await self.account.ensure_fresh_available_cash(max_age_sec=3.0, cache_key=str(self.current_condition_id or ""))
            if not cash_info["cash_verified"]:
                self._set_decision(f"[实盘锁利跳过] 可用余额核验失败({cash_info.get('error') or 'SDK未就绪'})", sticky=False, now=now)
                return False
            avail_pusd = float(cash_info["available_cash"])
            req_arb_cash = self.learner.required_buy_cash(arb_size_usdc, opp_limit, use_taker_arb, self.current_taker_fee_rate)
            if avail_pusd < req_arb_cash:
                self._set_decision(f"[实盘锁利跳过] 可用pUSD不足 (可用${avail_pusd:.2f} < 需${req_arb_cash:.2f}含费)", sticky=False, now=now)
                return False

            token_id = self.current_up_token if opp_side == "UP" else self.current_down_token
            ok_safe, safe_msg, _ = await self._check_live_round_safety(
                arb_size_usdc, risk_cfg_arb, is_arb=True, opp_token_id=token_id
            )
            if not ok_safe:
                self._set_decision(f"[实盘锁利拦截] {safe_msg}", sticky=False, now=now)
                return False

            check_px = fak_max_px if use_taker_arb and fak_max_px is not None else opp_limit
            ok_bk, bk_msg, fresh_bk = await self.clob.verify_fresh_book_rest(
                opp_side, check_px, is_buy=True, is_taker=use_taker_arb
            )
            if not ok_bk:
                self._set_decision(f"[实盘锁利跳过] {bk_msg}", sticky=False, now=now)
                return False

            # Re-verify profit math against fresh REST book
            if fresh_bk and float(fresh_bk.get("best_bid") or 0) > 0.02 and float(fresh_bk.get("best_ask") or 0) > 0.02:
                fb, fa = float(fresh_bk["best_bid"]), float(fresh_bk["best_ask"])
                opp_bid, opp_ask = fb, fa
                if use_taker_arb:
                    opp_limit = self.clob.round_to_tick(fa, side=opp_side, mode="nearest")
                    taker_fee_ps = self.current_taker_fee_rate * opp_limit * (1.0 - opp_limit)
                    combined_px = round(primary_px + primary_fee_ps + opp_limit + taker_fee_ps, 4)
                    if combined_px > max_combined:
                        return False
                    max_profit_px = max_combined - primary_px - primary_fee_ps - taker_fee_ps
                    fak_max_px = max(opp_limit, self.clob.round_to_tick(min(opp_limit + tick, max_profit_px), side=opp_side, mode="floor"))
                else:
                    opp_limit = self.clob.round_to_tick(fb + 0.01 if (fb + 0.01 < fa) else fb, side=opp_side, mode="floor")
                    combined_px = round(primary_px + primary_fee_ps + opp_limit, 4)
                    if combined_px > max_combined:
                        return False
                arb_size_usdc = round(primary_shares * opp_limit, 2)
                req_arb_cash = self.learner.required_buy_cash(arb_size_usdc, opp_limit, use_taker_arb, self.current_taker_fee_rate)
                if arb_size_usdc < 1.0 or avail_pusd < req_arb_cash:
                    return False
                guaranteed_margin = round(1.0 - combined_px, 4)

            self.last_arb_attempt_ts = now
            self.round_arb_attempts += 1
            try:
                bal_before_arb = round(avail_pusd, 4)
                res = await self.account.place_live_order(
                    token_id, opp_limit, arb_size_usdc, use_taker_arb, int(arb_arm["ttl_ms"]),
                    tick_size=self.clob.get_tick_size(opp_side),
                    min_size=self.current_min_size,
                    geoblocked=bool(self.oracle.status["geo_check"].get("blocked")),
                    neg_risk=self.current_neg_risk,
                    market_version=self.current_market_version,
                    condition_id=self.current_condition_id,
                    max_price_override=fak_max_px if use_taker_arb else None,
                    bal_before=bal_before_arb,
                )
                matched = float(res.get("matched_shares") or 0.0)
                is_delayed_arb = bool(res.get("is_delayed", False))
                filled_now = matched > 0.0
                m_px = float(res.get("matched_price") or opp_limit)
                real_cost = round(matched * m_px, 4) if filled_now else arb_size_usdc
                arb_fee = round(matched * self.current_taker_fee_rate * m_px * (1.0 - m_px), 4) if (filled_now and use_taker_arb) else 0.0
                init_status = "CONFIRMING" if (filled_now or is_delayed_arb) else ("CANCELLED" if use_taker_arb else "PENDING")
                if init_status in ("CONFIRMING", "PENDING"):
                    self.account.reserve_local_cash(real_cost + arb_fee if filled_now else req_arb_cash)

                self.db.save_order({
                    "order_id": res["order_id"] or f"live_arb_{int(now*1000)}",
                    "window_ts": self.current_window_ts,
                    "mode": "LIVE",
                    "side": opp_side,
                    "arm_id": arb_arm["id"],
                    "limit_price": opp_limit,
                    "fill_price": round(m_px, 4) if filled_now else None,
                    "size_usdc": real_cost,
                    "shares": matched if filled_now else res["shares"],
                    "fee_usdc": arb_fee,
                    "status": init_status,
                    "model_win_prob": round(1.0 - primary_px, 4),
                    "market_implied_prob": opp_book["mid"],
                    "edge": guaranteed_margin,
                    "net_power_bps": snap["net_power_bps"],
                    "elapsed_sec": elapsed_sec,
                    "regime": snap["regime"],
                    "official_outcome": None,
                    "pnl_usdc": 0.0,
                    "created_at": now,
                    "filled_at": now if filled_now else None,
                    "settled_at": now if init_status == "CANCELLED" else None,
                    "book_bid": opp_bid,
                    "book_ask": opp_ask,
                    "exit_arm": "ARB_LOCK_AUTO",
                    "entry_strategy": "arb_lock",
                    "bal_before": bal_before_arb,
                    "bal_after_entry": None,
                    "bal_after_settle": bal_before_arb if init_status == "CANCELLED" else None,
                    "recon_status": "OK" if init_status == "CANCELLED" else "PENDING",
                    "recon_diff": 0.0 if init_status == "CANCELLED" else None,
                    "recon_note": f"等待锁利确权 (前${bal_before_arb:.2f})" if init_status != "CANCELLED" else f"锁利未成无扣款(${bal_before_arb:.2f})",
                    "notes": f"实盘{'FAK吃单' if use_taker_arb else 'Maker挂单'}锁利({res.get('exchange_status')}) 合计{combined_px:.2f} | 盘口 {opp_bid:.2f}/{opp_ask:.2f}",
                })
                if init_status == "CANCELLED":
                    self._set_decision(f"[实盘FAK锁利未成] {opp_side}@{opp_limit:.2f} 保护价内无卖盘", sticky=False, now=now)
                else:
                    st_lbl = "已成" if filled_now else "挂单中"
                    mode_lbl = "FAK锁利" if use_taker_arb else "Maker锁利"
                    self._set_decision(
                        f"[实盘{mode_lbl}{st_lbl}] {opp_side}@{m_px if filled_now else opp_limit:.2f} (合计{combined_px:.2f}, +{guaranteed_margin*100:.1f}%)",
                        sticky=True, now=now,
                    )
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
        snap: Optional[dict] = None,
    ) -> bool:
        """Execute FV Dynamic FAK Exit OR legacy Maker TP / emergency SL."""
        if primary_ord.get("status") != "FILLED" or mode not in ("PAPER", "LIVE"):
            return False
        side = primary_ord["side"]
        if not self.clob.is_book_fresh(side, max_age_sec=8.0) or not self.clob.is_parity_ok():
            return False

        book = up_book if side == "UP" else dn_book
        model_p = model_up_p if side == "UP" else model_dn_p
        if str(primary_ord.get("entry_strategy") or "") == "fv_discount_leader" or str(primary_ord.get("arm_id") or "") == "FV_LEADER_FAK":
            exit_sig = self.fv_strategy.evaluate_dynamic_exit(
                primary_ord, book, snap or self.features.latest_snapshot, elapsed_sec, risk_cfg, now,
                taker_fee_rate=self.current_taker_fee_rate,
            )
        else:
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
            if any(m["primary_order_id"] == primary_ord["order_id"] for m in self._pending_live_exits.values()):
                return True
            if self.oracle.status["geo_check"].get("blocked") or self.round_live_cancel_failed:
                return False
            if now - self.last_exit_attempt_ts < 15.0:
                return False
            live_taker_only = bool(risk_cfg.get("live_taker_only", True))
            if live_taker_only and is_maker:
                cur_bid = float(book.get("best_bid") or 0.0)
                if cur_bid < trigger_px:
                    return False
                is_maker = False
                trigger_px = cur_bid

            token_id = self.current_up_token if side == "UP" else self.current_down_token
            shares = float(primary_ord.get("shares") or 0.0)
            if token_id and shares > 0:
                exp = await self.account.check_round_exchange_exposure(self.current_condition_id, [token_id])
                if exp.get("open_orders_count", 0) > 0:
                    return True
                ex_sh = float((exp.get("position_shares_by_token") or {}).get(str(token_id), 0.0))
                if ex_sh > 0.01:
                    shares = min(shares, ex_sh)
                ok_bk, bk_msg, _ = await self.clob.verify_fresh_book_rest(side, trigger_px, is_buy=False, is_taker=(not is_maker))
                if not ok_bk:
                    self._set_decision(f"[实盘平仓等待] {bk_msg}", sticky=False, now=now)
                    return False
                self.last_exit_attempt_ts = now
                try:
                    res = await self.account.place_live_sell_order(
                        token_id, trigger_px, shares,
                        is_taker=(not is_maker), ttl_ms=ttl_ms,
                        tick_size=self.clob.get_tick_size(side),
                        geoblocked=bool(self.oracle.status["geo_check"].get("blocked")),
                        neg_risk=self.current_neg_risk,
                        market_version=self.current_market_version,
                        condition_id=self.current_condition_id,
                    )
                    sell_oid = str(res.get("order_id") or "")
                    matched_sh = float(res.get("matched_shares") or 0.0)
                    if matched_sh > 0.0:
                        exec_px = float(res.get("exec_price") or trigger_px)
                        entry_fee = float(primary_ord.get("fee_usdc") or 0.0)
                        exit_fee = (matched_sh * self.current_taker_fee_rate * exec_px * (1.0 - exec_px)) if (not is_maker) else 0.0
                        pnl = round(matched_sh * exec_px - float(primary_ord["size_usdc"]) - entry_fee - exit_fee, 4)
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
                        self._set_decision(f"[实盘{exit_sig['exit_reason']}成交] @{exec_px:.2f} (净{pnl:+.2f})", sticky=True, now=now)
                        asyncio.create_task(self._audit_live_settlement(int(ord_upd["window_ts"]), [ord_upd]))
                    elif sell_oid and not is_maker:
                        self._pending_live_exits[sell_oid] = {
                            "primary_order_id": primary_ord["order_id"],
                            "exit_reason": exit_sig["exit_reason"],
                            "trigger_px": trigger_px,
                            "is_maker": True,
                            "created_at": now,
                        }
                        self._set_decision(f"[实盘{exit_sig['exit_reason']}挂卖中] @{trigger_px:.2f} ({shares:.2f}份)", sticky=True, now=now)
                    else:
                        self._set_decision(f"[实盘{exit_sig['exit_reason']}未成] FAK无对手盘，继续监控", sticky=False, now=now)
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
        max_price_override: Optional[float] = None,
    ):
        if effective_mode == "SHADOW":
            submitted = self.paper.submit_order(
                self.current_window_ts, sig, arm, limit_px, size_usdc,
                model_prob, book["mid"], model_prob - limit_px, snap["net_power_bps"], elapsed_sec, snap["regime"],
                mode="SHADOW", book_bid=book["best_bid"], book_ask=book["best_ask"],
                extra_note=extra_note, exit_arm=exit_arm, entry_strategy=entry_strategy,
                max_price_override=max_price_override,
            )
            if submitted is None:
                return
            if not shadow_reason:
                self.last_signal_ts = now
            reason = shadow_reason or ("暂停" if raw_mode == "PAUSED" else ("熔断" if in_breaker else "日亏上限"))
            self._set_decision(f"[{reason}] 影子单({extra_note}) {sig}@{limit_px:.2f}", sticky=False, now=now)
            return

        if effective_mode == "PAPER":
            submitted = self.paper.submit_order(
                self.current_window_ts, sig, arm, limit_px, size_usdc,
                model_prob, book["mid"], model_prob - limit_px, snap["net_power_bps"], elapsed_sec, snap["regime"],
                mode="PAPER", book_bid=book["best_bid"], book_ask=book["best_ask"],
                extra_note=extra_note, exit_arm=exit_arm, entry_strategy=entry_strategy,
                max_price_override=max_price_override,
            )
            if submitted is None:
                self._set_decision("本场已有活跃主仓/挂单，原子锁已拦截重复下单", sticky=False, now=now)
                return
            self.last_signal_ts = now
            self.round_orders_count += 1
            self._set_decision(f"[{extra_note}] {sig} @{limit_px:.2f} (${size_usdc})", sticky=True, now=now)
        elif effective_mode == "LIVE":
            if self.oracle.status["geo_check"].get("blocked"):
                self._record_block("当前 IP 处于 Polymarket 地区限制，禁止实盘下单", "地区限制", now=now)
                return
            if not self.current_accepting_orders:
                self._record_block("本场盘口已停止接单(acceptingOrders=false)", "盘口关闭", now=now)
                return
            if not self.current_ptb_official:
                self._record_block("实盘等待官方目标价确认后开单", "目标未确权", now=now)
                return
            is_tk = bool(arm.get("is_taker"))
            px_tick = self.clob.round_to_tick(limit_px, side=sig, mode="floor" if not is_tk else "nearest")
            risk_cfg_live = self.db.get_json("risk_config", {})
            ok_safe, safe_msg, _ = await self._check_live_round_safety(size_usdc, risk_cfg_live, is_arb=False)
            if not ok_safe:
                self._record_block(f"实盘硬熔断拦截: {safe_msg}", "单场熔断", now=now)
                return

            rest_check_px = max_price_override if (is_tk and max_price_override is not None) else px_tick
            ok_bk, bk_msg, fresh_bk = await self.clob.verify_fresh_book_rest(sig, rest_check_px, is_buy=True, is_taker=is_tk)
            if not ok_bk:
                self._record_block(f"实盘盘口复核拦截: {bk_msg}", "盘口复核", now=now)
                return
            if fresh_bk and float(fresh_bk.get("best_bid") or 0) > 0 and float(fresh_bk.get("best_ask") or 0) > 0:
                if entry_strategy == "fv_discount_leader":
                    _, _, z_c = self.fv_strategy.compute_fair_probs(snap, elapsed_sec)
                    sz_c = z_c if sig == "UP" else -z_c
                    ok_fv, fv_msg, fv_ask, fv_cap = self.fv_strategy.revalidate_on_fresh_book(
                        sig, model_prob, sz_c, fresh_bk, self.current_taker_fee_rate,
                        initial_max_cap=max_price_override,
                    )
                    if not ok_fv:
                        self._record_block(f"实盘FV复核拦截: {fv_msg}", "盘口复核", now=now)
                        return
                    px_tick = self.clob.round_to_tick(fv_ask, side=sig, mode="nearest")
                    max_price_override = fv_cap
                    book = fresh_bk
                else:
                    max_px_live = float(self.learner.get_auto_params().get("max_entry_price", 0.75))
                    recomputed = self.learner.compute_order_price(arm, float(fresh_bk["best_bid"]), float(fresh_bk["best_ask"]), max_px_live)
                    if recomputed is None:
                        self._record_block(f"最新盘口 {fresh_bk['best_bid']:.2f}/{fresh_bk['best_ask']:.2f} 超出赔率边界", "盘口复核", now=now)
                        return
                    px_tick = self.clob.round_to_tick(recomputed, side=sig, mode="floor" if not is_tk else "nearest")
                    book = fresh_bk

            # Final fee-aware available_cash verification right before firing real money order
            avail_now = self.account.get_available_cash()
            min_req_notional = max(HARD_LIMITS["min_unit_usdc"], round(float(self.current_min_size or 5.0) * px_tick + 0.01, 2))
            max_aff = self.learner.max_affordable_notional(avail_now, px_tick, is_tk, self.current_taker_fee_rate)
            if max_aff < min_req_notional:
                min_cash_need = self.learner.required_buy_cash(min_req_notional, px_tick, is_tk, self.current_taker_fee_rate)
                self._record_block(f"实盘可用 pUSD 不足 (可用${avail_now:.2f} < 最低需${min_cash_need:.2f}含费)", "余额不足", now=now)
                return
            if size_usdc > max_aff:
                size_usdc = max_aff
            req_entry_cash = self.learner.required_buy_cash(size_usdc, px_tick, is_tk, self.current_taker_fee_rate)

            ps_chk = self._get_round_position_state(self.current_window_ts, "LIVE")
            if ps_chk["has_pending"] or ps_chk["primary_filled"] is not None:
                return

            # Stage 3 (v46): Atomic pre-flight SUBMITTING slot reservation with exact pre-order balance snapshot!
            bal_before_snap = round(avail_now, 4)
            tmp_oid = f"sub_{int(now*1000)}_{sig}"
            placeholder = {
                "order_id": tmp_oid,
                "window_ts": self.current_window_ts,
                "mode": "LIVE",
                "side": sig,
                "arm_id": arm["id"],
                "limit_price": px_tick,
                "fill_price": None,
                "size_usdc": size_usdc,
                "shares": round(size_usdc / max(0.01, px_tick), 2),
                "fee_usdc": 0.0,
                "status": "SUBMITTING",
                "model_win_prob": model_prob,
                "market_implied_prob": book["mid"],
                "edge": round(model_prob - px_tick, 4),
                "net_power_bps": snap["net_power_bps"],
                "elapsed_sec": elapsed_sec,
                "regime": snap["regime"],
                "official_outcome": None,
                "pnl_usdc": 0.0,
                "created_at": now,
                "filled_at": None,
                "settled_at": None,
                "book_bid": book["best_bid"],
                "book_ask": book["best_ask"],
                "exit_arm": exit_arm,
                "entry_strategy": entry_strategy,
                "bal_before": bal_before_snap,
                "bal_after_entry": None,
                "bal_after_settle": None,
                "recon_status": "PENDING",
                "recon_diff": None,
                "recon_note": f"发单前可用 ${bal_before_snap:.2f}",
                "notes": f"实盘提交中[{extra_note}] | 盘口 {book['best_bid']:.2f}/{book['best_ask']:.2f}",
            }
            if not self.db.try_insert_order_atomic(placeholder):
                self._set_decision("本场已有活跃实盘订单，原子锁已拦截重复下单", sticky=False, now=now)
                return

            token_id = self.current_up_token if sig == "UP" else self.current_down_token
            try:
                res = await self.account.place_live_order(
                    token_id, px_tick, size_usdc, is_tk, int(arm["ttl_ms"]),
                    tick_size=self.clob.get_tick_size(sig),
                    min_size=self.current_min_size,
                    geoblocked=bool(self.oracle.status["geo_check"].get("blocked")),
                    neg_risk=self.current_neg_risk,
                    market_version=self.current_market_version,
                    condition_id=self.current_condition_id,
                    max_price_override=max_price_override,
                    bal_before=bal_before_snap,
                )
                matched = float(res.get("matched_shares") or 0.0)
                is_delayed = bool(res.get("is_delayed", False))
                filled_now = matched > 0.0
                m_px = float(res.get("matched_price") or px_tick)
                real_cost = round(matched * m_px, 4) if filled_now else size_usdc
                init_fee = round(matched * self.current_taker_fee_rate * m_px * (1.0 - m_px), 4) if (filled_now and is_tk) else 0.0

                # Stage 4-5 (v46): MATCHED / DELAYED enter CONFIRMING until on-chain settlement verifies real position!
                if filled_now or is_delayed:
                    init_status = "CONFIRMING"
                    self.account.reserve_local_cash(real_cost + init_fee if filled_now else req_entry_cash)
                elif is_tk:
                    init_status = "CANCELLED"
                else:
                    init_status = "PENDING"
                    self.account.reserve_local_cash(req_entry_cash)

                final_order = dict(placeholder)
                final_order.update({
                    "order_id": res["order_id"] or tmp_oid,
                    "fill_price": round(m_px, 4) if filled_now else None,
                    "size_usdc": real_cost,
                    "shares": matched if filled_now else res["shares"],
                    "fee_usdc": init_fee,
                    "status": init_status,
                    "filled_at": now if filled_now else None,
                    "settled_at": now if init_status == "CANCELLED" else None,
                    "bal_after_settle": bal_before_snap if init_status == "CANCELLED" else None,
                    "recon_status": "OK" if init_status == "CANCELLED" else "PENDING",
                    "recon_diff": 0.0 if init_status == "CANCELLED" else None,
                    "recon_note": f"FAK未成无扣款(余额${bal_before_snap:.2f})" if init_status == "CANCELLED" else f"等待链上确权 (前${bal_before_snap:.2f})",
                    "notes": f"实盘V2[{extra_note}] {res.get('exchange_status')}({res.get('raw_summary') or ''}) 离场:{exit_arm} | 盘口 {book['best_bid']:.2f}/{book['best_ask']:.2f}",
                })
                self.db.replace_order_id(tmp_oid, final_order)

                if init_status == "CANCELLED":
                    self.last_cancel_ts = now
                    self.last_cancel_side = sig
                    self._set_decision(f"[实盘FAK未成] {sig}@{px_tick:.2f} 对手盘不足", sticky=False, now=now)
                else:
                    self.last_signal_ts = now
                    self.round_orders_count += 1
                    st_txt = "链上确权中" if init_status == "CONFIRMING" else "挂单中"
                    self._set_decision(f"[实盘·{st_txt}] {sig} @{m_px if filled_now else px_tick:.2f} (${real_cost:.2f})", sticky=True, now=now)
            except Exception as e:
                err_s = str(e)[:90]
                rej = dict(placeholder)
                rej.update({
                    "status": "REJECTED",
                    "settled_at": now,
                    "bal_after_settle": bal_before_snap,
                    "recon_status": "OK",
                    "recon_diff": 0.0,
                    "recon_note": f"拒单/异常未扣款(余额${bal_before_snap:.2f}): {err_s[:45]}",
                    "notes": f"实盘下单失败: {err_s}",
                })
                self.db.save_order(rej)
                self._set_decision(f"[实盘失败] {err_s[:60]}", sticky=False, now=now)
                await self._send_webhook("实盘下单失败", err_s)

    async def _evaluate_fv_discount_leader(
        self, now, elapsed_sec, snap, model_up_p, model_dn_p, up_book, dn_book, up_edge, dn_edge, risk_cfg
    ):
        """
        v47 Clean-Slate Verified Execution Path (`FV_DISCOUNT_LEADER`):
        - Entry: Pure F0 Discounted Leader FAK (z>=0, ask 35-56c, fee-net edge >= +6%, 35-240s, fixed stake).
        - Exit: User-selected G3 (`TIME_TWAP_MOAT` default) / G2 (`DYNAMIC_EV`) / G0 (`HOLD_EXPIRY`), zero SL.
        - Zero EMA13 / Elder Ray / 11-dim logit hallucinations / Maker orders / Arb-Lock / auto-drift.
        """
        strict_5s = bool(risk_cfg.get("fv_strict_5s_confirm", False))
        exit_mode = self.fv_strategy.resolve_exit_mode(risk_cfg)
        fv_sig, fv_block = self.fv_strategy.evaluate_entry(
            self.current_window_ts,
            elapsed_sec,
            snap,
            up_book,
            dn_book,
            taker_fee_rate=self.current_taker_fee_rate,
            strict_5s_confirm=strict_5s,
            advance_streak=True,
        )
        sig_side = fv_sig["side"] if fv_sig else "NONE"
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
            "signal": sig_side,
            "trigger_source": fv_sig["trigger_label"] if fv_sig else "FV贴水领跑",
        })
        if fv_block:
            b_cat = "确认中" if "确认中" in fv_block else ("甜区外" if "甜区" in fv_block else "盘口过滤")
            self._record_block(fv_block, b_cat, now=now)

        if elapsed_sec < 4 or self.current_ptb is None or not self.current_up_token:
            return

        mode = self.db.get_trading_mode()
        paused = self.db.is_trading_paused()
        in_breaker = now < self.circuit_breaker_until
        perf = self.db.get_performance_stats(mode)
        daily_loss_hit = perf["daily_pnl"] <= -abs(float(risk_cfg.get("max_daily_loss_usdc", 50.0)))
        effective_mode = "SHADOW" if (paused or in_breaker or daily_loss_hit) else mode
        raw_mode_tag = "PAUSED" if paused else mode

        pos_state = self._get_round_position_state(self.current_window_ts, effective_mode)
        decision_stale = (
            self._decision_window_ts != self.current_window_ts
            or ((not self._decision_sticky) and (now - self._decision_ts >= 15.0))
        )
        exit_lbl_map = {
            "TIME_TWAP_MOAT": "监控动态护城河离场(G3)",
            "DYNAMIC_EV": "监控纯EV溢价离场(G2)",
            "HOLD_EXPIRY": "持有到期结算(G0)",
        }
        if decision_stale and not fv_sig and not fv_block:
            if pos_state["has_pending"]:
                live_msg = "订单确权/撮合中，锁定本场"
            elif pos_state["primary_filled"] is not None:
                pf0 = pos_state["primary_filled"]
                if pf0.get("status") == "CLOSED":
                    live_msg = f"本场已平仓（{pf0.get('exit_reason') or '动态FAK止盈'}），等待下场"
                else:
                    p0_px = float(pf0.get("fill_price") or pf0["limit_price"])
                    live_msg = f"本场已持 {pf0['side']}@{p0_px:.2f}（{exit_lbl_map.get(exit_mode, '监控离场')}）"
            elif paused:
                live_msg = f"{'实盘' if mode == 'LIVE' else '模拟'}已暂停下单（仅影子观测）"
            elif in_breaker:
                live_msg = f"连亏熔断保护中（剩{max(1, int(self.circuit_breaker_until - now))}s）"
            elif daily_loss_hit:
                live_msg = "已触及单日亏损上限，暂停开仓"
            elif elapsed_sec < self.fv_strategy.ELAPSED_MIN_SEC:
                live_msg = f"开场观察期（{elapsed_sec}s / {self.fv_strategy.ELAPSED_MIN_SEC}s后进入FV扫描）"
            elif elapsed_sec > self.fv_strategy.ELAPSED_MAX_SEC:
                live_msg = "已过240s安全窗口（避开尾盘负期望区）"
            else:
                live_msg = "FV贴水领跑巡航中（等待领跑方35~56¢且含费净优势≥6%）"
            if live_msg != self.latest_pricing.get("last_decision") or self._decision_window_ts != self.current_window_ts:
                self.latest_pricing["last_decision"] = live_msg
                self.latest_pricing["last_decision_ts"] = round(now, 1)
                self._decision_ts = now
                self._decision_window_ts = self.current_window_ts

        # 1. Single active order per round
        if pos_state["has_pending"]:
            return

        # 2. Already holding primary position -> check Dynamic FAK Exit (G3 / G2 / G0, ZERO Arb-Lock, ZERO SL)
        if pos_state["primary_filled"] is not None:
            pf = pos_state["primary_filled"]
            if pf.get("status") == "CLOSED":
                return
            if effective_mode in ("PAPER", "LIVE"):
                await self._try_early_exit(
                    now, elapsed_sec, effective_mode, pf, model_up_p, model_dn_p, up_book, dn_book, risk_cfg, snap=snap
                )
            return

        # 3. Evaluate fresh Discounted Leader FAK entry
        if not fv_sig:
            return
        if now - self.last_cancel_ts < 3.0:
            return
        if self.last_signal_ts > 0 and (now - self.last_signal_ts) < 15.0:
            return

        side = fv_sig["side"]
        if not self.clob.is_book_fresh(side, max_age_sec=8.0) or not self.clob.is_parity_ok():
            self._record_block("实时订单簿延迟或双边未对齐，跳过下单", "盘口异常", now=now)
            return

        book = up_book if side == "UP" else dn_book
        limit_px = float(fv_sig["limit_price"])
        spendable: Optional[float] = None
        if effective_mode == "LIVE":
            ci = await self.account.ensure_fresh_available_cash(max_age_sec=4.0, cache_key=str(self.current_condition_id or ""))
            if not ci["cash_verified"]:
                self._record_block(f"实盘余额核验失败，暂缓开单({ci.get('error') or '请刷新资产'})", "余额核验", now=now)
                return
            spendable = float(ci["available_cash"])

        size_usdc = self.fv_strategy.calculate_fixed_stake(
            risk_cfg,
            limit_px,
            taker_fee_rate=self.current_taker_fee_rate,
            spendable_cash_usdc=spendable,
            min_shares=self.current_min_size if effective_mode == "LIVE" else 1.0,
        )
        if size_usdc <= 0:
            min_n = max(HARD_LIMITS["min_unit_usdc"], round(float(self.current_min_size or 5.0) * limit_px + 0.01, 2))
            need_c = self.learner.required_buy_cash(min_n, limit_px, True, self.current_taker_fee_rate)
            self._record_block(f"实盘可用 pUSD 不足 (可用${spendable or 0.0:.2f} < 最低需${need_c:.2f}含费)", "余额不足", now=now)
            return

        exit_arm_tag = {"TIME_TWAP_MOAT": "FV_TIME_TWAP", "DYNAMIC_EV": "FV_DYNAMIC_EV", "HOLD_EXPIRY": "HOLD_EXPIRY"}[exit_mode]
        await self._submit_entry_order(
            now, elapsed_sec, effective_mode, raw_mode_tag, in_breaker,
            side, fv_sig["arm"], limit_px, size_usdc, fv_sig["fair_prob"], book, snap,
            extra_note="FV贴水领跑",
            exit_arm=exit_arm_tag,
            entry_strategy="fv_discount_leader",
            max_price_override=fv_sig["max_fak_price"],
        )

    async def _evaluate_and_trade(self, now, elapsed_sec, snap, model_up_p, model_dn_p, up_book, dn_book, up_edge, dn_edge):
        risk_cfg = self.db.get_json("risk_config", {})
        if str(risk_cfg.get("strategy_engine") or "FV_DISCOUNT_LEADER").upper() == "FV_DISCOUNT_LEADER":
            await self._evaluate_fv_discount_leader(
                now, elapsed_sec, snap, model_up_p, model_dn_p, up_book, dn_book, up_edge, dn_edge, risk_cfg
            )
            return
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

        mode = self.db.get_trading_mode()
        paused = self.db.is_trading_paused()
        in_breaker = now < self.circuit_breaker_until
        perf = self.db.get_performance_stats(mode)
        daily_loss_hit = perf["daily_pnl"] <= -abs(float(risk_cfg.get("max_daily_loss_usdc", 100.0)))
        effective_mode = "SHADOW" if (paused or in_breaker or daily_loss_hit) else mode
        raw_mode_tag = "PAUSED" if paused else mode

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
            elif paused:
                acct_lbl = "实盘" if mode == "LIVE" else "模拟"
                live_msg = f"{acct_lbl}已暂停下单（仅影子学习中）"
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
        sniper = self.learner.evaluate_twap_sniper(
            snap, elapsed_sec, up_book, dn_book, risk_cfg,
            taker_fee_rate_override=self.current_taker_fee_rate,
        )
        if sniper:
            s_side = sniper["side"]
            if self.clob.is_book_fresh(s_side, max_age_sec=8.0) and self.clob.is_parity_ok():
                s_book = up_book if s_side == "UP" else dn_book
                s_mode = "SHADOW" if sniper.get("shadow_only") else effective_mode
                if s_mode == "SHADOW":
                    sh_state = self._get_round_position_state(self.current_window_ts, "SHADOW")
                    if sh_state["has_pending"] or sh_state["primary_filled"] is not None:
                        s_mode = ""
                if s_mode:
                    s_arm = sniper["arm"]
                    s_lim = float(sniper["limit_price"])
                    if s_mode == "LIVE" and bool(risk_cfg.get("live_taker_only", True)) and not s_arm.get("is_taker"):
                        s_arm = {"id": "TAKER_FAST", "offset_cents": 0.00, "ttl_ms": 1500, "is_taker": True}
                        s_lim = float(s_book.get("best_ask") or s_lim)
                    s_is_tk = bool(s_arm.get("is_taker"))

                    spendable_s: Optional[float] = None
                    cash_ok_s = True
                    if s_mode == "LIVE":
                        ci_s = await self.account.ensure_fresh_available_cash(max_age_sec=4.0, cache_key=str(self.current_condition_id or ""))
                        if not ci_s["cash_verified"]:
                            cash_ok_s = False
                            self._record_block(f"实盘余额核验失败({ci_s.get('error') or '请刷新资产'})", "余额核验", now=now)
                        else:
                            spendable_s = float(ci_s["available_cash"])
                            bankroll = max(float(ci_s["total_equity"]), spendable_s)
                    else:
                        bankroll = self.paper.get_balance()

                    if cash_ok_s:
                        size_usdc = self.learner.calculate_position_size(
                            bankroll, sniper["true_prob"], s_lim,
                            s_is_tk, risk_cfg,
                            remaining_sec=int(snap.get("remaining_sec", 30)),
                            is_twap_sniper=True,
                            taker_fee_rate_override=self.current_taker_fee_rate,
                            spendable_cash_usdc=spendable_s,
                            min_shares=self.current_min_size if s_mode == "LIVE" else 1.0,
                        )
                        if size_usdc > 0:
                            self.latest_pricing["signal"] = s_side
                            await self._submit_entry_order(
                                now, elapsed_sec, s_mode, raw_mode_tag, in_breaker,
                                s_side, s_arm, s_lim, size_usdc,
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
        if target_mode == "LIVE" and bool(risk_cfg.get("live_taker_only", True)):
            arm = next((a for a in self.learner.state["arms"] if a.get("is_taker")), None) or {
                "id": "TAKER_FAST", "offset_cents": 0.00, "ttl_ms": 1500, "is_taker": True
            }
        else:
            arm = self.learner.select_execution_arm(
                edge_est,
                snap["regime"] == "TRENDING" or entry_strat in ("orderflow_lead", "energy_accum"),
                recent_cancel_same_side=recent_cancel_same,
                best_ask=book["best_ask"],
                taker_fee_rate_override=self.current_taker_fee_rate,
            )
        max_px = float(auto_params.get("max_entry_price", 0.75))
        limit_px = self.learner.compute_order_price(arm, book["best_bid"], book["best_ask"], max_px)
        if limit_px is None:
            self._record_block(
                f"{sig}({trigger_src})盘口 {book['best_bid']:.2f}/{book['best_ask']:.2f} 超出AI赔率上限({max_px:.2f})",
                "赔率超限", now=now,
            )
            return

        is_tk_std = bool(arm.get("is_taker"))
        spendable_std: Optional[float] = None
        if target_mode == "LIVE":
            ci = await self.account.ensure_fresh_available_cash(max_age_sec=4.0, cache_key=str(self.current_condition_id or ""))
            if not ci["cash_verified"]:
                self._record_block(f"实盘余额核验失败，暂缓开单({ci.get('error') or '请刷新资产'})", "余额核验", now=now)
                return
            spendable_std = float(ci["available_cash"])
            bankroll = max(float(ci["total_equity"]), spendable_std)
            min_notional_live = max(HARD_LIMITS["min_unit_usdc"], round(float(self.current_min_size or 5.0) * limit_px + 0.01, 2))
            min_cash_live = self.learner.required_buy_cash(min_notional_live, limit_px, is_tk_std, self.current_taker_fee_rate)
            if spendable_std < min_cash_live:
                lock_hint = f"，挂单锁定${ci['locked']:.2f}" if ci["locked"] > 0 else ""
                self._record_block(
                    f"实盘可用 pUSD 不足 (可用${spendable_std:.2f} < 最低需${min_cash_live:.2f}含费{lock_hint})",
                    "余额不足", now=now,
                )
                return
        else:
            bankroll = self.paper.get_balance()

        rem_sec = int(snap.get("remaining_sec", max(1, 300 - elapsed_sec)))
        z_now = float(snap.get("moneyness_z", 0.0))
        is_buying_itm = (sig == "UP" and z_now > 0.35) or (sig == "DOWN" and z_now < -0.35)
        size_usdc = self.learner.calculate_position_size(
            bankroll, model_prob, limit_px, is_tk_std, risk_cfg,
            remaining_sec=rem_sec, iv_state=str(snap.get("iv_state", "FAIR")), is_buying_itm=is_buying_itm,
            taker_fee_rate_override=self.current_taker_fee_rate,
            spendable_cash_usdc=spendable_std,
            min_shares=self.current_min_size if target_mode == "LIVE" else 1.0,
        )

        if size_usdc <= 0:
            req_edge, _ = self.learner.compute_dynamic_min_edge(
                limit_px, risk_cfg, remaining_sec=rem_sec,
                iv_state=str(snap.get("iv_state", "FAIR")), is_buying_itm=is_buying_itm,
            )
            fee_ps = self.current_taker_fee_rate * limit_px * (1.0 - limit_px) if bool(arm.get("is_taker")) else 0.0
            net_e = (model_prob - (limit_px + fee_ps)) * 100.0
            self._record_block(
                f"{sig}({trigger_src})未达动态门槛 (净{net_e:+.1f}% < {limit_px:.2f}价位需{req_edge*100:.1f}%)",
                "优势不足", now=now,
            )
            return

        allow_arb = (target_mode != "LIVE") or bool(risk_cfg.get("live_enable_arb_lock", False))
        chosen_exit_arm = self.learner.select_exit_arm(
            entry_strat, str(snap.get("regime", "CHOPPY")), elapsed_sec, allow_arb_lock=allow_arb
        )
        await self._submit_entry_order(
            now, elapsed_sec, target_mode, raw_mode_tag, in_breaker,
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
        last_geo = time.time()
        last_trade_sync = 0.0
        while not self._stop:
            now = time.time()
            if now - last_geo >= 60:
                last_geo = now
                await self.oracle.check_geo_and_clock()

            if now - last_trade_sync >= 10:
                last_trade_sync = now
                if self.db.get_trading_mode() == "LIVE":
                    await self.reconcile_live_orders()

            # Periodic orphan settlement sweep every 60s so no order ever stays stuck
            if now - last_recover >= 60:
                last_recover = now
                await self.recover_unsettled_windows()

            # Refresh wallet balances every 15s in LIVE mode (or 45s in PAPER) with condition_id cache key
            bal_interval = 15 if self.db.get_trading_mode() == "LIVE" else 45
            if now - last_bal >= bal_interval:
                last_bal = now
                if self.account.get_public_wallet_info().get("has_private_key"):
                    await self.account.refresh_balances_and_portfolio(cache_key=str(self.current_condition_id or ""))

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
            mismatch_tag = "⚠待核 " if (m == "LIVE" and pf.get("recon_status") == "MISMATCH") else ""
            cur_bid = float(self.clob.books[pf["side"]].get("best_bid") or 0.0)
            if cur_bid > 0:
                sh = float(pf.get("shares") or (float(pf["size_usdc"]) / max(0.01, px)))
                u_pnl = sh * cur_bid - float(pf["size_usdc"])
                return f"{mismatch_tag}持 {pf['side']}@{px:.2f} (浮{u_pnl:+.2f})"
            return f"{mismatch_tag}已持主仓 {pf['side']}@{px:.2f}"
        if ps.get("confirming_order"):
            co = ps["confirming_order"]
            cpx = float(co.get("fill_price") or co.get("limit_price") or 0.0)
            return f"链上确权中 {co['side']}@{cpx:.2f}"
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
                is_arb = (o.get("arm_id") in ARB_ARM_IDS)
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
        now_srv = self.clock.server_now()
        elapsed = max(0, min(300, int(now_srv - self.current_window_ts))) if self.current_window_ts else 0
        remaining = 300 - elapsed
        mode = self.db.get_trading_mode()
        paused = self.db.is_trading_paused()

        eff_twap = self.oracle.get_effective_twap60(now_srv)
        ptb_str = str(self.current_ptb.quantize(Decimal("0.01"))) if self.current_ptb is not None else "--"
        twap_str = str(eff_twap.quantize(Decimal("0.01"))) if eff_twap > 0 else "--"
        twap_diff = None
        if self.current_ptb is not None and eff_twap > 0:
            twap_diff = str((eff_twap - self.current_ptb).quantize(Decimal("0.01")))
        best_spot = self.cex.get_best_spot()
        oracle_spot_val = round(best_spot - self.basis_ema, 2) if best_spot > 0 else (
            round(self.oracle.latest_spot_float, 2) if self.oracle.latest_spot_float > 0 else 0.0
        )
        spot_twap_diff = None
        if oracle_spot_val > 0 and eff_twap > 0:
            spot_twap_diff = f"{(oracle_spot_val - float(eff_twap)):.2f}"

        return {
            "ts": now,
            "mode": mode,
            "paused": paused,
            "system": {**self.monitor.sample(), "duplicate_instance": self.is_duplicate_instance},
            "feeds": {
                "binance": self.cex.status["binance"],
                "binance_perp": self.cex.status["binance_perp"],
                "polybolt": self.oracle.status["polybolt"],
                "rtds": self.oracle.status["rtds"],
                "active_oracle": self.oracle.active_oracle_source,
                "gamma": self.oracle.status["gamma"],
                "clob": self.clob.refresh_status_metrics(),
                "geo_check": self.oracle.status["geo_check"],
                "twap_err": self.oracle.status["twap_err"],
                "live_channel": self.account.live_channel_status,
            },
            "round": {
                "window_ts": self.current_window_ts,
                "slug": self.current_slug,
                "elapsed_sec": elapsed,
                "remaining_sec": remaining,
                "price_to_beat": ptb_str,
                "ptb_source": self.current_ptb_source,
                "target_confirmed": self.current_ptb_official,
                "tick_size": self.clob.get_tick_size("UP"),
                "min_size": self.current_min_size,
                "taker_fee_rate": self.current_taker_fee_rate,
                "accepting_orders": self.current_accepting_orders,
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
            "audit": {
                "PAPER": self.db.get_account_audit_summary("PAPER"),
                "LIVE": self.db.get_account_audit_summary("LIVE"),
            },
            "learning": self.learner.get_dashboard_summary(),
            "dataset_counts": self.db.get_slice_counts(),
            "training_status": self.training_status,
            "wallet": self.account.get_public_wallet_info(),
            "portfolio": self.account.cached_portfolio,
            "chart_points": list(self.live_chart_buffer),
            "round_events": self._build_round_chart_events(mode),
        }
