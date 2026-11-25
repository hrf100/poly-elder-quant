import time
import uuid
from backend.config import HARD_LIMITS
from backend.storage.db import Database


class PaperExecutionEngine:
    """
    Strict Real-Orderbook Paper Matching Engine (v18):
    - Entry matching: 300ms latency, real live best_bid/best_ask recorded, zero stale last_trade fills.
    - Early-Exit SELL matching:
        * 0%-fee Post-Only Maker limit SELL (used for Fair-Value TP & Proportional TP):
          rests at limit_price (> best_bid at submission) after 300ms latency, fills with $0 exit fee
          ONLY when live best_bid rises to/above limit_price OR a fresh post-activation BUY trade hits >= limit_price.
          Cleanly expires after ttl_ms if unfilled so position remains open without ghost fills.
        * Emergency Taker SELL (used for Stop-Loss):
          executes into live T+300ms best_bid and deducts exact dynamic Polymarket taker fee.
    - Settles remaining FILLED orders strictly using Polymarket official outcome (zero double counting on CLOSED).
    """

    def __init__(self, db: Database, on_order_cancelled=None):
        self.db = db
        self.on_order_cancelled = on_order_cancelled
        self.pending_orders: list[dict] = []
        self.pending_exits: list[dict] = []

    def get_balance(self) -> float:
        return float(self.db.get_kv("paper_balance", "1000.0"))

    def has_pending_exit(self, order_id: str) -> bool:
        return any(e["order_id"] == order_id for e in self.pending_exits)

    def submit_order(
        self,
        window_ts: int,
        side: str,
        arm: dict,
        limit_price: float,
        size_usdc: float,
        model_win_prob: float,
        market_implied_prob: float,
        edge: float,
        net_power_bps: float,
        elapsed_sec: int,
        regime: str,
        mode: str = "PAPER",
        book_bid: float = 0.0,
        book_ask: float = 0.0,
        extra_note: str = "",
        exit_arm: str = "HOLD_EXPIRY",
        entry_strategy: str = "momentum_breakout",
        max_price_override: float | None = None,
    ) -> dict | None:
        now = time.time()
        tag = f" | {extra_note}" if extra_note else ""
        order = {
            "order_id": f"{mode.lower()}_{uuid.uuid4().hex[:12]}",
            "window_ts": window_ts,
            "mode": mode,
            "side": side,
            "arm_id": arm["id"],
            "limit_price": round(limit_price, 4),
            "fill_price": None,
            "size_usdc": round(size_usdc, 2),
            "shares": None,
            "fee_usdc": 0.0,
            "status": "PENDING",
            "model_win_prob": round(model_win_prob, 4),
            "market_implied_prob": round(market_implied_prob, 4),
            "edge": round(edge, 4),
            "net_power_bps": round(net_power_bps, 3),
            "elapsed_sec": elapsed_sec,
            "regime": regime,
            "official_outcome": None,
            "pnl_usdc": 0.0,
            "created_at": now,
            "filled_at": None,
            "settled_at": None,
            "book_bid": round(book_bid, 4) if book_bid > 0 else None,
            "book_ask": round(book_ask, 4) if book_ask > 0 else None,
            "exit_price": None,
            "exit_reason": None,
            "exit_arm": exit_arm,
            "entry_strategy": entry_strategy,
            "bal_before": round(self.get_balance(), 4) if mode == "PAPER" else None,
            "bal_after_entry": None,
            "bal_after_settle": None,
            "recon_status": "PENDING" if mode == "PAPER" else None,
            "recon_diff": None,
            "recon_note": None,
            "notes": (
                f"盘口 {book_bid:.2f}/{book_ask:.2f}"
                + (f" | 封顶{float(max_price_override):.2f}" if max_price_override is not None else "")
                + f" | 离场:{exit_arm} | {'吃单' if arm.get('is_taker') else '挂单'}{tag}"
            ),
            "_active_after": now + 0.30,
            "_expire_at": now + 0.30 + float(arm["ttl_ms"]) / 1000.0,
            "_is_taker": bool(arm.get("is_taker", False)),
            "_arrived_checked": False,
            "_max_accept_price": round(float(max_price_override), 4) if max_price_override is not None else None,
        }
        # Atomic cross-process & in-process single-slot guard: refuse duplicate primary/arb order in same round!
        arb_ids = ("ARB_LOCK_MAKER", "ARB_LOCK_TAKER")
        is_arb = (arm["id"] in arb_ids)
        for existing in self.pending_orders:
            if existing["window_ts"] == window_ts and existing["mode"] == mode and existing["status"] == "PENDING":
                if (existing["arm_id"] in arb_ids) == is_arb:
                    return None

        clean = {k: v for k, v in order.items() if not k.startswith("_")}
        if not self.db.try_insert_order_atomic(clean):
            return None

        self.pending_orders.append(order)
        return order

    def submit_early_exit(
        self,
        order: dict,
        exit_reason: str,
        trigger_price: float,
        detail: str = "",
        is_maker: bool = False,
        ttl_ms: int = 8000,
    ) -> bool:
        """
        Queue an intraday SELL exit for a FILLED paper order:
        - is_maker=True:  0%-fee Post-Only Maker limit SELL resting at trigger_price for ttl_ms.
        - is_maker=False: Fast Taker SELL into live best_bid after 300ms latency.
        """
        if order.get("status") != "FILLED" or self.has_pending_exit(order["order_id"]):
            return False
        now = time.time()
        self.pending_exits.append({
            "order_id": order["order_id"],
            "window_ts": order["window_ts"],
            "side": order["side"],
            "mode": order["mode"],
            "exit_reason": exit_reason,
            "limit_price": round(trigger_price, 4),
            "is_maker": bool(is_maker),
            "detail": detail,
            "created_at": now,
            "_active_after": now + 0.30,
            "_expire_at": now + 0.30 + (float(ttl_ms) / 1000.0 if is_maker else 3.0),
            "_arrived_checked": False,
            "order_ref": dict(order),
        })
        return True

    def _persist(self, order: dict):
        clean = {k: v for k, v in order.items() if not k.startswith("_")}
        self.db.save_order(clean)

    def _notify_cancel(self, order: dict):
        if self.on_order_cancelled:
            try:
                self.on_order_cancelled(order)
            except Exception:
                pass

    def _process_pending_exits(self, side: str, book: dict, event_type: str = "book"):
        if not self.pending_exits:
            return
        now = time.time()
        best_bid = float(book.get("best_bid") or 0.0)
        last_trade = float(book.get("last_trade") or 0.0)
        last_trade_ts = float(book.get("last_trade_ts") or 0.0)
        last_trade_side = str(book.get("last_trade_side") or "").upper()
        still_exiting = []

        for ex in self.pending_exits:
            if now >= ex["_expire_at"]:
                continue  # Maker TP expired unfilled -> position stays open FILLED cleanly
            if ex["side"] != side or now < ex["_active_after"]:
                still_exiting.append(ex)
                continue

            exec_px = None
            charge_exit_taker_fee = False
            fill_tag = ""

            if ex["is_maker"]:
                lim = float(ex["limit_price"])
                if not ex["_arrived_checked"]:
                    ex["_arrived_checked"] = True
                    # If live best_bid ALREADY rose >= limit_price upon arrival at 300ms: GTC crosses at best_bid (taker fee)
                    if best_bid >= lim:
                        exec_px = best_bid
                        charge_exit_taker_fee = True
                        fill_tag = f"抵达即成交@{best_bid:.2f}"
                else:
                    # Resting Maker SELL -> 0% fee ONLY when live best_bid reaches limit_price OR fresh post-activation BUY trade hits >= limit_price
                    if event_type in ("book", "price_change", "sweep") and best_bid >= lim:
                        exec_px = lim
                        charge_exit_taker_fee = False
                        fill_tag = f"0费Maker止盈@{lim:.2f}"
                    elif event_type == "trade" and last_trade > 0 and last_trade_ts > ex["_active_after"]:
                        if last_trade > lim or (abs(last_trade - lim) < 1e-6 and last_trade_side == "BUY"):
                            exec_px = lim
                            charge_exit_taker_fee = False
                            fill_tag = f"0费Maker被扫@{lim:.2f}"
            else:
                if best_bid > 0.01:
                    is_fv_ex = (str(ex["order_ref"].get("arm_id") or "") == "FV_LEADER_FAK")
                    exec_px = max(0.01, round(best_bid - (0.01 if is_fv_ex else 0.0), 4))
                    charge_exit_taker_fee = True
                    fill_tag = f"悲观FAK止盈(-1¢滑)@{exec_px:.2f}" if is_fv_ex else f"市价平仓@{best_bid:.2f}"

            if exec_px is not None:
                o = ex["order_ref"]
                shares = float(o.get("shares") or (float(o["size_usdc"]) / max(0.01, float(o.get("fill_price") or o["limit_price"]))))
                entry_fee = float(o.get("fee_usdc") or 0.0)
                exit_fee = (shares * HARD_LIMITS["taker_fee_rate"] * exec_px * (1.0 - exec_px)) if charge_exit_taker_fee else 0.0
                proceeds = shares * exec_px
                pnl = round(proceeds - float(o["size_usdc"]) - entry_fee - exit_fee, 4)

                o["status"] = "CLOSED"
                o["exit_price"] = round(exec_px, 4)
                o["exit_reason"] = ex["exit_reason"]
                o["fee_usdc"] = round(entry_fee + exit_fee, 4)
                o["pnl_usdc"] = pnl
                o["settled_at"] = now
                o["notes"] = (o.get("notes") or "") + f" | {ex['exit_reason']} {fill_tag} (净{pnl:+.2f}, {int((now - ex['created_at'])*1000)}ms)"
                self._persist(o)

                if o["mode"] == "PAPER":
                    prev_bal = self.get_balance()
                    new_bal = round(prev_bal + pnl, 4)
                    self.db.set_kv("paper_balance", str(new_bal))
                    b0 = float(o.get("bal_before") if o.get("bal_before") is not None else prev_bal)
                    rdiff = round((new_bal - prev_bal) - pnl, 4)
                    o["bal_after_settle"] = new_bal
                    o["recon_diff"] = rdiff
                    o["recon_status"] = "OK" if abs(rdiff) <= 0.02 else "MISMATCH"
                    o["recon_note"] = f"平仓: 前${b0:.2f}→结后${new_bal:.2f} | 理论盈亏{pnl:+.2f} (差{rdiff:+.2f})"
                    self._persist(o)
                    stats = self.db.get_performance_stats("PAPER")
                    self.db.record_equity("PAPER", int(o["window_ts"]), new_bal, stats["total_pnl"], stats["total_trades"])
            else:
                still_exiting.append(ex)

        self.pending_exits = still_exiting

    def sweep_expired_orders(self, books: dict | None = None):
        now = time.time()
        still_pending = []
        for o in self.pending_orders:
            if o["status"] != "PENDING":
                continue
            if now >= o["_expire_at"]:
                o["status"] = "CANCELLED"
                o["notes"] = (o["notes"] or "") + " | 超时未成交"
                if o.get("mode") == "PAPER":
                    cb = round(self.get_balance(), 4)
                    o["bal_after_settle"] = cb
                    o["recon_diff"] = 0.0
                    o["recon_status"] = "OK"
                    o["recon_note"] = f"撤单无扣款 (余额${cb:.2f})"
                self._persist(o)
                self._notify_cancel(o)
                continue
            still_pending.append(o)
        self.pending_orders = still_pending
        if books:
            for side in ("UP", "DOWN"):
                b = books.get(side)
                if b and float(b.get("updated_at") or 0.0) > 0:
                    self.on_orderbook_tick(side, b, event_type="sweep")

    def on_orderbook_tick(self, side: str, book: dict, event_type: str = "book"):
        if not self.pending_orders and not self.pending_exits:
            return
        # 1. Process pending intraday SELL exits first
        if self.pending_exits:
            self._process_pending_exits(side, book, event_type=event_type)

        if not self.pending_orders:
            return
        # 2. Process pending entry BUY orders
        now = time.time()
        best_ask = float(book.get("best_ask") or 0.0)
        last_trade = float(book.get("last_trade") or 0.0)
        last_trade_ts = float(book.get("last_trade_ts") or 0.0)
        last_trade_side = str(book.get("last_trade_side") or "").upper()
        still_pending = []

        for o in self.pending_orders:
            if o["status"] != "PENDING":
                continue
            if now >= o["_expire_at"]:
                o["status"] = "CANCELLED"
                o["notes"] = (o["notes"] or "") + " | 超时未成交"
                if o.get("mode") == "PAPER":
                    cb = round(self.get_balance(), 4)
                    o["bal_after_settle"] = cb
                    o["recon_diff"] = 0.0
                    o["recon_status"] = "OK"
                    o["recon_note"] = f"撤单无扣款 (余额${cb:.2f})"
                self._persist(o)
                self._notify_cancel(o)
                continue

            if o["side"] != side or now < o["_active_after"]:
                still_pending.append(o)
                continue

            filled_px = None
            charge_taker_fee = False
            fill_reason = ""

            if o["_is_taker"]:
                if best_ask > 0:
                    is_fv_pess = (str(o.get("arm_id") or "") == "FV_LEADER_FAK")
                    pess_slip = 0.01 if is_fv_pess else 0.0
                    custom_cap = o.get("_max_accept_price")
                    if custom_cap is not None and float(custom_cap) > 0:
                        max_accept = min(round(float(custom_cap), 4), 0.88)
                    else:
                        max_accept = min(round(o["limit_price"] + (0.01 if is_fv_pess else 0.02), 4), 0.88)
                    if best_ask <= max_accept + 1e-6:
                        # Pessimistic +1c fill slippage, never exceeding the FAK order's own max_price cap
                        filled_px = min(max_accept, round(best_ask + pess_slip, 4)) if is_fv_pess else best_ask
                        charge_taker_fee = True
                        fill_reason = (
                            f"动态FAK吃单(顶{max_accept:.2f})@{filled_px:.2f}"
                            if is_fv_pess
                            else f"市价吃卖一@{best_ask:.2f}"
                        )
                    else:
                        o["status"] = "CANCELLED"
                        o["notes"] = (o["notes"] or "") + f" | 300ms滑点超限(卖一{best_ask:.2f}>封顶{max_accept:.2f})"
                        if o.get("mode") == "PAPER":
                            cb = round(self.get_balance(), 4)
                            o["bal_after_settle"] = cb
                            o["recon_diff"] = 0.0
                            o["recon_status"] = "OK"
                            o["recon_note"] = f"撤单无扣款 (余额${cb:.2f})"
                        self._persist(o)
                        self._notify_cancel(o)
                        continue
            else:
                if not o["_arrived_checked"]:
                    o["_arrived_checked"] = True
                    if best_ask > 0 and best_ask <= o["limit_price"]:
                        filled_px = best_ask
                        charge_taker_fee = True
                        fill_reason = f"抵达时卖一已至{best_ask:.2f}(跨价吃单)"
                else:
                    if event_type in ("book", "price_change", "sweep") and best_ask > 0 and best_ask <= o["limit_price"]:
                        filled_px = o["limit_price"]
                        fill_reason = f"实时卖一跌至{best_ask:.2f}成交"
                    elif event_type == "trade" and last_trade > 0 and last_trade_ts > o["_active_after"]:
                        if last_trade < o["limit_price"]:
                            filled_px = o["limit_price"]
                            fill_reason = f"新成交价{last_trade:.2f}穿透挂单"
                        elif abs(last_trade - o["limit_price"]) < 1e-6 and last_trade_side == "SELL":
                            filled_px = o["limit_price"]
                            fill_reason = f"新卖单砸穿买一@{last_trade:.2f}"

            if filled_px is not None:
                shares = o["size_usdc"] / filled_px
                fee = 0.0
                if charge_taker_fee:
                    fee = shares * HARD_LIMITS["taker_fee_rate"] * filled_px * (1.0 - filled_px)
                o["fill_price"] = round(filled_px, 4)
                o["shares"] = round(shares, 4)
                o["fee_usdc"] = round(fee, 4)
                o["status"] = "FILLED"
                o["filled_at"] = now
                if o.get("mode") == "PAPER":
                    b_cur = round(self.get_balance(), 4)
                    o["bal_before"] = b_cur
                    o["bal_after_entry"] = round(b_cur - float(o["size_usdc"]) - float(o["fee_usdc"] or 0.0), 4)
                    o["recon_status"] = "OK"
                    o["recon_diff"] = 0.0
                    o["recon_note"] = f"开仓占用 ${float(o['size_usdc']) + float(o['fee_usdc'] or 0.0):.2f} (前${b_cur:.2f}→可用${o['bal_after_entry']:.2f})"
                o["notes"] = (o["notes"] or "") + f" | {fill_reason} ({int((now - o['created_at'])*1000)}ms)"
                self._persist(o)
            else:
                still_pending.append(o)

        self.pending_orders = still_pending

    def cancel_window_pending(self, window_ts: int):
        still = []
        for o in self.pending_orders:
            if o["window_ts"] == window_ts and o["status"] == "PENDING":
                o["status"] = "CANCELLED"
                o["notes"] = (o["notes"] or "") + " | 场次结束撤单"
                self._persist(o)
            else:
                still.append(o)
        self.pending_orders = still
        self.pending_exits = [e for e in self.pending_exits if e["window_ts"] != window_ts]

    def void_unverified_window(self, window_ts: int) -> None:
        self.cancel_window_pending(window_ts)
        orders = self.db.get_orders_by_window(window_ts)
        for o in orders:
            if o["mode"] in ("PAPER", "SHADOW", "LIVE") and o["status"] in ("PENDING", "FILLED"):
                o["status"] = "UNVERIFIED"
                o["pnl_usdc"] = 0.0
                o["settled_at"] = time.time()
                o["notes"] = (o["notes"] or "") + " | 官方裁定超时，已作废不计盈亏"
                self.db.save_order(o)

    def settle_window(self, window_ts: int, official_outcome: str) -> list[dict]:
        """Settles remaining FILLED orders; preserves already CLOSED early-exit PnL (zero double counting!)."""
        self.cancel_window_pending(window_ts)
        orders = self.db.get_orders_by_window(window_ts)
        paper_balance = self.get_balance()
        paper_changed = False
        settled_list = []

        for o in orders:
            if o["mode"] not in ("PAPER", "SHADOW", "LIVE"):
                continue
            if o["status"] == "CLOSED":
                if not o.get("official_outcome"):
                    o["official_outcome"] = official_outcome
                    self.db.save_order(o)
                settled_list.append(o)
                continue
            if o["status"] == "CANCELLED":
                o["official_outcome"] = official_outcome
                o["settled_at"] = time.time()
                self.db.save_order(o)
                settled_list.append(o)
                continue
            if o["status"] in ("FAILED", "REJECTED", "SUBMITTING", "UNVERIFIED"):
                continue
            if o["status"] == "CONFIRMING":
                if o["mode"] == "LIVE":
                    continue  # Never settle an unconfirmed LIVE order; reconciler must prove fill or FAILED first!
                o["status"] = "CANCELLED"
                o["official_outcome"] = official_outcome
                o["settled_at"] = time.time()
                self.db.save_order(o)
                continue
            if o["status"] == "PENDING":
                o["status"] = "CANCELLED"
                o["official_outcome"] = official_outcome
                o["settled_at"] = time.time()
                o["notes"] = (o["notes"] or "") + " | 场次结束撤单"
                if o["mode"] == "PAPER":
                    o["bal_after_settle"] = round(paper_balance, 4)
                    o["recon_diff"] = 0.0
                    o["recon_status"] = "OK"
                    o["recon_note"] = f"场次结束撤单 (余额${paper_balance:.2f})"
                self.db.save_order(o)
                settled_list.append(o)
                continue
            if o["status"] != "FILLED":
                continue

            won = (o["side"] == official_outcome)
            shares = float(o["shares"] or 0.0)
            size = float(o["size_usdc"] or 0.0)
            fee = float(o["fee_usdc"] or 0.0)
            payout = shares * 1.0 if won else 0.0
            pnl = round(payout - size - fee, 4)

            o["status"] = "SETTLED"
            o["official_outcome"] = official_outcome
            o["pnl_usdc"] = pnl
            o["settled_at"] = time.time()

            if o["mode"] == "PAPER":
                prev_pb = paper_balance
                paper_balance = round(paper_balance + pnl, 4)
                paper_changed = True
                rdiff = round((paper_balance - prev_pb) - pnl, 4)
                o["bal_after_settle"] = paper_balance
                o["recon_diff"] = rdiff
                o["recon_status"] = "OK" if abs(rdiff) <= 0.02 else "MISMATCH"
                o["recon_note"] = f"结算({official_outcome}): 前${prev_pb:.2f}→结后${paper_balance:.2f} | 理论{pnl:+.2f} (差{rdiff:+.2f})"

            self.db.save_order(o)
            settled_list.append(o)

        if paper_changed:
            self.db.set_kv("paper_balance", str(paper_balance))
            stats = self.db.get_performance_stats("PAPER")
            self.db.record_equity("PAPER", window_ts, paper_balance, stats["total_pnl"], stats["total_trades"])

        return settled_list
