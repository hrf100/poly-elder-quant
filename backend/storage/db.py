import json
import sqlite3
import threading
import time
from typing import Any, Optional

from backend.auth_crypto import hash_password
from backend.config import DB_PATH, DEFAULT_PASSWORD


class Database:
    """Thread-safe SQLite WAL persistence layer optimized for low-spec VPS."""

    def __init__(self, db_path=DB_PATH):
        self.db_path = str(db_path)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=15.0)
        self._conn.row_factory = sqlite3.Row
        # v29 read-through memory caches (eliminates 1 Hz SQLite full scans & kv reads)
        self._kv_cache: dict[str, Any] = {}
        self._json_cache: dict[str, Any] = {}
        self._stats_cache: dict[str, tuple[float, dict]] = {}
        self._slice_counts_cache: Optional[dict] = None
        self._window_orders_cache: dict[int, list[dict]] = {}
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
            self._conn.execute("PRAGMA temp_store=MEMORY;")
            self._conn.execute("PRAGMA cache_size=-16000;")
            self._conn.execute("PRAGMA mmap_size=67108864;")
            self._init_schema()
            self._init_defaults()

    def _init_schema(self):
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS rounds (
                window_ts INTEGER PRIMARY KEY,
                slug TEXT NOT NULL,
                condition_id TEXT,
                up_token_id TEXT,
                down_token_id TEXT,
                price_to_beat TEXT,
                ptb_source TEXT,
                settlement_price TEXT,
                official_outcome TEXT,
                status TEXT DEFAULT 'ACTIVE',
                created_at REAL NOT NULL,
                settled_at REAL
            );

            CREATE TABLE IF NOT EXISTS episode_slices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_ts INTEGER NOT NULL,
                elapsed_sec INTEGER NOT NULL,
                ts REAL NOT NULL,
                spot_price REAL,
                twap60_price REAL,
                dist_to_strike_bps REAL,
                ema13_slope_bps REAL,
                net_power_bps REAL,
                bull_power_bps REAL,
                bear_power_bps REAL,
                atr_bps REAL,
                path_efficiency REAL,
                mfe_bps REAL,
                mae_bps REAL,
                locked_twap_ratio REAL,
                pm_up_bid REAL,
                pm_up_ask REAL,
                pm_down_bid REAL,
                pm_down_ask REAL,
                pm_spread REAL,
                book_imbalance REAL,
                regime TEXT,
                moneyness_z REAL DEFAULT 0.0,
                bs_up_prob REAL DEFAULT 0.5,
                sigma_rem_bps REAL DEFAULT 15.0,
                iv_1s_bps REAL DEFAULT 0.0,
                iv_rv_ratio REAL DEFAULT 1.0,
                log_iv_rv REAL DEFAULT 0.0,
                iv_state TEXT DEFAULT 'FAIR',
                cvd_3s_imbalance REAL DEFAULT 0.0,
                perp_basis_jump_bps REAL DEFAULT 0.0,
                orderflow_score REAL DEFAULT 0.0,
                clob_liquidity_pull REAL DEFAULT 0.0,
                energy_accum_score REAL DEFAULT 0.0,
                energy_consistency REAL DEFAULT 0.0
            );
            CREATE INDEX IF NOT EXISTS idx_slices_window ON episode_slices(window_ts, elapsed_sec);

            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                window_ts INTEGER NOT NULL,
                mode TEXT NOT NULL,
                side TEXT NOT NULL,
                arm_id TEXT NOT NULL,
                limit_price REAL NOT NULL,
                fill_price REAL,
                size_usdc REAL NOT NULL,
                shares REAL,
                fee_usdc REAL DEFAULT 0,
                status TEXT NOT NULL,
                model_win_prob REAL,
                market_implied_prob REAL,
                edge REAL,
                net_power_bps REAL,
                elapsed_sec INTEGER,
                regime TEXT,
                official_outcome TEXT,
                pnl_usdc REAL DEFAULT 0,
                created_at REAL NOT NULL,
                filled_at REAL,
                settled_at REAL,
                notes TEXT,
                book_bid REAL,
                book_ask REAL,
                exit_price REAL,
                exit_reason TEXT,
                exit_arm TEXT,
                entry_strategy TEXT,
                bal_before REAL,
                bal_after_entry REAL,
                bal_after_settle REAL,
                recon_status TEXT,
                recon_diff REAL,
                recon_note TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_orders_mode_ts ON orders(mode, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_orders_window ON orders(window_ts);
            CREATE INDEX IF NOT EXISTS idx_orders_mode_status ON orders(mode, status, settled_at);
            CREATE INDEX IF NOT EXISTS idx_rounds_status ON rounds(status);

            CREATE TABLE IF NOT EXISTS equity_curve (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mode TEXT NOT NULL,
                ts REAL NOT NULL,
                window_ts INTEGER,
                balance_usdc REAL NOT NULL,
                cumulative_pnl REAL NOT NULL,
                trade_count INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_equity_mode_ts ON equity_curve(mode, ts);

            CREATE TABLE IF NOT EXISTS model_versions (
                version_id TEXT PRIMARY KEY,
                created_at REAL NOT NULL,
                model_type TEXT NOT NULL,
                role TEXT NOT NULL,
                train_rounds INTEGER,
                val_brier REAL,
                val_auc REAL,
                shadow_rounds INTEGER DEFAULT 0,
                shadow_brier REAL,
                shadow_pnl REAL DEFAULT 0,
                params_json TEXT NOT NULL,
                artifact_path TEXT
            );
        """)
        # Safe backward-compatible column migration for existing SQLite databases
        existing_cols = {row[1] for row in self._conn.execute("PRAGMA table_info(episode_slices)").fetchall()}
        for col_name, col_def in (
            ("moneyness_z", "REAL DEFAULT 0.0"),
            ("bs_up_prob", "REAL DEFAULT 0.5"),
            ("sigma_rem_bps", "REAL DEFAULT 15.0"),
            ("iv_1s_bps", "REAL DEFAULT 0.0"),
            ("iv_rv_ratio", "REAL DEFAULT 1.0"),
            ("log_iv_rv", "REAL DEFAULT 0.0"),
            ("iv_state", "TEXT DEFAULT 'FAIR'"),
            ("cvd_3s_imbalance", "REAL DEFAULT 0.0"),
            ("perp_basis_jump_bps", "REAL DEFAULT 0.0"),
            ("orderflow_score", "REAL DEFAULT 0.0"),
            ("clob_liquidity_pull", "REAL DEFAULT 0.0"),
            ("energy_accum_score", "REAL DEFAULT 0.0"),
            ("energy_consistency", "REAL DEFAULT 0.0"),
        ):
            if col_name not in existing_cols:
                self._conn.execute(f"ALTER TABLE episode_slices ADD COLUMN {col_name} {col_def}")
        order_cols = {row[1] for row in self._conn.execute("PRAGMA table_info(orders)").fetchall()}
        for col_name, col_def in (
            ("book_bid", "REAL"),
            ("book_ask", "REAL"),
            ("exit_price", "REAL"),
            ("exit_reason", "TEXT"),
            ("exit_arm", "TEXT"),
            ("entry_strategy", "TEXT"),
            ("bal_before", "REAL"),
            ("bal_after_entry", "REAL"),
            ("bal_after_settle", "REAL"),
            ("recon_status", "TEXT"),
            ("recon_diff", "REAL"),
            ("recon_note", "TEXT"),
        ):
            if col_name not in order_cols:
                self._conn.execute(f"ALTER TABLE orders ADD COLUMN {col_name} {col_def}")
        self._conn.commit()

    def _init_defaults(self):
        defaults = {
            "auth_password_hash": hash_password(DEFAULT_PASSWORD),
            "is_default_password": "true",
            "trading_mode": "PAPER",
            "trading_paused": "false",
            "paper_initial_balance": "1000.0",
            "paper_balance": "1000.0",
            "risk_config": json.dumps({
                "unit_size_usdc": 10.0,
                "max_daily_loss_usdc": 100.0,
                "consecutive_loss_limit": 3,
            }),
            "wallet_config": json.dumps({
                "signature_type": 2,
                "funder_address": "",
                "wallet_address": "",
                "has_private_key": False,
                "has_api_creds": False,
                "is_observer_only": False,
            }),
            "webhook_config": json.dumps({
                "enabled": False,
                "feishu_url": "",
                "telegram_bot_token": "",
                "telegram_chat_id": "",
            }),
            "net_totals": json.dumps({
                "rx_bytes": 0,
                "tx_bytes": 0,
                "last_os_rx": 0,
                "last_os_tx": 0,
            }),
            "auto_train_config": json.dumps({
                "enabled": True,
                "interval_hours": 12,
                "min_rounds": 16,
            }),
        }
        now = time.time()
        for k, v in defaults.items():
            self._conn.execute(
                "INSERT OR IGNORE INTO kv_store (key, value, updated_at) VALUES (?, ?, ?)",
                (k, v, now),
            )
        cur = self._conn.execute("SELECT COUNT(*) as c FROM equity_curve WHERE mode='PAPER'").fetchone()
        if cur["c"] == 0:
            self._conn.execute(
                "INSERT INTO equity_curve (mode, ts, window_ts, balance_usdc, cumulative_pnl, trade_count) VALUES ('PAPER', ?, 0, 1000.0, 0.0, 0)",
                (now,),
            )
        self._conn.commit()

    def get_trading_mode(self) -> str:
        """Always returns active account mode ('PAPER' or 'LIVE'); auto-migrates legacy 'PAUSED'."""
        raw = str(self.get_kv("trading_mode", "PAPER") or "PAPER").upper()
        if raw == "PAUSED":
            self.set_kv("trading_paused", "true")
            self.set_kv("trading_mode", "PAPER")
            return "PAPER"
        return "LIVE" if raw == "LIVE" else "PAPER"

    def is_trading_paused(self) -> bool:
        """Independent trading pause flag (decoupled from PAPER/LIVE account view)."""
        raw_mode = str(self.get_kv("trading_mode", "PAPER") or "PAPER").upper()
        if raw_mode == "PAUSED":
            self.set_kv("trading_paused", "true")
            self.set_kv("trading_mode", "PAPER")
            return True
        raw_p = str(self.get_kv("trading_paused", "false") or "false").strip().lower()
        return raw_p in ("1", "true", "yes", "on")

    def set_trading_paused(self, paused: bool) -> None:
        self.set_kv("trading_paused", "true" if paused else "false")

    def get_kv(self, key: str, default: Any = None) -> Any:
        with self._lock:
            if key in self._kv_cache:
                val = self._kv_cache[key]
                return default if val is None else val
            row = self._conn.execute("SELECT value FROM kv_store WHERE key=?", (key,)).fetchone()
            val = row["value"] if row else None
            self._kv_cache[key] = val
            return default if val is None else val

    def get_json(self, key: str, default: Any = None) -> Any:
        with self._lock:
            if key in self._json_cache:
                cached = self._json_cache[key]
                if cached is None:
                    return default
                return dict(cached) if isinstance(cached, dict) else cached
            raw = self.get_kv(key)
            if raw is None:
                self._json_cache[key] = None
                return default
            try:
                parsed = json.loads(raw)
                self._json_cache[key] = parsed
                return dict(parsed) if isinstance(parsed, dict) else parsed
            except Exception:
                return default

    def set_kv(self, key: str, value: str) -> None:
        sval = str(value)
        with self._lock:
            self._conn.execute(
                "INSERT INTO kv_store (key, value, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (key, sval, time.time()),
            )
            self._conn.commit()
            self._kv_cache[key] = sval
            self._json_cache.pop(key, None)

    def set_json(self, key: str, obj: Any) -> None:
        raw = json.dumps(obj, ensure_ascii=False)
        with self._lock:
            self.set_kv(key, raw)
            self._json_cache[key] = obj

    def upsert_round(self, data: dict) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO rounds (window_ts, slug, condition_id, up_token_id, down_token_id, price_to_beat, ptb_source, status, created_at)
                VALUES (:window_ts, :slug, :condition_id, :up_token_id, :down_token_id, :price_to_beat, :ptb_source, :status, :created_at)
                ON CONFLICT(window_ts) DO UPDATE SET
                    condition_id=COALESCE(excluded.condition_id, rounds.condition_id),
                    up_token_id=COALESCE(excluded.up_token_id, rounds.up_token_id),
                    down_token_id=COALESCE(excluded.down_token_id, rounds.down_token_id),
                    price_to_beat=COALESCE(excluded.price_to_beat, rounds.price_to_beat),
                    ptb_source=COALESCE(excluded.ptb_source, rounds.ptb_source)
                """,
                {
                    "window_ts": data["window_ts"],
                    "slug": data["slug"],
                    "condition_id": data.get("condition_id"),
                    "up_token_id": data.get("up_token_id"),
                    "down_token_id": data.get("down_token_id"),
                    "price_to_beat": str(data["price_to_beat"]) if data.get("price_to_beat") is not None else None,
                    "ptb_source": data.get("ptb_source", "UNKNOWN"),
                    "status": data.get("status", "ACTIVE"),
                    "created_at": time.time(),
                },
            )
            self._conn.commit()

    def settle_round(self, window_ts: int, settlement_price: str, official_outcome: str, price_to_beat: Optional[str] = None) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE rounds
                SET settlement_price=?, official_outcome=?, status='SETTLED', settled_at=?,
                    price_to_beat=COALESCE(?, price_to_beat)
                WHERE window_ts=?
                """,
                (settlement_price, official_outcome, time.time(), price_to_beat, window_ts),
            )
            self._conn.commit()
            self._slice_counts_cache = None

    def insert_slice(self, s: dict) -> None:
        s.setdefault("moneyness_z", 0.0)
        s.setdefault("bs_up_prob", 0.5)
        s.setdefault("sigma_rem_bps", 15.0)
        s.setdefault("iv_1s_bps", 0.0)
        s.setdefault("iv_rv_ratio", 1.0)
        s.setdefault("log_iv_rv", 0.0)
        s.setdefault("iv_state", "FAIR")
        s.setdefault("cvd_3s_imbalance", 0.0)
        s.setdefault("perp_basis_jump_bps", 0.0)
        s.setdefault("orderflow_score", 0.0)
        s.setdefault("clob_liquidity_pull", 0.0)
        s.setdefault("energy_accum_score", 0.0)
        s.setdefault("energy_consistency", 0.0)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO episode_slices (
                    window_ts, elapsed_sec, ts, spot_price, twap60_price, dist_to_strike_bps,
                    ema13_slope_bps, net_power_bps, bull_power_bps, bear_power_bps, atr_bps,
                    path_efficiency, mfe_bps, mae_bps, locked_twap_ratio,
                    pm_up_bid, pm_up_ask, pm_down_bid, pm_down_ask, pm_spread, book_imbalance, regime,
                    moneyness_z, bs_up_prob, sigma_rem_bps, iv_1s_bps, iv_rv_ratio, log_iv_rv, iv_state,
                    cvd_3s_imbalance, perp_basis_jump_bps, orderflow_score, clob_liquidity_pull,
                    energy_accum_score, energy_consistency
                ) VALUES (
                    :window_ts, :elapsed_sec, :ts, :spot_price, :twap60_price, :dist_to_strike_bps,
                    :ema13_slope_bps, :net_power_bps, :bull_power_bps, :bear_power_bps, :atr_bps,
                    :path_efficiency, :mfe_bps, :mae_bps, :locked_twap_ratio,
                    :pm_up_bid, :pm_up_ask, :pm_down_bid, :pm_down_ask, :pm_spread, :book_imbalance, :regime,
                    :moneyness_z, :bs_up_prob, :sigma_rem_bps, :iv_1s_bps, :iv_rv_ratio, :log_iv_rv, :iv_state,
                    :cvd_3s_imbalance, :perp_basis_jump_bps, :orderflow_score, :clob_liquidity_pull,
                    :energy_accum_score, :energy_consistency
                )
                """,
                s,
            )
            self._conn.commit()
            if self._slice_counts_cache is not None:
                self._slice_counts_cache["total_slices"] += 1

    @staticmethod
    def _ensure_order_defaults(order: dict) -> None:
        order.setdefault("book_bid", None)
        order.setdefault("book_ask", None)
        order.setdefault("exit_price", None)
        order.setdefault("exit_reason", None)
        order.setdefault("exit_arm", None)
        order.setdefault("entry_strategy", None)
        order.setdefault("bal_before", None)
        order.setdefault("bal_after_entry", None)
        order.setdefault("bal_after_settle", None)
        order.setdefault("recon_status", None)
        order.setdefault("recon_diff", None)
        order.setdefault("recon_note", None)

    def save_order(self, order: dict) -> None:
        self._ensure_order_defaults(order)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO orders (
                    order_id, window_ts, mode, side, arm_id, limit_price, fill_price, size_usdc,
                    shares, fee_usdc, status, model_win_prob, market_implied_prob, edge,
                    net_power_bps, elapsed_sec, regime, official_outcome, pnl_usdc,
                    created_at, filled_at, settled_at, notes, book_bid, book_ask,
                    exit_price, exit_reason, exit_arm, entry_strategy,
                    bal_before, bal_after_entry, bal_after_settle, recon_status, recon_diff, recon_note
                ) VALUES (
                    :order_id, :window_ts, :mode, :side, :arm_id, :limit_price, :fill_price, :size_usdc,
                    :shares, :fee_usdc, :status, :model_win_prob, :market_implied_prob, :edge,
                    :net_power_bps, :elapsed_sec, :regime, :official_outcome, :pnl_usdc,
                    :created_at, :filled_at, :settled_at, :notes, :book_bid, :book_ask,
                    :exit_price, :exit_reason, :exit_arm, :entry_strategy,
                    :bal_before, :bal_after_entry, :bal_after_settle, :recon_status, :recon_diff, :recon_note
                )
                ON CONFLICT(order_id) DO UPDATE SET
                    fill_price=excluded.fill_price,
                    size_usdc=excluded.size_usdc,
                    shares=excluded.shares,
                    fee_usdc=excluded.fee_usdc,
                    status=excluded.status,
                    official_outcome=excluded.official_outcome,
                    pnl_usdc=excluded.pnl_usdc,
                    filled_at=excluded.filled_at,
                    settled_at=excluded.settled_at,
                    notes=excluded.notes,
                    exit_price=COALESCE(excluded.exit_price, orders.exit_price),
                    exit_reason=COALESCE(excluded.exit_reason, orders.exit_reason),
                    exit_arm=COALESCE(excluded.exit_arm, orders.exit_arm),
                    entry_strategy=COALESCE(excluded.entry_strategy, orders.entry_strategy),
                    bal_before=COALESCE(excluded.bal_before, orders.bal_before),
                    bal_after_entry=COALESCE(excluded.bal_after_entry, orders.bal_after_entry),
                    bal_after_settle=COALESCE(excluded.bal_after_settle, orders.bal_after_settle),
                    recon_status=COALESCE(excluded.recon_status, orders.recon_status),
                    recon_diff=CASE WHEN excluded.recon_diff IS NOT NULL THEN excluded.recon_diff ELSE orders.recon_diff END,
                    recon_note=COALESCE(excluded.recon_note, orders.recon_note)
                """,
                order,
            )
            self._conn.commit()
            self._stats_cache.pop(str(order.get("mode") or ""), None)
            self._window_orders_cache.pop(int(order["window_ts"]), None)

    def replace_order_id(self, old_order_id: str, new_order: dict) -> None:
        """Atomically swap a temporary pre-flight SUBMITTING placeholder row with the real exchange order_id."""
        self._ensure_order_defaults(new_order)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if old_order_id != new_order["order_id"]:
                    self._conn.execute("DELETE FROM orders WHERE order_id=?", (old_order_id,))
                self._conn.execute(
                    """
                    INSERT INTO orders (
                        order_id, window_ts, mode, side, arm_id, limit_price, fill_price, size_usdc,
                        shares, fee_usdc, status, model_win_prob, market_implied_prob, edge,
                        net_power_bps, elapsed_sec, regime, official_outcome, pnl_usdc,
                        created_at, filled_at, settled_at, notes, book_bid, book_ask,
                        exit_price, exit_reason, exit_arm, entry_strategy,
                        bal_before, bal_after_entry, bal_after_settle, recon_status, recon_diff, recon_note
                    ) VALUES (
                        :order_id, :window_ts, :mode, :side, :arm_id, :limit_price, :fill_price, :size_usdc,
                        :shares, :fee_usdc, :status, :model_win_prob, :market_implied_prob, :edge,
                        :net_power_bps, :elapsed_sec, :regime, :official_outcome, :pnl_usdc,
                        :created_at, :filled_at, :settled_at, :notes, :book_bid, :book_ask,
                        :exit_price, :exit_reason, :exit_arm, :entry_strategy,
                        :bal_before, :bal_after_entry, :bal_after_settle, :recon_status, :recon_diff, :recon_note
                    )
                    ON CONFLICT(order_id) DO UPDATE SET
                        fill_price=excluded.fill_price,
                        size_usdc=excluded.size_usdc,
                        shares=excluded.shares,
                        fee_usdc=excluded.fee_usdc,
                        status=excluded.status,
                        official_outcome=excluded.official_outcome,
                        pnl_usdc=excluded.pnl_usdc,
                        filled_at=excluded.filled_at,
                        settled_at=excluded.settled_at,
                        notes=excluded.notes,
                        bal_before=COALESCE(excluded.bal_before, orders.bal_before),
                        bal_after_entry=COALESCE(excluded.bal_after_entry, orders.bal_after_entry),
                        bal_after_settle=COALESCE(excluded.bal_after_settle, orders.bal_after_settle),
                        recon_status=COALESCE(excluded.recon_status, orders.recon_status),
                        recon_diff=CASE WHEN excluded.recon_diff IS NOT NULL THEN excluded.recon_diff ELSE orders.recon_diff END,
                        recon_note=COALESCE(excluded.recon_note, orders.recon_note)
                    """,
                    new_order,
                )
                self._conn.commit()
                self._stats_cache.pop(str(new_order.get("mode") or ""), None)
                self._window_orders_cache.pop(int(new_order["window_ts"]), None)
            except Exception:
                try:
                    self._conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    def get_order(self, order_id: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()
            return dict(row) if row else None

    def get_orders(self, mode: Optional[str] = None, limit: int = 100) -> list[dict]:
        with self._lock:
            if mode and mode != "ALL":
                rows = self._conn.execute(
                    "SELECT * FROM orders WHERE mode=? ORDER BY created_at DESC LIMIT ?", (mode, limit)
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM orders WHERE mode != 'SHADOW' ORDER BY created_at DESC LIMIT ?", (limit,)
                ).fetchall()
            return [dict(r) for r in rows]

    def try_insert_order_atomic(self, order: dict) -> bool:
        """
        Cross-process atomic single-primary-order guard using SQLite BEGIN IMMEDIATE:
        Guarantees at most ONE active primary order (or ONE arb-lock hedge) per (window_ts, mode)
        even if multiple coroutines or multiple OS processes hit the same SQLite database simultaneously.
        Returns True if inserted, False if an active order already exists for that slot.
        """
        self._ensure_order_defaults(order)

        wts = int(order["window_ts"])
        mode = str(order["mode"])
        is_arb = str(order.get("arm_id") or "") in ("ARB_LOCK_MAKER", "ARB_LOCK_TAKER")

        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                if is_arb:
                    exists = self._conn.execute(
                        """
                        SELECT 1 FROM orders
                        WHERE window_ts=? AND mode=? AND arm_id IN ('ARB_LOCK_MAKER', 'ARB_LOCK_TAKER')
                          AND status IN ('SUBMITTING', 'CONFIRMING', 'PENDING', 'FILLED', 'SETTLED', 'CLOSED')
                        LIMIT 1
                        """,
                        (wts, mode),
                    ).fetchone()
                else:
                    exists = self._conn.execute(
                        """
                        SELECT 1 FROM orders
                        WHERE window_ts=? AND mode=? AND arm_id NOT IN ('ARB_LOCK_MAKER', 'ARB_LOCK_TAKER')
                          AND status IN ('SUBMITTING', 'CONFIRMING', 'PENDING', 'FILLED', 'SETTLED', 'CLOSED')
                        LIMIT 1
                        """,
                        (wts, mode),
                    ).fetchone()
                if exists is not None:
                    self._conn.execute("ROLLBACK")
                    return False

                self._conn.execute(
                    """
                    INSERT INTO orders (
                        order_id, window_ts, mode, side, arm_id, limit_price, fill_price, size_usdc,
                        shares, fee_usdc, status, model_win_prob, market_implied_prob, edge,
                        net_power_bps, elapsed_sec, regime, official_outcome, pnl_usdc,
                        created_at, filled_at, settled_at, notes, book_bid, book_ask,
                        exit_price, exit_reason, exit_arm, entry_strategy,
                        bal_before, bal_after_entry, bal_after_settle, recon_status, recon_diff, recon_note
                    ) VALUES (
                        :order_id, :window_ts, :mode, :side, :arm_id, :limit_price, :fill_price, :size_usdc,
                        :shares, :fee_usdc, :status, :model_win_prob, :market_implied_prob, :edge,
                        :net_power_bps, :elapsed_sec, :regime, :official_outcome, :pnl_usdc,
                        :created_at, :filled_at, :settled_at, :notes, :book_bid, :book_ask,
                        :exit_price, :exit_reason, :exit_arm, :entry_strategy,
                        :bal_before, :bal_after_entry, :bal_after_settle, :recon_status, :recon_diff, :recon_note
                    )
                    """,
                    order,
                )
                self._conn.commit()
                self._stats_cache.pop(mode, None)
                self._window_orders_cache.pop(wts, None)
                return True
            except Exception:
                try:
                    self._conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    def get_orders_by_window(self, window_ts: int) -> list[dict]:
        """Always read ground-truth SQLite (indexed <0.05ms) to prevent any cross-process stale cache."""
        wts = int(window_ts)
        with self._lock:
            rows = self._conn.execute("SELECT * FROM orders WHERE window_ts=?", (wts,)).fetchall()
            return [dict(r) for r in rows]

    def record_equity(self, mode: str, window_ts: int, balance_usdc: float, cumulative_pnl: float, trade_count: int) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO equity_curve (mode, ts, window_ts, balance_usdc, cumulative_pnl, trade_count) VALUES (?, ?, ?, ?, ?, ?)",
                (mode, time.time(), window_ts, balance_usdc, cumulative_pnl, trade_count),
            )
            self._conn.commit()

    def get_equity_curve(self, mode: str = "PAPER", limit: int = 500) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT ts, window_ts, balance_usdc, cumulative_pnl, trade_count FROM equity_curve WHERE mode=? ORDER BY id DESC LIMIT ?",
                (mode, limit),
            ).fetchall()
            return [dict(r) for r in reversed(rows)]

    def get_performance_stats(self, mode: str = "PAPER") -> dict:
        now = time.time()
        with self._lock:
            cached = self._stats_cache.get(mode)
            if cached is not None and (now - cached[0]) < 30.0:
                return dict(cached[1])
            rows = self._conn.execute(
                "SELECT pnl_usdc, fee_usdc, size_usdc, status FROM orders WHERE mode=? AND status IN ('SETTLED', 'CLOSED') ORDER BY settled_at ASC",
                (mode,),
            ).fetchall()
            total = len(rows)
            wins = sum(1 for r in rows if (r["pnl_usdc"] or 0) > 0)
            losses = sum(1 for r in rows if (r["pnl_usdc"] or 0) < 0)
            gross_profit = sum(r["pnl_usdc"] for r in rows if (r["pnl_usdc"] or 0) > 0)
            gross_loss = abs(sum(r["pnl_usdc"] for r in rows if (r["pnl_usdc"] or 0) < 0))
            total_pnl = sum(r["pnl_usdc"] or 0 for r in rows)
            total_fees = sum(r["fee_usdc"] or 0 for r in rows)

            peak = 0.0
            cur = 0.0
            max_dd = 0.0
            for r in rows:
                cur += r["pnl_usdc"] or 0
                if cur > peak:
                    peak = cur
                dd = peak - cur
                if dd > max_dd:
                    max_dd = dd

            day_start = time.time() - 86400
            day_row = self._conn.execute(
                "SELECT COALESCE(SUM(pnl_usdc), 0) as dp FROM orders WHERE mode=? AND status IN ('SETTLED', 'CLOSED') AND settled_at >= ?",
                (mode, day_start),
            ).fetchone()

            res = {
                "mode": mode,
                "total_trades": total,
                "wins": wins,
                "losses": losses,
                "win_rate": round(wins / total * 100, 2) if total else 0.0,
                "total_pnl": round(total_pnl, 4),
                "daily_pnl": round(float(day_row["dp"]), 4) if day_row else 0.0,
                "total_fees": round(total_fees, 4),
                "profit_factor": round(gross_profit / gross_loss, 2) if gross_loss > 0 else (99.0 if gross_profit > 0 else 0.0),
                "max_drawdown": round(max_dd, 4),
                "early_exits": sum(1 for r in rows if r["status"] == "CLOSED"),
            }
            self._stats_cache[mode] = (now, res)
            return dict(res)

    def get_account_audit_summary(self, mode: str = "PAPER") -> dict:
        """
        Strict per-mode independent ledger & fund reconciliation summary (PAPER / LIVE / SHADOW).
        Never mixes PAPER and LIVE orders or balances!
        """
        m = str(mode or "PAPER").upper()
        if m not in ("PAPER", "LIVE", "SHADOW"):
            m = "PAPER"
        stats = self.get_performance_stats(m)
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, recon_status, recon_diff, size_usdc, fee_usdc, pnl_usdc FROM orders WHERE mode=?",
                (m,),
            ).fetchall()

        status_counts: dict[str, int] = {}
        recon_counts = {"OK": 0, "WARN": 0, "MISMATCH": 0, "PENDING": 0}
        active_cost_sum = 0.0
        for r in rows:
            st = str(r["status"] or "")
            status_counts[st] = status_counts.get(st, 0) + 1
            rs = str(r["recon_status"] or "")
            if rs in recon_counts:
                recon_counts[rs] += 1
            if st in ("FILLED", "CONFIRMING", "PENDING", "SUBMITTING"):
                active_cost_sum += float(r["size_usdc"] or 0.0) + float(r["fee_usdc"] or 0.0)

        out = {
            "mode": m,
            "stats": stats,
            "total_orders": len(rows),
            "filled_active": status_counts.get("FILLED", 0),
            "confirming": status_counts.get("CONFIRMING", 0) + status_counts.get("SUBMITTING", 0),
            "pending": status_counts.get("PENDING", 0),
            "cancelled": status_counts.get("CANCELLED", 0),
            "failed": status_counts.get("FAILED", 0) + status_counts.get("REJECTED", 0),
            "active_cost_usdc": round(active_cost_sum, 4),
            "recon_ok": recon_counts["OK"],
            "recon_warn": recon_counts["WARN"],
            "recon_mismatch": recon_counts["MISMATCH"],
            "recon_pending": recon_counts["PENDING"],
        }

        if m == "PAPER":
            init_bal = float(self.get_kv("paper_initial_balance", "1000.0") or 1000.0)
            cur_bal = float(self.get_kv("paper_balance", "1000.0") or 1000.0)
            expected_bal = round(init_bal + float(stats["total_pnl"]), 4)
            diff = round(cur_bal - expected_bal, 4)
            out.update({
                "initial_balance": round(init_bal, 4),
                "current_balance": round(cur_bal, 4),
                "expected_balance": expected_bal,
                "ledger_diff": diff,
                "ledger_ok": abs(diff) <= 0.02,
            })
        return out

    def get_round(self, window_ts: int) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM rounds WHERE window_ts=?", (window_ts,)).fetchone()
            return dict(row) if row else None

    def get_unsettled_past_windows(self, current_window_ts: int, limit: int = 30) -> list[int]:
        """Find past 5m windows that still have PENDING/FILLED orders or ACTIVE rounds (survives restarts)."""
        with self._lock:
            order_rows = self._conn.execute(
                """
                SELECT DISTINCT window_ts FROM orders
                WHERE window_ts < ? AND status IN ('PENDING', 'CONFIRMING', 'FILLED')
                ORDER BY window_ts ASC LIMIT ?
                """,
                (current_window_ts, limit),
            ).fetchall()
            round_rows = self._conn.execute(
                """
                SELECT window_ts FROM rounds
                WHERE window_ts < ? AND status != 'SETTLED'
                ORDER BY window_ts DESC LIMIT ?
                """,
                (current_window_ts, min(limit, 6)),
            ).fetchall()
            merged = sorted({int(r["window_ts"]) for r in order_rows} | {int(r["window_ts"]) for r in round_rows})
            return merged[:limit]

    def get_recent_settled_rounds(self, limit: int = 20) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM rounds WHERE status='SETTLED' ORDER BY window_ts DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_slices_by_window(self, window_ts: int) -> list[dict]:
        """Fetch ordered 5s episode slices for a single 5m round (for counterfactual exit-arm evaluation)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM episode_slices WHERE window_ts=? ORDER BY elapsed_sec ASC",
                (window_ts,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_training_dataset(self, max_rounds: int = 864) -> list[dict]:
        """Fetch settled rounds joined with their full 300s episode slices ordered chronologically."""
        with self._lock:
            rounds = self._conn.execute(
                "SELECT window_ts, official_outcome FROM rounds WHERE status='SETTLED' AND official_outcome IN ('UP', 'DOWN') ORDER BY window_ts DESC LIMIT ?",
                (max_rounds,),
            ).fetchall()
            if not rounds:
                return []
            round_map = {r["window_ts"]: (1 if r["official_outcome"] == "UP" else 0) for r in rounds}
            min_w = min(round_map.keys())
            slices = self._conn.execute(
                "SELECT * FROM episode_slices WHERE window_ts >= ? ORDER BY window_ts ASC, elapsed_sec ASC",
                (min_w,),
            ).fetchall()
            out = []
            for s in slices:
                w = s["window_ts"]
                if w in round_map:
                    d = dict(s)
                    d["label"] = round_map[w]
                    out.append(d)
            return out

    def get_slice_counts(self) -> dict:
        with self._lock:
            if self._slice_counts_cache is not None:
                return dict(self._slice_counts_cache)
            r_cnt = self._conn.execute("SELECT COUNT(*) as c FROM rounds WHERE status='SETTLED'").fetchone()["c"]
            s_cnt = self._conn.execute("SELECT COUNT(*) as c FROM episode_slices").fetchone()["c"]
            self._slice_counts_cache = {"settled_rounds": r_cnt, "total_slices": s_cnt}
            return dict(self._slice_counts_cache)

    def save_model_version(self, m: dict) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO model_versions (
                    version_id, created_at, model_type, role, train_rounds, val_brier, val_auc,
                    shadow_rounds, shadow_brier, shadow_pnl, params_json, artifact_path
                ) VALUES (
                    :version_id, :created_at, :model_type, :role, :train_rounds, :val_brier, :val_auc,
                    :shadow_rounds, :shadow_brier, :shadow_pnl, :params_json, :artifact_path
                )
                ON CONFLICT(version_id) DO UPDATE SET
                    role=excluded.role,
                    shadow_rounds=excluded.shadow_rounds,
                    shadow_brier=excluded.shadow_brier,
                    shadow_pnl=excluded.shadow_pnl
                """,
                m,
            )
            self._conn.commit()

    def list_model_versions(self, limit: int = 15) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM model_versions ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def set_champion_version(self, version_id: str) -> bool:
        with self._lock:
            exists = self._conn.execute("SELECT 1 FROM model_versions WHERE version_id=?", (version_id,)).fetchone()
            if not exists:
                return False
            self._conn.execute("UPDATE model_versions SET role='ARCHIVED' WHERE role='CHAMPION'")
            self._conn.execute("UPDATE model_versions SET role='CHAMPION' WHERE version_id=?", (version_id,))
            self._conn.commit()
            return True

    def cleanup_old_data(self, retain_days: int = 7) -> int:
        cutoff = time.time() - retain_days * 86400
        with self._lock:
            cur = self._conn.execute("DELETE FROM episode_slices WHERE ts < ?", (cutoff,))
            self._conn.commit()
            self._slice_counts_cache = None
            return cur.rowcount

    def reset_dataset(self, clear_models: bool = True) -> dict:
        """Clear recorded 300s episode slices & rounds (and optionally offline model versions)."""
        with self._lock:
            slices_del = self._conn.execute("DELETE FROM episode_slices").rowcount
            rounds_del = self._conn.execute("DELETE FROM rounds").rowcount
            models_del = 0
            if clear_models:
                models_del = self._conn.execute("DELETE FROM model_versions").rowcount
            self._conn.commit()
            try:
                self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            except Exception:
                pass
            self._slice_counts_cache = {"settled_rounds": 0, "total_slices": 0}
        return {
            "deleted_slices": slices_del,
            "deleted_rounds": rounds_del,
            "deleted_models": models_del,
        }

    def reset_paper_account(self, initial_balance: float = 1000.0) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM orders WHERE mode='PAPER'")
            self._conn.execute("DELETE FROM equity_curve WHERE mode='PAPER'")
            self._conn.execute(
                "INSERT INTO equity_curve (mode, ts, window_ts, balance_usdc, cumulative_pnl, trade_count) VALUES ('PAPER', ?, 0, ?, 0.0, 0)",
                (time.time(), initial_balance),
            )
            self._conn.commit()
        with self._lock:
            self._stats_cache.pop("PAPER", None)
            self._window_orders_cache.clear()
        self.set_kv("paper_initial_balance", str(initial_balance))
        self.set_kv("paper_balance", str(initial_balance))
