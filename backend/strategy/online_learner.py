import math
import random
from typing import Optional

from backend.config import HARD_LIMITS
from backend.storage.db import Database

FEATURE_NAMES = [
    "bias",
    "bs_moneyness_logit",      # logit(Phi(Z)): distance-to-strike scaled by remaining-time TWAP vol
    "net_power_time_scaled",   # Quant Elder Net Power weighted by remaining runway
    "bull_bear_asym",          # (bull + bear) / (|bull| + |bear| + 1)
    "ema_slope_norm",          # ema13_slope_bps / 3.0
    "path_eff_signed",         # path_efficiency * sign(eff_dist_bps)
    "time_decay_urgency",      # (1 - remaining_sec/300) * tanh(moneyness_z)
    "book_imbalance_centered", # Polymarket orderbook depth imbalance
    "iv_rv_mismatch_signed",   # log(rel_IV/RV) * tanh(moneyness_z): Binary Vega mismatch
    "orderflow_lead_score",    # Binance Perp 3s/10s CVD + Perp-Spot Basis Jump + CLOB Liquidity Pull
    "energy_accum_score",      # Sustained decaying momentum area x directional consistency x anti-lag progress
]


class SelfLearningCoordinator:
    """
    Autonomous Self-Evolving Quant Decision Engine (v18):
    1. 10-feature Online Calibrator anchored on Binary Option Moneyness + Leading Perp Orderflow + IV/RV Vega.
    2. Fully Autonomous Strategy Parameters (`auto_params`): min_edge, max_entry_price, cooldown_sec,
       arb_max_combined_price, prop_tp_ratio evolve continuously within strict hard safety bounds.
    3. 4 Mutually-Exclusive Counterfactual Exit Strategy Arms (`exit_arms`):
       - HOLD_EXPIRY:    Hold to official T=300s settlement ($0 exit fee).
       - MAKER_FAIR_TP:  0%-fee Post-Only Maker limit SELL when market overpays model fair prob.
       - PROP_TP_45:     Proportional upside 0%-fee Maker limit SELL + strict collapse Stop-Loss (only if bid <= 0.45).
       - ARB_LOCK_AUTO:  Equal-share opposite Maker hedge when combined <= arb_max_combined_price.
       Updated via BOTH real Exit Alpha AND counterfactual replay on post-fill 5s episode slices!
    4. Sub-Strategy Auto-Hibernation & Shadow Recovery (`sub_strategies`):
       `orderflow_lead` & `twap_sniper` automatically hibernate to SHADOW on sustained losses and
       auto-wakeup to ACTIVE once shadow replay proves positive ROI again.
    """

    def __init__(self, db: Database):
        self.db = db
        self.offline_model = None
        self.active_version_id = "online_baseline_v24"
        self._load_state()

    def _default_state(self) -> dict:
        return {
            "weights": [0.0, 0.90, 0.30, 0.16, 0.14, 0.22, 0.20, 0.28, 0.15, 0.38, 0.22],
            "schema_version": 13,
            "learning_rate": 0.02,
            "l2_decay": 0.002,
            "updates_count": 0,
            "recent_brier": [0.20] * 10,
            "time_buckets": {
                "0_60":    {"net_power_min_bps": 2.4, "wins": 0, "losses": 0, "ema_ev": 0.0, "enabled": True},
                "60_180":  {"net_power_min_bps": 1.8, "wins": 0, "losses": 0, "ema_ev": 0.0, "enabled": True},
                "180_255": {"net_power_min_bps": 2.0, "wins": 0, "losses": 0, "ema_ev": 0.0, "enabled": True},
                "255_300": {"net_power_min_bps": 3.5, "wins": 0, "losses": 0, "ema_ev": 0.0, "enabled": True},
            },
            "arms": [
                {"id": "MAKER_BID_0_TTL6S", "offset_cents": 0.00, "ttl_ms": 6000, "is_taker": False, "count": 2.0, "mean_ev": 0.008, "m2": 0.02},
                {"id": "MAKER_BID_1_TTL4S", "offset_cents": 0.01, "ttl_ms": 5000, "is_taker": False, "count": 2.0, "mean_ev": 0.012, "m2": 0.02},
                {"id": "MAKER_MID_TTL3S",   "offset_cents": 0.02, "ttl_ms": 4000, "is_taker": False, "count": 2.0, "mean_ev": 0.012, "m2": 0.02},
                {"id": "TAKER_FAST",        "offset_cents": 0.00, "ttl_ms": 1500, "is_taker": True,  "count": 2.0, "mean_ev": 0.005, "m2": 0.02},
            ],
            "edge_curve": {
                "low_anchor": 0.45,
                "high_anchor": 0.60,
                "low_slope": 0.22,
                "high_slope": 0.12,
                "mid_offset": 0.0,
                "low_size_mult": 0.60,
                "zones": {
                    "low":  {"wins": 0, "losses": 0, "samples": 0, "ema_roi": 0.0, "ema_calib_bias": 0.0},
                    "mid":  {"wins": 0, "losses": 0, "samples": 0, "ema_roi": 0.0, "ema_calib_bias": 0.0},
                    "high": {"wins": 0, "losses": 0, "samples": 0, "ema_roi": 0.0, "ema_calib_bias": 0.0},
                },
            },
            # v18 Autonomous Evolved Strategy Parameters (no manual tuning needed!)
            "auto_params": {
                "min_edge": 0.042,              # bounds [0.035, 0.085]
                "max_entry_price": 0.75,        # bounds [0.65, 0.78]
                "cooldown_sec": 15,             # bounds [10, 30]
                "arb_max_combined_price": 0.95, # bounds [0.92, 0.96]
                "prop_tp_ratio": 0.48,          # bounds [0.35, 0.65] of (1 - entry_px)
                "sl_floor_bid": 0.45,           # hard ceiling: NEVER stop-loss when bid > 0.45
                "sl_drop_cents": 0.20,          # bounds [0.16, 0.28]
            },
            # v18 4 Mutually-Exclusive Counterfactual Exit Strategy Arms
            "exit_arms": [
                {"id": "HOLD_EXPIRY",   "count": 3.0, "mean_ev": 0.015, "m2": 0.03, "alpha_ema": 0.0, "wins": 0, "losses": 0},
                {"id": "MAKER_FAIR_TP", "count": 3.0, "mean_ev": 0.018, "m2": 0.03, "alpha_ema": 0.0, "wins": 0, "losses": 0},
                {"id": "PROP_TP_45",    "count": 3.0, "mean_ev": 0.014, "m2": 0.03, "alpha_ema": 0.0, "wins": 0, "losses": 0},
                {"id": "ARB_LOCK_AUTO", "count": 3.0, "mean_ev": 0.012, "m2": 0.03, "alpha_ema": 0.0, "wins": 0, "losses": 0},
            ],
            # v18 Entry Sub-Strategy Autonomous Shadow Self-Gating
            "sub_strategies": {
                "orderflow_lead":    {"status": "ACTIVE", "wins": 0, "losses": 0, "samples": 0, "ema_roi": 0.0, "shadow_samples": 0, "shadow_ema_roi": 0.0},
                "momentum_breakout": {"status": "ACTIVE", "wins": 0, "losses": 0, "samples": 0, "ema_roi": 0.0, "shadow_samples": 0, "shadow_ema_roi": 0.0},
                "energy_accum":      {"status": "ACTIVE", "wins": 0, "losses": 0, "samples": 0, "ema_roi": 0.0, "shadow_samples": 0, "shadow_ema_roi": 0.0},
                "twap_sniper":       {"status": "ACTIVE", "wins": 0, "losses": 0, "samples": 0, "ema_roi": 0.0, "shadow_samples": 0, "shadow_ema_roi": 0.0},
            },
            "ema_period": 13,
            "hl_window": 5,
            "atr_min_bps": 0.30,
        }

    def _load_state(self):
        saved = self.db.get_json("self_learning_state")
        def_st = self._default_state()
        if saved and "weights" in saved and "arms" in saved:
            old_v = int(saved.get("schema_version", 0))
            if old_v in (11, 12) and len(saved["weights"]) == 10:
                # Seamless v14-v18 -> v19 migration: preserve all 10 learned weights & append 11th energy_accum_score weight
                saved["weights"].append(0.22)
            elif old_v == 10 and len(saved["weights"]) == 9:
                saved["weights"].extend([0.38, 0.22])
            elif old_v == 9 and len(saved["weights"]) == 8:
                saved["weights"].extend([0.15, 0.38, 0.22])
            elif old_v < 9 or len(saved["weights"]) != len(FEATURE_NAMES):
                saved["weights"] = list(def_st["weights"])
            saved["schema_version"] = 13

            # Migrate legacy execution arms
            valid_arm_ids = {"MAKER_BID_0_TTL6S", "MAKER_BID_1_TTL4S", "MAKER_MID_TTL3S", "TAKER_FAST"}
            cur_arms = saved.get("arms") or []
            cur_ids = {a.get("id") for a in cur_arms if isinstance(a, dict)}
            if cur_ids != valid_arm_ids or any((not a.get("is_taker")) and int(a.get("ttl_ms", 0)) < 3500 for a in cur_arms):
                saved["arms"] = list(def_st["arms"])

            # Migrate / sanitize edge_curve
            ec = saved.get("edge_curve")
            if not isinstance(ec, dict) or "zones" not in ec:
                saved["edge_curve"] = def_st["edge_curve"]
            else:
                ec["low_anchor"] = 0.45
                ec["high_anchor"] = 0.60
                ec["low_slope"] = round(max(0.10, min(0.45, float(ec.get("low_slope", 0.22)))), 4)
                ec["high_slope"] = round(max(0.04, min(0.30, float(ec.get("high_slope", 0.12)))), 4)
                ec["mid_offset"] = round(max(-0.005, min(0.025, float(ec.get("mid_offset", 0.0)))), 4)
                ec["low_size_mult"] = round(max(0.35, min(0.90, float(ec.get("low_size_mult", 0.60)))), 3)
                for zk in ("low", "mid", "high"):
                    if zk not in ec["zones"] or not isinstance(ec["zones"][zk], dict):
                        ec["zones"][zk] = dict(def_st["edge_curve"]["zones"][zk])

            # Migrate / sanitize v18 auto_params
            ap = saved.get("auto_params")
            if not isinstance(ap, dict):
                saved["auto_params"] = dict(def_st["auto_params"])
            else:
                for k, v in def_st["auto_params"].items():
                    ap.setdefault(k, v)
                ap["min_edge"] = round(max(HARD_LIMITS["min_edge_prob"], min(0.085, float(ap["min_edge"]))), 4)
                ap["max_entry_price"] = round(max(0.72, min(HARD_LIMITS["max_entry_price"], float(ap["max_entry_price"]))), 2)
                ap["cooldown_sec"] = max(10, min(30, int(ap["cooldown_sec"])))
                ap["arb_max_combined_price"] = round(max(0.92, min(0.96, float(ap["arb_max_combined_price"]))), 2)
                ap["prop_tp_ratio"] = round(max(0.35, min(0.65, float(ap["prop_tp_ratio"]))), 3)
                ap["sl_floor_bid"] = 0.45
                ap["sl_drop_cents"] = round(max(0.16, min(0.28, float(ap["sl_drop_cents"]))), 3)

            # Migrate / sanitize v18 exit_arms
            valid_exit_ids = {"HOLD_EXPIRY", "MAKER_FAIR_TP", "PROP_TP_45", "ARB_LOCK_AUTO"}
            cur_ex = saved.get("exit_arms") or []
            if {a.get("id") for a in cur_ex if isinstance(a, dict)} != valid_exit_ids:
                saved["exit_arms"] = list(def_st["exit_arms"])

            # Migrate / sanitize v18 sub_strategies
            subs = saved.get("sub_strategies")
            if not isinstance(subs, dict):
                saved["sub_strategies"] = dict(def_st["sub_strategies"])
            else:
                for sk, sv in def_st["sub_strategies"].items():
                    if sk not in subs or not isinstance(subs[sk], dict):
                        subs[sk] = dict(sv)
                    else:
                        for fk, fv in sv.items():
                            subs[sk].setdefault(fk, fv)
                # Base momentum strategy always remains ACTIVE (its threshold self-tightens instead)
                subs["momentum_breakout"]["status"] = "ACTIVE"

            # v20/v24 migration: relax legacy dead-stop ATR floor & un-clamp any locked-out time_bucket thresholds
            if float(saved.get("atr_min_bps", 0.6)) >= 0.55:
                saved["atr_min_bps"] = 0.30
            tbs = saved.get("time_buckets")
            if isinstance(tbs, dict):
                for _bk, _bv in tbs.items():
                    if isinstance(_bv, dict) and "net_power_min_bps" in _bv:
                        _bv["net_power_min_bps"] = round(max(1.2, min(4.5, float(_bv["net_power_min_bps"]))), 3)
            saved["weights"][0] = max(-0.35, min(0.35, float(saved["weights"][0])))
            saved["weights"][1] = max(0.35, min(1.50, float(saved["weights"][1])))
            self.state = saved
            self._save_state()
        else:
            self.state = def_st
            self._save_state()

    def _save_state(self):
        self.db.set_json("self_learning_state", self.state)

    def reset_online_state(self):
        self.state = self._default_state()
        self._save_state()

    def get_auto_params(self) -> dict:
        return self.state.get("auto_params") or self._default_state()["auto_params"]

    def get_sub_strategy_status(self, key: str) -> str:
        subs = self.state.get("sub_strategies") or {}
        st = subs.get(key) or {}
        return str(st.get("status", "ACTIVE"))

    def get_best_exit_arm_id(self) -> str:
        arms = self.state.get("exit_arms") or []
        if not arms:
            return "HOLD_EXPIRY"
        return max(arms, key=lambda a: float(a.get("mean_ev", 0.0)))["id"]

    @staticmethod
    def time_bucket_for(elapsed_sec: int) -> str:
        if elapsed_sec < 60:
            return "0_60"
        if elapsed_sec < 180:
            return "60_180"
        if elapsed_sec < 255:
            return "180_255"
        return "255_300"

    @staticmethod
    def _logit(p: float) -> float:
        p = max(0.01, min(0.99, p))
        return math.log(p / (1.0 - p))

    @staticmethod
    def _sigmoid(z: float) -> float:
        z = max(-18.0, min(18.0, z))
        return 1.0 / (1.0 + math.exp(-z))

    @classmethod
    def build_feature_vector(cls, snap: dict, book_imbalance: float) -> list[float]:
        bs_p = float(snap.get("bs_up_prob", 0.5))
        bs_logit = max(-4.5, min(4.5, cls._logit(bs_p)))

        z_val = float(snap.get("moneyness_z", 0.0))
        eff_dist = float(snap.get("eff_dist_bps", snap.get("dist_to_strike_bps", 0.0)))
        rem_sec = float(snap.get("remaining_sec", 150.0))
        locked = float(snap.get("locked_twap_ratio", 0.0))

        net_p = float(snap.get("net_power_bps", 0.0))
        bull = float(snap.get("bull_power_bps", 0.0))
        bear = float(snap.get("bear_power_bps", 0.0))
        slope = float(snap.get("ema13_slope_bps", 0.0))
        er = float(snap.get("path_efficiency", 0.5))
        log_iv_rv = float(snap.get("log_iv_rv", 0.0))
        of_score = float(snap.get("orderflow_score", 0.0))
        energy_score = float(snap.get("energy_accum_score", 0.0))

        runway_scale = max(0.15, (1.0 - 0.80 * locked) * math.sqrt(max(15.0, rem_sec) / 150.0))
        net_power_time_scaled = max(-3.5, min(3.5, (net_p / 8.0) * runway_scale))
        of_time_scaled = max(-2.5, min(2.5, (of_score / 1.5) * runway_scale))
        energy_time_scaled = max(-2.5, min(2.5, energy_score * runway_scale))

        sign_dist = 1.0 if eff_dist > 0 else (-1.0 if eff_dist < 0 else 0.0)
        asym = (bull + bear) / (abs(bull) + abs(bear) + 1.0)
        time_decay_urgency = (1.0 - rem_sec / 300.0) * math.tanh(z_val)
        iv_rv_mismatch_signed = max(-1.5, min(1.5, log_iv_rv * math.tanh(z_val)))

        return [
            1.0,
            bs_logit,
            net_power_time_scaled,
            max(-1.0, min(1.0, asym)),
            max(-3.5, min(3.5, slope / 3.0)),
            er * sign_dist,
            max(-1.0, min(1.0, time_decay_urgency)),
            max(-0.5, min(0.5, book_imbalance - 0.5)),
            iv_rv_mismatch_signed,
            of_time_scaled,
            energy_time_scaled,
        ]

    def predict_up_prob(
        self,
        snap: dict,
        book_imbalance: float,
        elapsed_sec: int = 150,
        pm_spread: float = 0.02,
    ) -> tuple[float, list[float]]:
        feats = self.build_feature_vector(snap, book_imbalance)
        w = self.state["weights"]
        z = sum(x * wi for x, wi in zip(feats, w))
        online_p = self._sigmoid(z)

        if self.offline_model is not None:
            try:
                off_p = float(self.offline_model.predict_prob_from_snap(snap, book_imbalance, elapsed_sec=elapsed_sec, pm_spread=pm_spread))
                prob = 0.60 * off_p + 0.40 * online_p
            except Exception:
                prob = online_p
        else:
            prob = online_p

        return round(max(0.015, min(0.985, prob)), 4), feats

    def get_rolling_brier(self) -> float:
        arr = self.state.get("recent_brier") or [0.20]
        return round(sum(arr) / len(arr), 4)

    def select_exit_arm(
        self,
        entry_strategy: str,
        regime: str,
        elapsed_sec: int,
        allow_arb_lock: bool = True,
    ) -> str:
        """
        Gaussian Thompson Sampling over the 4 mutually-exclusive Exit Strategy Arms.
        - TWAP Sniper always uses HOLD_EXPIRY (designed to collect official T=300s payout).
        - Late entries (>= 230s) prefer HOLD_EXPIRY since little time remains for intraday exit.
        - When `allow_arb_lock=False` (e.g. LIVE with arb-lock switch OFF), excludes ARB_LOCK_AUTO
          so the round is assigned an active TP/Hold exit arm instead of wasting exit opportunities.
        """
        if entry_strategy == "twap_sniper" or elapsed_sec >= 230:
            return "HOLD_EXPIRY"

        arms = self.state.get("exit_arms") or self._default_state()["exit_arms"]
        best_id = "HOLD_EXPIRY"
        best_sample = -1e9
        for arm in arms:
            aid = arm["id"]
            if not allow_arb_lock and aid == "ARB_LOCK_AUTO":
                continue
            cnt = max(1.0, float(arm.get("count", 2.0)))
            var = max(0.001, float(arm.get("m2", 0.03)) / cnt)
            std = math.sqrt(var / cnt)
            # Gentle contextual prior nudge: choppy regimes favor Maker TP / Arb Lock, strong trends favor Hold/FairTP
            prior_nudge = 0.0
            if regime == "CHOPPY" and aid in ("MAKER_FAIR_TP", "PROP_TP_45", "ARB_LOCK_AUTO"):
                prior_nudge = 0.004
            elif regime == "TRENDING" and aid in ("HOLD_EXPIRY", "MAKER_FAIR_TP"):
                prior_nudge = 0.004
            elif entry_strategy == "orderflow_lead" and aid in ("MAKER_FAIR_TP", "PROP_TP_45"):
                prior_nudge = 0.004
            elif entry_strategy == "energy_accum" and aid in ("HOLD_EXPIRY", "MAKER_FAIR_TP"):
                prior_nudge = 0.004

            sample = random.gauss(float(arm.get("mean_ev", 0.0)) + prior_nudge, std)
            if sample > best_sample:
                best_sample = sample
                best_id = aid
        return best_id

    def evaluate_twap_sniper(
        self,
        snap: dict,
        elapsed_sec: int,
        up_book: dict,
        dn_book: dict,
        risk_cfg: dict | None = None,
        taker_fee_rate_override: Optional[float] = None,
    ) -> Optional[dict]:
        """
        Final-60s TWAP Rule Mispricing Sniper (autonomously governed by sub_strategies['twap_sniper']).
        Returns candidate dict with `shadow_only` flag if hibernated in SHADOW mode for self-recovery.
        """
        if elapsed_sec < 252 or elapsed_sec > 290:
            return None
        if snap.get("regime") in ("WARMING_UP", "EXTREME_SPIKE"):
            return None
        if int(snap.get("final_60s_count", 0)) < 12:
            return None

        ap = self.get_auto_params()
        shadow_only = (self.get_sub_strategy_status("twap_sniper") == "SHADOW")

        bs_up = float(snap.get("bs_up_prob", 0.5))
        bs_dn = 1.0 - bs_up
        z = float(snap.get("moneyness_z", 0.0))
        diverged = bool(snap.get("spot_vs_locked_diverged", False))

        candidates = [
            ("UP", bs_up, up_book, z),
            ("DOWN", bs_dn, dn_book, -z),
        ]
        max_sniper_px = min(0.86, max(0.78, float(ap.get("max_entry_price", 0.75)) + 0.10))

        for side, true_prob, book, signed_z in candidates:
            bid = float(book.get("best_bid") or 0.0)
            ask = float(book.get("best_ask") or 0.0)
            if bid <= 0.0 or ask <= 0.0 or (ask - bid) > 0.05:
                continue
            if signed_z < 0.85 or true_prob < 0.72:
                continue

            eff_fee_base = HARD_LIMITS["taker_fee_rate"] if taker_fee_rate_override is None else float(taker_fee_rate_override)
            taker_fee_rate = eff_fee_base * ask * (1.0 - ask)
            taker_edge = true_prob - (ask + taker_fee_rate)
            min_req_edge = 0.075 if diverged else 0.105

            if ask <= max_sniper_px and ask >= HARD_LIMITS["min_entry_price"] and taker_edge >= max(0.11, min_req_edge + 0.02):
                return {
                    "side": side,
                    "arm": {"id": "TWAP_SNIPER_TAKER", "offset_cents": 0.0, "ttl_ms": 1500, "is_taker": True},
                    "limit_price": round(ask, 2),
                    "true_prob": round(true_prob, 4),
                    "edge": round(taker_edge, 4),
                    "shadow_only": shadow_only,
                    "reason": f"尾盘TWAP{'现货背离' if diverged else '规则'}错价狙击(锁定Z={signed_z:+.2f}σ, 理论{true_prob*100:.0f}% vs 盘口{ask*100:.0f}¢)",
                }

            maker_px = round(bid + 0.01, 2) if (bid + 0.01 < ask) else round(bid, 2)
            maker_edge = true_prob - maker_px
            if HARD_LIMITS["min_entry_price"] <= maker_px <= max_sniper_px and maker_px < ask and maker_edge >= min_req_edge:
                return {
                    "side": side,
                    "arm": {"id": "TWAP_SNIPER_MAKER", "offset_cents": 0.01, "ttl_ms": 3500, "is_taker": False},
                    "limit_price": maker_px,
                    "true_prob": round(true_prob, 4),
                    "edge": round(maker_edge, 4),
                    "shadow_only": shadow_only,
                    "reason": f"尾盘TWAP{'现货背离' if diverged else '规则'}挂单狙击(锁定Z={signed_z:+.2f}σ, 理论{true_prob*100:.0f}% vs 挂{maker_px*100:.0f}¢)",
                }

        return None

    def evaluate_early_exit(
        self,
        primary_ord: dict,
        live_book: dict,
        model_win_prob: float,
        elapsed_sec: int,
        risk_cfg: dict | None,
        now: float,
    ) -> Optional[dict]:
        """
        v18 Autonomous Exit-Arm Evaluator with 0%-Fee Post-Only Maker SELL TP:
        - HOLD_EXPIRY / ARB_LOCK_AUTO: never triggers early SELL exit.
        - MAKER_FAIR_TP:  0%-fee Maker SELL when market pays >= model_win_prob + 2.5% AND gain >= +5c (no blind SL).
        - PROP_TP_45:     0%-fee Maker SELL at entry + prop_tp_ratio * (1 - entry), PLUS strict emergency Taker SL
                          ONLY when best_bid <= 0.45 (actually losing!), drop >= sl_drop_cents, AND model confirms collapse.
        """
        if primary_ord.get("status") != "FILLED":
            return None
        if primary_ord.get("arm_id") in ("ARB_LOCK_MAKER", "ARB_LOCK_TAKER", "TWAP_SNIPER_MAKER", "TWAP_SNIPER_TAKER"):
            return None

        exit_arm = str(primary_ord.get("exit_arm") or "HOLD_EXPIRY")
        if exit_arm in ("HOLD_EXPIRY", "ARB_LOCK_AUTO"):
            return None

        filled_at = float(primary_ord.get("filled_at") or primary_ord.get("created_at") or 0.0)
        if now - filled_at < 5.0:
            return None

        best_bid = float(live_book.get("best_bid") or 0.0)
        best_ask = float(live_book.get("best_ask") or 0.0)
        if best_bid <= 0.02 or best_bid >= 0.98:
            return None

        entry_px = float(primary_ord.get("fill_price") or primary_ord["limit_price"])
        ap = self.get_auto_params()

        # Post-Only Maker SELL price: sit at best_ask - 1c if spread >= 2c, else at best_ask (strictly > best_bid!)
        if best_ask > best_bid + 0.015:
            maker_sell_px = round(best_ask - 0.01, 2)
        elif best_ask > best_bid:
            maker_sell_px = round(best_ask, 2)
        else:
            maker_sell_px = round(min(0.98, best_bid + 0.01), 2)

        # 1. MAKER_FAIR_TP (also active as opportunistic fair-value exit inside PROP_TP_45)
        fair_gain = maker_sell_px - entry_px
        if fair_gain >= 0.05 and maker_sell_px >= (model_win_prob + 0.025):
            return {
                "exit_reason": "公允Maker止盈",
                "trigger_price": maker_sell_px,
                "is_maker": True,
                "ttl_ms": 8000,
                "detail": f"0费挂卖@{maker_sell_px:.2f} (高于模型公允{model_win_prob:.2f}, 盈+{fair_gain*100:.1f}¢)",
            }

        # 2. PROP_TP_45: Proportional remaining-upside 0%-fee Maker TP + strict collapse SL (only when bid <= 0.45)
        if exit_arm == "PROP_TP_45":
            prop_ratio = float(ap.get("prop_tp_ratio", 0.48))
            target_gain = max(0.08, min(0.28, prop_ratio * max(0.10, 1.0 - entry_px)))
            target_px = round(min(0.96, entry_px + target_gain), 2)

            if maker_sell_px >= target_px:
                return {
                    "exit_reason": "比例Maker止盈",
                    "trigger_price": maker_sell_px,
                    "is_maker": True,
                    "ttl_ms": 8000,
                    "detail": f"0费比例挂卖@{maker_sell_px:.2f} (目标{target_px:.2f}, 成本{entry_px:.2f})",
                }

            sl_floor = float(ap.get("sl_floor_bid", 0.45))
            sl_drop = float(ap.get("sl_drop_cents", 0.20))
            if (
                elapsed_sec <= 260
                and best_bid <= sl_floor
                and (entry_px - best_bid) >= sl_drop
                and model_win_prob <= (best_bid + 0.02)
            ):
                return {
                    "exit_reason": "防坍塌止损",
                    "trigger_price": best_bid,
                    "is_maker": False,
                    "ttl_ms": 2500,
                    "detail": f"跌破{sl_floor:.2f}防线(买一{best_bid:.2f}, 模型仅{model_win_prob*100:.0f}%)",
                }

        return None

    def evaluate_signal_with_reason(
        self,
        snap: dict,
        elapsed_sec: int,
        up_mid: float = 0.5,
        dn_mid: float = 0.5,
        enable_orderflow_lead: bool = True,
    ) -> tuple[Optional[str], Optional[str], str, str]:
        """
        v20 Triple-Engine Session-Adaptive Entry Trigger:
        - Path 1 (`orderflow_lead`):   Adaptive Perp 3s CVD relative to 10m volume baseline + adaptive basis jump.
        - Path 2 (`momentum_breakout`): Volatility-scaled Quant Elder Net Power (adapts cleanly to calm & busy sessions).
        - Path 3 (`energy_accum`):     Sustained Energy Accumulation slow-drift trigger with anti-climax & Z guards.
        Returns (signal, block_reason, trigger_label, entry_strategy_key).
        """
        if snap.get("regime") in ("WARMING_UP", "EXTREME_SPIKE"):
            return None, None, "", ""

        bucket_key = self.time_bucket_for(elapsed_sec)
        bucket = self.state["time_buckets"][bucket_key]
        if not bucket.get("enabled", True):
            return None, None, "", ""

        base_thr = float(bucket["net_power_min_bps"])
        atr = float(snap.get("atr_bps", 1.2))
        atr_base = float(snap.get("atr_baseline_bps", atr))
        ref_atr = max(atr, atr_base)

        # Volatility-adaptive momentum threshold:
        # Calm session (ref_atr=0.45) -> vol_scale=0.45 -> eff_thr ~ 0.81 bps (instead of unreachable 1.8~2.4!)
        # Active session (ref_atr=1.40) -> vol_scale=1.04 -> eff_thr ~ 1.87 bps
        # Hot session (ref_atr=2.50) -> vol_scale=1.35 -> eff_thr ~ 3.12 bps
        vol_scale = max(0.45, min(1.35, ref_atr / 1.35))
        eff_thr = max(0.75, base_thr * vol_scale, atr * 1.25)
        bull_bear_thr = eff_thr * 0.35

        net_p = float(snap["net_power_bps"])
        bull_p = float(snap["bull_power_bps"])
        bear_p = float(snap["bear_power_bps"])
        ema_slope = float(snap.get("ema13_slope_bps", 0.0))

        raw_sig = None
        trigger_source = ""
        entry_strat = ""

        # Path 1: Session-Adaptive Leading Perp Orderflow Trigger
        of_score = float(snap.get("orderflow_score", 0.0))
        cvd_3s_usd = float(snap.get("cvd_3s_usd", 0.0))
        req_cvd_usd = max(25000.0, float(snap.get("adaptive_cvd_3s_min_usd", 65000.0)))
        basis_jump = float(snap.get("perp_basis_jump_bps", 0.0))
        req_basis_jump = max(0.35, min(0.85, atr * 0.65))
        if enable_orderflow_lead and elapsed_sec <= 245:
            if of_score >= 1.20 and cvd_3s_usd >= req_cvd_usd and (basis_jump >= req_basis_jump or net_p > 0.0):
                raw_sig = "UP"
                trigger_source = "永续订单流抢跑"
                entry_strat = "orderflow_lead"
            elif of_score <= -1.20 and cvd_3s_usd <= -req_cvd_usd and (basis_jump <= -req_basis_jump or net_p < 0.0):
                raw_sig = "DOWN"
                trigger_source = "永续订单流抢跑"
                entry_strat = "orderflow_lead"

        # Path 2: Volatility-Adaptive Quant Elder Momentum Confirmation
        atr_floor = min(0.35, float(self.state.get("atr_min_bps", 0.30)))
        if not raw_sig and atr >= atr_floor:
            if net_p > eff_thr and bull_p > bull_bear_thr:
                if enable_orderflow_lead and of_score <= -1.10:
                    return None, f"永续订单流反向砸盘(OF={of_score:.2f})，过滤滞后UP动量", "", ""
                raw_sig = "UP"
                trigger_source = "动量突破"
                entry_strat = "momentum_breakout"
            elif net_p < -eff_thr and bear_p < -bull_bear_thr:
                if enable_orderflow_lead and of_score >= 1.10:
                    return None, f"永续订单流反向扫货(OF={of_score:+.2f})，过滤滞后DOWN动量", "", ""
                raw_sig = "DOWN"
                trigger_source = "动量突破"
                entry_strat = "momentum_breakout"

        # Path 3: Sustained Energy Accumulation Slow-Drift Trigger (captures calm & institutional slow-bleed trends)
        if not raw_sig and 20 <= elapsed_sec <= 245 and atr >= max(0.22, atr_floor * 0.8):
            e_score = float(snap.get("energy_accum_score", 0.0))
            e_cons = float(snap.get("energy_consistency", 0.0))
            z_cur = float(snap.get("moneyness_z", 0.0))
            # Anti-climax & anti-exhaustion gate: enter during steady build-up, never chase extreme climax spikes
            not_climax = abs(net_p) <= max(4.5, atr * 3.5) and abs(z_cur) < 1.45
            if not_climax:
                # v24 decoupled gate: energy_accum_score already embeds consistency & 12s/5s real price progress
                if e_score >= 1.15 and e_cons >= 0.50 and net_p > 0.10 and ema_slope >= 0.0:
                    if enable_orderflow_lead and of_score <= -0.85:
                        return None, f"永续反向砸盘(OF={of_score:.2f})，过滤UP蓄能信号", "", ""
                    raw_sig = "UP"
                    trigger_source = "持续能量蓄势"
                    entry_strat = "energy_accum"
                elif e_score <= -1.15 and e_cons <= -0.50 and net_p < -0.10 and ema_slope <= 0.0:
                    if enable_orderflow_lead and of_score >= 0.85:
                        return None, f"永续反向扫货(OF={of_score:+.2f})，过滤DOWN蓄能信号", "", ""
                    raw_sig = "DOWN"
                    trigger_source = "持续能量蓄势"
                    entry_strat = "energy_accum"

        if not raw_sig:
            return None, None, "", ""

        z = float(snap.get("moneyness_z", 0.0))
        rem_sec = int(snap.get("remaining_sec", max(1, 300 - elapsed_sec)))
        iv_state = str(snap.get("iv_state", "FAIR"))
        iv_rv = float(snap.get("iv_rv_ratio", 1.0))

        # Guard A: Final 12s near-target Gamma pin lottery
        if rem_sec <= 12 and abs(z) < 0.85:
            return None, f"尾盘最后{rem_sec}秒贴线博弈(|Z|={abs(z):.2f})，跳过", "", ""

        # Guard B: Deep-ITM/OTM already decided in late stage
        if rem_sec <= 90 and abs(z) >= 2.55:
            return None, f"剩余{rem_sec}秒价差过大(Z={z:+.2f}σ)，胜负已定价", "", ""

        # Guard C: Anti-Dead-Cat-Bounce against remaining-time deficit
        max_counter_z = 0.85 if rem_sec <= 60 else (1.15 if rem_sec <= 150 else 1.45)
        if raw_sig == "UP" and z < -max_counter_z:
            return None, f"拦截UP死猫跳(落后目标{z:.2f}σ, 仅剩{rem_sec}秒)", "", ""
        if raw_sig == "DOWN" and z > max_counter_z:
            return None, f"拦截DOWN逆势反抽(领先目标+{z:.2f}σ, 仅剩{rem_sec}秒)", "", ""

        # Guard D: IV/RV Volatility Mismatch Protection
        is_buying_itm = (raw_sig == "UP" and z > 0.35) or (raw_sig == "DOWN" and z < -0.35)
        is_buying_otm = (raw_sig == "UP" and z < -0.35) or (raw_sig == "DOWN" and z > 0.35)
        sig_mid = up_mid if raw_sig == "UP" else dn_mid

        if iv_state == "IV_UNDERPRICED" and is_buying_itm and sig_mid >= 0.64:
            return None, f"盘口低估波动(IV/RV={iv_rv:.2f})，拦截高价追买领先方{raw_sig}@{sig_mid:.2f}", "", ""

        if iv_state == "IV_OVERPRICED" and is_buying_otm:
            return None, f"盘口恐慌溢价(IV/RV={iv_rv:.2f})，拦截抄底高估落后方{raw_sig}", "", ""

        return raw_sig, None, trigger_source, entry_strat

    def should_trigger_signal(self, snap: dict, elapsed_sec: int) -> Optional[str]:
        sig, _, _, _ = self.evaluate_signal_with_reason(snap, elapsed_sec)
        return sig

    def select_execution_arm(
        self,
        edge: float,
        is_strong_trend: bool,
        recent_cancel_same_side: bool = False,
        best_ask: float = 0.5,
        taker_fee_rate_override: Optional[float] = None,
    ) -> dict:
        taker_arm = next((a for a in self.state["arms"] if a.get("is_taker")), None)
        p_ask = max(0.05, min(0.95, best_ask))
        eff_fee_base = HARD_LIMITS["taker_fee_rate"] if taker_fee_rate_override is None else float(taker_fee_rate_override)
        taker_fee_est = eff_fee_base * p_ask * (1.0 - p_ask)
        net_taker_edge = edge - taker_fee_est

        if taker_arm and ((edge >= 0.105 and net_taker_edge >= 0.065) or (recent_cancel_same_side and net_taker_edge >= 0.055)):
            return taker_arm

        candidates = [
            a for a in self.state["arms"]
            if not (recent_cancel_same_side and a.get("id") == "MAKER_BID_0_TTL6S")
        ] or list(self.state["arms"])

        best_arm = candidates[0]
        best_sample = -1e9
        for arm in candidates:
            if arm.get("is_taker") and (edge < 0.085 or net_taker_edge < 0.05 or not is_strong_trend):
                continue
            cnt = max(1.0, float(arm["count"]))
            var = max(0.0008, float(arm["m2"]) / cnt)
            std = math.sqrt(var / cnt)
            sample = random.gauss(float(arm["mean_ev"]), std)
            if sample > best_sample:
                best_sample = sample
                best_arm = arm
        return best_arm

    def compute_order_price(self, arm: dict, best_bid: float, best_ask: float, max_entry_price: Optional[float] = None) -> Optional[float]:
        if best_bid <= 0.0 or best_ask <= 0.0 or best_bid > best_ask:
            return None

        if arm.get("is_taker"):
            px = round(best_ask, 2)
        elif arm["id"] == "MAKER_MID_TTL3S":
            px = round((best_bid + best_ask) / 2.0, 2)
            if px >= best_ask:
                px = round(best_ask - 0.01, 2)
            if px < best_bid:
                px = round(best_bid, 2)
        else:
            px = round(best_bid + float(arm["offset_cents"]), 2)
            if px >= best_ask:
                px = round(best_ask - 0.01, 2)
            if px < best_bid:
                px = round(best_bid, 2)

        auto_max = float(self.get_auto_params().get("max_entry_price", 0.75))
        eff_max = auto_max if max_entry_price is None else float(max_entry_price)
        upper = min(eff_max, HARD_LIMITS["max_entry_price"])
        lower = HARD_LIMITS["min_entry_price"]
        if px < lower or px > upper:
            return None
        if not arm.get("is_taker") and px >= best_ask:
            return None
        return round(px, 2)

    @staticmethod
    def price_zone_for(price: float) -> str:
        if price < 0.45:
            return "low"
        if price <= 0.60:
            return "mid"
        return "high"

    def compute_dynamic_min_edge(
        self,
        entry_price: float,
        risk_cfg: dict | None = None,
        remaining_sec: int = 150,
        iv_state: str = "FAIR",
        is_buying_itm: bool = False,
        is_twap_sniper: bool = False,
    ) -> tuple[float, dict]:
        """
        Fully Autonomous Asymmetric Price-Adaptive Edge Threshold (anchored on evolved auto_params['min_edge']).
        """
        ec = self.state.get("edge_curve") or self._default_state()["edge_curve"]
        ap = self.get_auto_params()
        low_anchor = float(ec.get("low_anchor", 0.45))
        high_anchor = float(ec.get("high_anchor", 0.60))
        low_slope = float(ec.get("low_slope", 0.22))
        high_slope = float(ec.get("high_slope", 0.12))
        mid_offset = float(ec.get("mid_offset", 0.0))

        base_auto = float(ap.get("min_edge", 0.042))
        zone = self.price_zone_for(entry_price)
        price_adj = 0.0
        if entry_price < low_anchor:
            price_adj = low_slope * (low_anchor - entry_price)
        elif entry_price > high_anchor and not is_twap_sniper:
            price_adj = high_slope * (entry_price - high_anchor)

        late_cushion = 0.0 if is_twap_sniper else (0.015 if remaining_sec <= 45 else 0.0)
        iv_adj = 0.0
        if not is_twap_sniper:
            if iv_state == "IV_OVERPRICED" and is_buying_itm:
                iv_adj = -0.005
            elif iv_state == "IV_UNDERPRICED":
                iv_adj = 0.015

        raw_min = base_auto + mid_offset + price_adj + iv_adj
        min_edge = max(HARD_LIMITS["min_edge_prob"], raw_min) + late_cushion
        min_edge = round(min(0.22, min_edge), 4)
        return min_edge, {
            "zone": zone,
            "base_edge": round(base_auto, 4),
            "price_adj": round(price_adj, 4),
            "mid_offset": round(mid_offset, 4),
            "iv_adj": round(iv_adj, 4),
            "late_cushion": round(late_cushion, 4),
            "min_edge": min_edge,
        }

    @staticmethod
    def required_buy_cash(
        size_usdc: float,
        entry_price: float,
        is_taker: bool,
        taker_fee_rate: Optional[float] = None,
        safety_pad_usdc: float = 0.02,
    ) -> float:
        """
        Exact Polymarket CLOB V2 total pUSD cash required to execute a BUY order of `size_usdc`:
        - Taker: size_usdc * (1 + fee_rate * (1 - p)) + safety_pad (verified to $0.0001 against Polymarket CSV)
        - Maker: size_usdc + safety_pad (0% maker fee)
        """
        sz = max(0.0, float(size_usdc))
        if sz <= 0.0:
            return 0.0
        p = max(0.01, min(0.99, float(entry_price)))
        eff_fee = HARD_LIMITS["taker_fee_rate"] if taker_fee_rate is None else max(0.0, float(taker_fee_rate))
        fee_mult = (1.0 + eff_fee * (1.0 - p)) if is_taker else 1.0
        return round(sz * fee_mult + max(0.0, float(safety_pad_usdc)), 4)

    @staticmethod
    def max_affordable_notional(
        available_cash_usdc: float,
        entry_price: float,
        is_taker: bool,
        taker_fee_rate: Optional[float] = None,
        safety_pad_usdc: float = 0.02,
    ) -> float:
        """Maximum BUY notional (`size_usdc`) safely affordable within `available_cash_usdc` after fees & rounding."""
        avail = float(available_cash_usdc) - max(0.0, float(safety_pad_usdc))
        if avail <= 0.0:
            return 0.0
        p = max(0.01, min(0.99, float(entry_price)))
        eff_fee = HARD_LIMITS["taker_fee_rate"] if taker_fee_rate is None else max(0.0, float(taker_fee_rate))
        fee_mult = (1.0 + eff_fee * (1.0 - p)) if is_taker else 1.0
        return round(max(0.0, math.floor((avail / fee_mult) * 100.0) / 100.0), 2)

    def calculate_position_size(
        self,
        bankroll_usdc: float,
        model_win_prob: float,
        entry_price: float,
        is_taker: bool,
        risk_cfg: dict,
        remaining_sec: int = 150,
        iv_state: str = "FAIR",
        is_buying_itm: bool = False,
        is_twap_sniper: bool = False,
        taker_fee_rate_override: Optional[float] = None,
        spendable_cash_usdc: Optional[float] = None,
        min_shares: float = 1.0,
    ) -> float:
        eff_fee_base = HARD_LIMITS["taker_fee_rate"] if taker_fee_rate_override is None else float(taker_fee_rate_override)
        fee_per_share = eff_fee_base * entry_price * (1.0 - entry_price) if is_taker else 0.0
        eff_cost = entry_price + fee_per_share
        edge = model_win_prob - eff_cost

        min_edge, _ = self.compute_dynamic_min_edge(
            entry_price, risk_cfg,
            remaining_sec=remaining_sec, iv_state=iv_state,
            is_buying_itm=is_buying_itm, is_twap_sniper=is_twap_sniper,
        )
        if edge < min_edge or eff_cost >= 0.95:
            return 0.0

        ec = self.state.get("edge_curve") or self._default_state()["edge_curve"]
        low_size_mult = float(ec.get("low_size_mult", 0.60))
        if entry_price < 0.35 and not is_twap_sniper:
            t = max(0.0, min(1.0, (entry_price - 0.20) / 0.15))
            size_scale = low_size_mult + t * (1.0 - low_size_mult)
        else:
            size_scale = 1.0

        min_share_notional = round(max(0.0, float(min_shares)) * max(0.01, float(entry_price)) + 0.01, 2)
        min_notional = max(HARD_LIMITS["min_unit_usdc"], min_share_notional)

        base_unit = max(min_notional, float(risk_cfg.get("unit_size_usdc", 10.0)))
        max_cap = max(min_notional, float(bankroll_usdc) * HARD_LIMITS["max_position_pct"])
        # Autonomous Dynamic Fractional Kelly capped gently by user's unit_size_usdc ceiling (up to 1.8x base_unit)
        full_kelly = edge / max(0.05, 1.0 - eff_cost)
        brier = self.get_rolling_brier()
        kelly_mult = max(0.08, min(0.35, 0.55 - brier * 1.6))
        kelly_size = float(bankroll_usdc) * full_kelly * kelly_mult * size_scale
        user_cap = min(max_cap, max(base_unit * 1.8, min_notional))
        target = max(min_notional, min(kelly_size, user_cap))

        # Hard spendable-cash cap (accounting for dynamic taker fee & rounding pad!)
        if spendable_cash_usdc is not None:
            affordable = self.max_affordable_notional(
                spendable_cash_usdc, entry_price, is_taker, eff_fee_base
            )
            if affordable < min_notional:
                return 0.0
            target = min(target, affordable)

        return round(target, 2)

    def _simulate_counterfactual_exit_rois(
        self,
        ord_row: dict,
        official_outcome: str,
        round_slices: list[dict],
    ) -> dict[str, float]:
        """
        Counterfactual Evaluation across all 4 Exit Arms on a completed primary trade using
        the round's real post-fill 5s L2 orderbook slices & official Polymarket outcome!
        """
        side = str(ord_row.get("side") or "UP")
        entry_px = float(ord_row.get("fill_price") or ord_row.get("limit_price") or 0.5)
        size = max(0.01, float(ord_row.get("size_usdc") or 10.0))
        shares = float(ord_row.get("shares") or (size / max(0.01, entry_px)))
        entry_fee = float(ord_row.get("fee_usdc") or 0.0) if ord_row.get("status") == "SETTLED" else 0.0
        entry_elapsed = int(ord_row.get("elapsed_sec") or 0)
        won = (side == official_outcome)

        # 1. HOLD_EXPIRY exact counterfactual ROI
        hold_pnl = (shares * (1.0 if won else 0.0)) - size - entry_fee
        rois: dict[str, float] = {"HOLD_EXPIRY": hold_pnl / size}

        post_slices = [s for s in (round_slices or []) if int(s.get("elapsed_sec") or 0) >= entry_elapsed + 5]
        if not post_slices:
            return rois

        ap = self.get_auto_params()
        prop_ratio = float(ap.get("prop_tp_ratio", 0.48))
        prop_target = round(min(0.96, entry_px + max(0.08, min(0.28, prop_ratio * max(0.10, 1.0 - entry_px)))), 2)
        sl_floor = float(ap.get("sl_floor_bid", 0.45))
        sl_drop = float(ap.get("sl_drop_cents", 0.20))
        arb_max_c = float(ap.get("arb_max_combined_price", 0.95))

        fair_roi = None
        prop_roi = None
        arb_roi = None

        for s in post_slices:
            el = int(s.get("elapsed_sec") or 0)
            bid = float(s.get("pm_up_bid") if side == "UP" else s.get("pm_down_bid") or 0.0)
            ask = float(s.get("pm_up_ask") if side == "UP" else s.get("pm_down_ask") or 0.0)
            opp_bid = float(s.get("pm_down_bid") if side == "UP" else s.get("pm_up_bid") or 0.0)
            opp_ask = float(s.get("pm_down_ask") if side == "UP" else s.get("pm_up_ask") or 0.0)
            bs_up = float(s.get("bs_up_prob") or 0.5)
            fair_p = bs_up if side == "UP" else (1.0 - bs_up)

            if bid <= 0.01 or ask <= 0.01:
                continue

            # Counterfactual MAKER_FAIR_TP (0% Maker SELL filled if subsequent bid >= fair_sell_px)
            if fair_roi is None and (bid - entry_px) >= 0.05 and bid >= (fair_p + 0.025):
                fair_roi = ((shares * bid) - size - entry_fee) / size

            # Counterfactual PROP_TP_45 (0% Maker TP OR Taker collapse SL)
            if prop_roi is None:
                if bid >= prop_target:
                    prop_roi = ((shares * prop_target) - size - entry_fee) / size
                elif el <= 260 and bid <= sl_floor and (entry_px - bid) >= sl_drop and fair_p <= (bid + 0.02):
                    sl_fee = shares * HARD_LIMITS["taker_fee_rate"] * bid * (1.0 - bid)
                    prop_roi = ((shares * bid) - size - entry_fee - sl_fee) / size

            # Counterfactual ARB_LOCK_AUTO (0% Maker opposite equal-share hedge)
            if arb_roi is None and el <= 285 and opp_bid > 0.02 and opp_ask > 0.02:
                opp_lim = round(opp_bid + 0.01, 2) if (opp_bid + 0.01 < opp_ask) else round(opp_bid, 2)
                if (entry_px + opp_lim) <= arb_max_c and opp_ask <= opp_lim + 0.01:
                    arb_cost = shares * opp_lim
                    locked_pnl = shares * 1.0 - size - arb_cost - entry_fee
                    arb_roi = locked_pnl / (size + arb_cost)

        rois["MAKER_FAIR_TP"] = fair_roi if fair_roi is not None else rois["HOLD_EXPIRY"]
        rois["PROP_TP_45"] = prop_roi if prop_roi is not None else rois["HOLD_EXPIRY"]
        rois["ARB_LOCK_AUTO"] = arb_roi if arb_roi is not None else rois["HOLD_EXPIRY"]
        return rois

    def update_on_round_settlement(
        self,
        official_outcome: str,
        episode_samples: list[tuple[list[float], float]],
        settled_orders: list[dict],
        freeze_updates: bool = False,
        round_slices: Optional[list[dict]] = None,
    ):
        if freeze_updates or official_outcome not in ("UP", "DOWN"):
            return

        y = 1.0 if official_outcome == "UP" else 0.0

        # 1. Online 10-feature Probability Calibrator
        if episode_samples:
            lr = float(self.state["learning_rate"])
            l2 = float(self.state["l2_decay"])
            w = self.state["weights"]
            n = len(episode_samples)
            grad_acc = [0.0] * len(w)
            brier_acc = 0.0

            for feats, pred_p in episode_samples:
                if len(feats) != len(w):
                    continue
                err = pred_p - y
                brier_acc += err * err
                for i in range(len(w)):
                    grad_acc[i] += err * feats[i] / n

            for i in range(len(w)):
                reg_target = 0.95 if i == 1 else 0.0
                w[i] -= lr * (grad_acc[i] + l2 * (w[i] - reg_target))
                if i == 0:
                    w[i] = max(-0.35, min(0.35, w[i]))
                elif i == 1:
                    w[i] = max(0.35, min(1.50, w[i]))
                else:
                    w[i] = max(-3.0, min(3.0, w[i]))

            avg_brier = brier_acc / max(1, n)
            rb = self.state.get("recent_brier", [])
            rb.append(round(avg_brier, 5))
            self.state["recent_brier"] = rb[-40:]
            self.state["updates_count"] = int(self.state.get("updates_count", 0)) + 1

        # 2. Execution Arms + Time Buckets
        decay = 0.97
        for ord_row in settled_orders:
            arm_id = ord_row.get("arm_id")
            if arm_id in ("ARB_LOCK_MAKER", "ARB_LOCK_TAKER", "TWAP_SNIPER_MAKER", "TWAP_SNIPER_TAKER"):
                continue
            size = float(ord_row.get("size_usdc") or 1.0)
            pnl = float(ord_row.get("pnl_usdc") or 0.0)
            completed = ord_row.get("status") in ("SETTLED", "CLOSED") and ord_row.get("fill_price") is not None
            reward = (pnl / size) if completed else -0.002

            for arm in self.state["arms"]:
                if arm["id"] == arm_id:
                    arm["count"] = float(arm["count"]) * decay + 1.0
                    delta = reward - float(arm["mean_ev"])
                    arm["mean_ev"] = round(float(arm["mean_ev"]) + delta / arm["count"], 5)
                    delta2 = reward - float(arm["mean_ev"])
                    arm["m2"] = round(float(arm["m2"]) * decay + delta * delta2, 6)
                    break

            if completed and ord_row.get("elapsed_sec") is not None:
                bk = self.time_bucket_for(int(ord_row["elapsed_sec"]))
                b = self.state["time_buckets"][bk]
                if pnl > 0:
                    b["wins"] += 1
                elif pnl < 0:
                    b["losses"] += 1
                b["ema_ev"] = round(0.85 * float(b["ema_ev"]) + 0.15 * reward, 5)
                cur_thr = float(b["net_power_min_bps"])
                if b["ema_ev"] < -0.05:
                    b["net_power_min_bps"] = round(min(4.5, cur_thr * 1.06), 3)
                elif b["ema_ev"] > 0.05:
                    b["net_power_min_bps"] = round(max(1.2, cur_thr * 0.96), 3)

        # v24 Anti-Lockout Mean Reversion on all time buckets every settled round (prevents ratchet starvation)
        _bucket_priors = {"0_60": 2.4, "60_180": 1.8, "180_255": 2.0, "255_300": 3.5}
        for _bk, _prior in _bucket_priors.items():
            _b = self.state.get("time_buckets", {}).get(_bk)
            if isinstance(_b, dict):
                _cur = float(_b.get("net_power_min_bps", _prior))
                _b["net_power_min_bps"] = round(max(1.2, min(4.5, 0.98 * _cur + 0.02 * _prior)), 3)

        # 3. Self-Evolving Price-Zone Edge Curve + Autonomous auto_params
        ec = self.state.get("edge_curve")
        ap = self.get_auto_params()
        if isinstance(ec, dict) and "zones" in ec:
            zones = ec["zones"]
            if episode_samples:
                low_errs, mid_errs, high_errs = [], [], []
                for _, pred_up in episode_samples:
                    p_u = float(pred_up)
                    if p_u < 0.45:
                        low_errs.append(p_u - y)
                        high_errs.append((1.0 - p_u) - (1.0 - y))
                    elif p_u > 0.60:
                        high_errs.append(p_u - y)
                        low_errs.append((1.0 - p_u) - (1.0 - y))
                    else:
                        mid_errs.append((p_u - y) if p_u >= 0.5 else ((1.0 - p_u) - (1.0 - y)))

                if low_errs:
                    zones["low"]["ema_calib_bias"] = round(0.92 * float(zones["low"]["ema_calib_bias"]) + 0.08 * (sum(low_errs) / len(low_errs)), 5)
                if mid_errs:
                    zones["mid"]["ema_calib_bias"] = round(0.92 * float(zones["mid"]["ema_calib_bias"]) + 0.08 * (sum(mid_errs) / len(mid_errs)), 5)
                if high_errs:
                    zones["high"]["ema_calib_bias"] = round(0.92 * float(zones["high"]["ema_calib_bias"]) + 0.08 * (sum(high_errs) / len(high_errs)), 5)

            touched_zones = set()
            for ord_row in settled_orders:
                arm_id = ord_row.get("arm_id")
                if arm_id in ("ARB_LOCK_MAKER", "ARB_LOCK_TAKER", "TWAP_SNIPER_MAKER", "TWAP_SNIPER_TAKER"):
                    continue
                if ord_row.get("status") not in ("SETTLED", "CLOSED") or ord_row.get("fill_price") is None:
                    continue
                fill_px = float(ord_row["fill_price"])
                size = float(ord_row.get("size_usdc") or 1.0)
                pnl = float(ord_row.get("pnl_usdc") or 0.0)
                roi = pnl / max(0.01, size)
                zk = self.price_zone_for(fill_px)
                z_st = zones[zk]
                z_st["samples"] = int(z_st.get("samples", 0)) + 1
                if pnl > 0:
                    z_st["wins"] = int(z_st.get("wins", 0)) + 1
                elif pnl < 0:
                    z_st["losses"] = int(z_st.get("losses", 0)) + 1
                z_st["ema_roi"] = round(0.84 * float(z_st.get("ema_roi", 0.0)) + 0.16 * roi, 5)
                if ord_row.get("model_win_prob") is not None:
                    actual_win = 1.0 if ord_row.get("side") == official_outcome else 0.0
                    c_err = float(ord_row["model_win_prob"]) - actual_win
                    z_st["ema_calib_bias"] = round(0.85 * float(z_st.get("ema_calib_bias", 0.0)) + 0.15 * c_err, 5)
                touched_zones.add(zk)

            lz = zones["low"]
            ls = float(ec.get("low_slope", 0.22))
            lsm = float(ec.get("low_size_mult", 0.60))
            if ("low" in touched_zones and lz["samples"] >= 2) or abs(float(lz["ema_calib_bias"])) > 0.04:
                if (lz["samples"] >= 2 and float(lz["ema_roi"]) < -0.04) or float(lz["ema_calib_bias"]) > 0.04:
                    ls = min(0.45, ls + 0.025)
                    lsm = max(0.35, lsm - 0.04)
                elif lz["samples"] >= 3 and float(lz["ema_roi"]) > 0.06 and float(lz["ema_calib_bias"]) < 0.02:
                    ls = max(0.10, ls - 0.015)
                    lsm = min(0.90, lsm + 0.03)
            ec["low_slope"] = round(max(0.10, min(0.45, 0.992 * ls + 0.008 * 0.22)), 4)
            ec["low_size_mult"] = round(max(0.35, min(0.90, 0.992 * lsm + 0.008 * 0.60)), 3)

            hz = zones["high"]
            hs = float(ec.get("high_slope", 0.12))
            max_px = float(ap.get("max_entry_price", 0.75))
            if ("high" in touched_zones and hz["samples"] >= 2) or abs(float(hz["ema_calib_bias"])) > 0.035:
                if (hz["samples"] >= 2 and float(hz["ema_roi"]) < -0.03) or float(hz["ema_calib_bias"]) > 0.035:
                    hs = min(0.30, hs + 0.018)
                    max_px = max(0.72, max_px - 0.01)
                elif hz["samples"] >= 3 and float(hz["ema_roi"]) > 0.04 and float(hz["ema_calib_bias"]) < 0.02:
                    hs = max(0.04, hs - 0.010)
                    max_px = min(HARD_LIMITS["max_entry_price"], max_px + 0.01)
            ec["high_slope"] = round(max(0.04, min(0.30, 0.992 * hs + 0.008 * 0.12)), 4)
            ap["max_entry_price"] = round(max(0.72, min(HARD_LIMITS["max_entry_price"], 0.995 * max_px + 0.005 * 0.75)), 2)

            mz = zones["mid"]
            mo = float(ec.get("mid_offset", 0.0))
            base_me = float(ap.get("min_edge", 0.042))
            if "mid" in touched_zones and mz["samples"] >= 3:
                if float(mz["ema_roi"]) < -0.04 or float(mz["ema_calib_bias"]) > 0.035:
                    mo = min(0.025, mo + 0.003)
                    base_me = min(0.085, base_me + 0.002)
                elif float(mz["ema_roi"]) > 0.05 and float(mz["ema_calib_bias"]) < 0.02:
                    mo = max(-0.005, mo - 0.002)
                    base_me = max(HARD_LIMITS["min_edge_prob"], base_me - 0.0015)
            ec["mid_offset"] = round(max(-0.005, min(0.025, 0.985 * mo)), 4)
            # Also anchor min_edge gently to rolling Brier quality
            brier = self.get_rolling_brier()
            brier_target = max(HARD_LIMITS["min_edge_prob"], min(0.075, 0.038 + max(0.0, brier - 0.19) * 0.35))
            ap["min_edge"] = round(max(HARD_LIMITS["min_edge_prob"], min(0.085, 0.96 * base_me + 0.04 * brier_target)), 4)

        # 4. Counterfactual Exit-Arm Evolution (Realized Exit Alpha + 5s Slice Counterfactual Replay)
        exit_arms = self.state.get("exit_arms") or []
        exit_arm_map = {a["id"]: a for a in exit_arms}
        arb_orders = [o for o in settled_orders if o.get("arm_id") in ("ARB_LOCK_MAKER", "ARB_LOCK_TAKER") and o.get("status") == "SETTLED"]
        arb_by_mode = {o["mode"]: o for o in arb_orders}

        for ord_row in settled_orders:
            arm_id = ord_row.get("arm_id")
            if arm_id in ("ARB_LOCK_MAKER", "ARB_LOCK_TAKER", "TWAP_SNIPER_MAKER", "TWAP_SNIPER_TAKER"):
                continue
            if ord_row.get("status") not in ("SETTLED", "CLOSED") or ord_row.get("fill_price") is None:
                continue

            chosen_ex = str(ord_row.get("exit_arm") or "HOLD_EXPIRY")
            if chosen_ex not in exit_arm_map:
                chosen_ex = "HOLD_EXPIRY"

            size = max(0.01, float(ord_row.get("size_usdc") or 10.0))
            pnl = float(ord_row.get("pnl_usdc") or 0.0)
            # For ARB_LOCK_AUTO, combine primary + equal-share arb hedge for true net ROI
            if chosen_ex == "ARB_LOCK_AUTO" and ord_row["mode"] in arb_by_mode:
                ao = arb_by_mode[ord_row["mode"]]
                realized_roi = (pnl + float(ao.get("pnl_usdc") or 0.0)) / (size + float(ao.get("size_usdc") or 0.0))
            else:
                realized_roi = pnl / size

            # Counterfactual hold ROI & full 4-arm simulation on this round's real 5s slices
            cf_rois = self._simulate_counterfactual_exit_rois(ord_row, official_outcome, round_slices or [])
            cf_hold_roi = cf_rois.get("HOLD_EXPIRY", realized_roi)

            # 4a. Full weight update (w=1.0) on actually executed exit arm
            ex_obj = exit_arm_map[chosen_ex]
            ex_obj["count"] = round(float(ex_obj["count"]) * 0.97 + 1.0, 3)
            d1 = realized_roi - float(ex_obj["mean_ev"])
            ex_obj["mean_ev"] = round(float(ex_obj["mean_ev"]) + d1 / ex_obj["count"], 5)
            d2 = realized_roi - float(ex_obj["mean_ev"])
            ex_obj["m2"] = round(float(ex_obj["m2"]) * 0.97 + d1 * d2, 6)
            exit_alpha = realized_roi - cf_hold_roi
            ex_obj["alpha_ema"] = round(0.85 * float(ex_obj.get("alpha_ema", 0.0)) + 0.15 * exit_alpha, 5)
            if realized_roi > 0:
                ex_obj["wins"] = int(ex_obj.get("wins", 0)) + 1
            elif realized_roi < 0:
                ex_obj["losses"] = int(ex_obj.get("losses", 0)) + 1

            # 4b. Gentle counterfactual replay update (w=0.35) on the other 3 arms so Thompson Sampling never starves
            for other_id, other_roi in cf_rois.items():
                if other_id == chosen_ex or other_id not in exit_arm_map:
                    continue
                o_arm = exit_arm_map[other_id]
                w_cf = 0.35
                o_arm["count"] = round(float(o_arm["count"]) * 0.985 + w_cf, 3)
                od1 = other_roi - float(o_arm["mean_ev"])
                o_arm["mean_ev"] = round(float(o_arm["mean_ev"]) + (w_cf * od1) / o_arm["count"], 5)
                od2 = other_roi - float(o_arm["mean_ev"])
                o_arm["m2"] = round(float(o_arm["m2"]) * 0.985 + w_cf * od1 * od2, 6)
                o_alpha = other_roi - cf_hold_roi
                o_arm["alpha_ema"] = round(0.90 * float(o_arm.get("alpha_ema", 0.0)) + 0.10 * o_alpha, 5)

        # 5. Sub-Strategy Autonomous Shadow Hibernation & Wakeup
        subs = self.state.get("sub_strategies") or {}
        for ord_row in settled_orders:
            if ord_row.get("arm_id") in ("ARB_LOCK_MAKER", "ARB_LOCK_TAKER"):
                continue
            if ord_row.get("status") not in ("SETTLED", "CLOSED") or ord_row.get("fill_price") is None:
                continue
            sk = str(ord_row.get("entry_strategy") or "")
            if not sk:
                if ord_row.get("arm_id") in ("TWAP_SNIPER_MAKER", "TWAP_SNIPER_TAKER"):
                    sk = "twap_sniper"
                else:
                    sk = "momentum_breakout"
            if sk not in subs:
                continue

            st = subs[sk]
            size = max(0.01, float(ord_row.get("size_usdc") or 10.0))
            pnl = float(ord_row.get("pnl_usdc") or 0.0)
            roi = pnl / size
            is_shadow_ord = (ord_row.get("mode") == "SHADOW")

            if is_shadow_ord:
                st["shadow_samples"] = int(st.get("shadow_samples", 0)) + 1
                st["shadow_ema_roi"] = round(0.80 * float(st.get("shadow_ema_roi", 0.0)) + 0.20 * roi, 5)
                # Auto-wakeup from SHADOW once shadow performance proves positive ROI!
                if st.get("status") == "SHADOW" and st["shadow_samples"] >= 3 and float(st["shadow_ema_roi"]) > 0.03:
                    st["status"] = "ACTIVE"
                    st["ema_roi"] = round(max(0.0, float(st["shadow_ema_roi"]) * 0.5), 5)
                    st["shadow_samples"] = 0
            else:
                st["samples"] = int(st.get("samples", 0)) + 1
                if pnl > 0:
                    st["wins"] = int(st.get("wins", 0)) + 1
                elif pnl < 0:
                    st["losses"] = int(st.get("losses", 0)) + 1
                st["ema_roi"] = round(0.82 * float(st.get("ema_roi", 0.0)) + 0.18 * roi, 5)
                # Specialized alpha sub-strategies auto-hibernate to SHADOW on sustained losses
                if sk in ("orderflow_lead", "energy_accum", "twap_sniper") and st["samples"] >= 5 and float(st["ema_roi"]) < -0.06:
                    st["status"] = "SHADOW"
                    st["shadow_samples"] = 0
                    st["shadow_ema_roi"] = 0.0

        self._save_state()

    def apply_offline_params(self, params: dict, version_id: str, offline_model=None):
        self.active_version_id = version_id
        self.offline_model = offline_model
        if "ema_period" in params:
            self.state["ema_period"] = int(params["ema_period"])
        if "hl_window" in params:
            self.state["hl_window"] = int(params["hl_window"])
        if "atr_min_bps" in params:
            self.state["atr_min_bps"] = float(params["atr_min_bps"])
        if "prop_tp_ratio" in params and "auto_params" in self.state:
            self.state["auto_params"]["prop_tp_ratio"] = round(max(0.35, min(0.65, float(params["prop_tp_ratio"]))), 3)
        self._save_state()

    def get_dashboard_summary(self) -> dict:
        return {
            "active_version_id": self.active_version_id,
            "updates_count": self.state.get("updates_count", 0),
            "rolling_brier": self.get_rolling_brier(),
            "ema_period": self.state.get("ema_period", 13),
            "hl_window": self.state.get("hl_window", 5),
            "atr_min_bps": self.state.get("atr_min_bps", 0.6),
            "feature_weights": [
                {"name": n, "weight": round(w, 4)}
                for n, w in zip(FEATURE_NAMES, self.state["weights"])
            ],
            "time_buckets": self.state["time_buckets"],
            "arms": self.state["arms"],
            "edge_curve": self.state.get("edge_curve", self._default_state()["edge_curve"]),
            "auto_params": self.get_auto_params(),
            "exit_arms": self.state.get("exit_arms", self._default_state()["exit_arms"]),
            "best_exit_arm": self.get_best_exit_arm_id(),
            "sub_strategies": self.state.get("sub_strategies", self._default_state()["sub_strategies"]),
        }
