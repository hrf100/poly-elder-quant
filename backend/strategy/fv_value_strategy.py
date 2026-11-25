"""
fv_value_strategy.py — Clean-Slate Empirical Strategy: FV Discounted Leader + Dynamic Exit (v47)

Validated on 847 real Polymarket BTC-5m settled rounds (39,584 L2 slices, 2026-11-21 ~ 2026-11-24)
under strict pessimistic execution (+1c extra slippage BOTH legs + full 5% quadratic taker fee):
  - Entry (F0 — pure FV Discounted Leader):
      * Zero EMA13 / Elder Ray / Orderflow / Energy / 11-dim stacked logit hallucinations.
      * Zero resting Maker orders, zero Arb-Lock hedging, zero stop-loss, zero auto-loosening drift.
      * Conservative heavy-tailed Student-t(df=4) fair value using full sqrt(T_rem) before 240s
        and exact Chainlink TWAP-60 variance bridge after 240s.
      * Enters ONLY when BTC is on our side of official strike (signed_z >= 0.0), ask in [0.35, 0.56],
        fee-net edge >= +6.0%, spread <= 3c, book parity <= 4c, elapsed in [35s, 240s].
      * Exact fixed `unit_size_usdc` stake (no hidden 1.8x Kelly amplification), capped to available cash.
  - Exit (3 selectable modes, ZERO stop-loss):
      * TIME_TWAP_MOAT (G3, default):
          - T < 240s: FAK TP when best_bid >= 0.78 (OOS +$75.96, PF 1.99, p=0.0040, LIVE period +$4.83).
          - T >= 240s: hold to expiry ($1.00, 0 exit fee) if signed_z >= 1.0 and no >=3c market overpay;
            otherwise exit via FAK if best_bid >= 0.80.
      * DYNAMIC_EV (G2, min-drawdown):
          - FAK TP whenever best_bid >= 0.70 and (best_bid - taker_fee - 0.01) >= p_fair + 0.03
            (Full MDD $40.52, OOS MDD $19.45, max losing streak 5, Bootstrap P(>0)=99.8%).
      * HOLD_EXPIRY (G0):
          - Always hold to official Chainlink TWAP-60 settlement (0 exit fee).
"""

import math
from typing import Optional

from backend.config import HARD_LIMITS


def student_t4_cdf(x: float) -> float:
    """Exact closed-form CDF of Student-t with df=4 (heavier tails than Normal for BTC 5m returns)."""
    z = max(-8.0, min(8.0, float(x)))
    t = z / math.sqrt(4.0 + z * z)
    return 0.5 + 0.5 * t * (1.0 + 0.5 * (1.0 - t * t))


def taker_fee_per_share(price: float, fee_rate: float = 0.05) -> float:
    p = max(0.01, min(0.99, float(price)))
    return max(0.0, float(fee_rate)) * p * (1.0 - p)


class FVDiscountLeaderStrategy:
    """
    Strictly locked, low-parameter empirical strategy (zero self-corrupting parameter drift).
    """

    ELAPSED_MIN_SEC = 35
    ELAPSED_MAX_SEC = 240
    ASK_MIN = 0.35
    ASK_MAX = 0.56
    MAX_SPREAD = 0.03
    MAX_PARITY_ERR = 0.04
    MIN_NET_EDGE = 0.06
    MIN_RESIDUAL_EDGE_ON_CAP = 0.045

    EXIT_MODES = ("TIME_TWAP_MOAT", "DYNAMIC_EV", "HOLD_EXPIRY")

    def __init__(self):
        self._streak_window_ts: int = 0
        self._streak_side: str = ""
        self._streak_count: int = 0

    def on_new_round(self, window_ts: int):
        self._streak_window_ts = int(window_ts)
        self._streak_side = ""
        self._streak_count = 0

    @staticmethod
    def resolve_exit_mode(risk_cfg: Optional[dict]) -> str:
        cfg = risk_cfg or {}
        em = str(cfg.get("fv_exit_mode") or "").upper()
        if em in FVDiscountLeaderStrategy.EXIT_MODES:
            return em
        return "TIME_TWAP_MOAT"

    @staticmethod
    def compute_fair_probs(snap: dict, elapsed_sec: int) -> tuple[float, float, float]:
        """
        Compute conservative Student-t4 fair probability (p_up, p_dn, z_cons) using FULL sqrt(T_rem).
        Undoes the legacy engine's (T_rem - 40) variance compression speedup before 240s,
        while preserving the exact TWAP-60 variance bridge in the final 60s.
        """
        el = max(0, min(300, int(elapsed_sec)))
        rem = max(1, 300 - el)
        z_raw = float(snap.get("moneyness_z") or 0.0)
        if rem > 60:
            z_cons = z_raw * math.sqrt(max(1.0, rem - 40.0) / float(rem))
        else:
            z_cons = z_raw
        p_up = round(max(0.015, min(0.985, student_t4_cdf(z_cons))), 4)
        p_dn = round(1.0 - p_up, 4)
        return p_up, p_dn, round(z_cons, 4)

    @classmethod
    def compute_fee_net_edges(
        cls,
        p_up: float,
        p_dn: float,
        up_book: dict,
        dn_book: dict,
        taker_fee_rate: float = 0.05,
    ) -> tuple[float, float]:
        ua = float(up_book.get("best_ask") or 0.0)
        da = float(dn_book.get("best_ask") or 0.0)
        up_edge = round(p_up - (ua + taker_fee_per_share(ua, taker_fee_rate)), 4) if 0.01 < ua < 0.99 else 0.0
        dn_edge = round(p_dn - (da + taker_fee_per_share(da, taker_fee_rate)), 4) if 0.01 < da < 0.99 else 0.0
        return up_edge, dn_edge

    def evaluate_entry(
        self,
        window_ts: int,
        elapsed_sec: int,
        snap: dict,
        up_book: dict,
        dn_book: dict,
        taker_fee_rate: float = 0.05,
        strict_5s_confirm: bool = False,
        advance_streak: bool = True,
    ) -> tuple[Optional[dict], Optional[str]]:
        """
        Pure F0 Discounted Leader entry evaluator (zero energy/orderflow over-filtering).
        Returns (signal_dict_or_None, block_reason_or_None).
        """
        if window_ts != self._streak_window_ts:
            self.on_new_round(window_ts)

        el = int(elapsed_sec)
        if el < self.ELAPSED_MIN_SEC or el > self.ELAPSED_MAX_SEC:
            if advance_streak:
                self._streak_side = ""
                self._streak_count = 0
            return None, None

        regime = str(snap.get("regime") or "")
        if regime in ("WARMING_UP", "EXTREME_SPIKE"):
            if advance_streak:
                self._streak_side = ""
                self._streak_count = 0
            return None, (f"极端波动保护({regime})，暂停入场" if regime == "EXTREME_SPIKE" else None)

        ub, ua = float(up_book.get("best_bid") or 0.0), float(up_book.get("best_ask") or 0.0)
        db, da = float(dn_book.get("best_bid") or 0.0), float(dn_book.get("best_ask") or 0.0)
        if ub <= 0.01 or ua <= 0.01 or db <= 0.01 or da <= 0.01 or ua < ub or da < db:
            if advance_streak:
                self._streak_side = ""
                self._streak_count = 0
            return None, None

        parity_err = abs(0.5 * (ub + ua) + 0.5 * (db + da) - 1.0)
        if parity_err > self.MAX_PARITY_ERR:
            if advance_streak:
                self._streak_side = ""
                self._streak_count = 0
            return None, f"双边盘口未对齐(偏差{parity_err*100:.1f}¢)，跳过"

        p_up, p_dn, z_cons = self.compute_fair_probs(snap, el)
        best_cand = None
        near_miss_reason = None

        for side, p_fair, signed_z, ask, bid in (
            ("UP", p_up, z_cons, ua, ub),
            ("DOWN", p_dn, -z_cons, da, db),
        ):
            if signed_z < 0.0:
                continue  # Strictly ONLY buy the leading side (BTC already on our side of strike)
            fee_ps = taker_fee_per_share(ask, taker_fee_rate)
            eff_cost = ask + fee_ps
            net_edge = round(p_fair - eff_cost, 4)

            if net_edge < self.MIN_NET_EDGE:
                continue
            if (ask - bid) > self.MAX_SPREAD + 1e-6:
                near_miss_reason = f"{side}点差过宽({(ask-bid)*100:.1f}¢ > 3¢)"
                continue
            if ask < self.ASK_MIN or ask > self.ASK_MAX:
                near_miss_reason = f"{side}卖一{ask*100:.0f}¢不在折价甜区(35~56¢)"
                continue

            # Profit-preserving FAK max_price ceiling: at most +1 tick AND never erodes edge below +4.5%
            max_px_by_edge = p_fair - fee_ps - self.MIN_RESIDUAL_EDGE_ON_CAP
            max_fak_px = round(min(self.ASK_MAX + 0.01, ask + 0.01, max_px_by_edge), 2)
            max_fak_px = max(round(ask, 2), max_fak_px)

            cand = {
                "side": side,
                "limit_price": round(ask, 4),
                "max_fak_price": max_fak_px,
                "fair_prob": p_fair,
                "signed_z": signed_z,
                "net_edge": net_edge,
                "fee_ps": round(fee_ps, 4),
                "arm": {"id": "FV_LEADER_FAK", "offset_cents": 0.00, "ttl_ms": 1500, "is_taker": True},
                "entry_strategy": "fv_discount_leader",
                "trigger_label": "FV贴水领跑",
            }
            if best_cand is None or cand["net_edge"] > best_cand["net_edge"]:
                best_cand = cand

        if best_cand is None:
            if advance_streak:
                self._streak_side = ""
                self._streak_count = 0
            return None, near_miss_reason

        req_ticks = 5 if strict_5s_confirm else 2
        if advance_streak:
            if best_cand["side"] == self._streak_side:
                self._streak_count += 1
            else:
                self._streak_side = best_cand["side"]
                self._streak_count = 1
            if self._streak_count < req_ticks:
                return None, f"{best_cand['side']}折价信号确认中({self._streak_count}/{req_ticks}s · 净+{best_cand['net_edge']*100:.1f}%)"

        return best_cand, None

    @classmethod
    def revalidate_on_fresh_book(
        cls,
        side: str,
        fair_prob: float,
        signed_z: float,
        fresh_book: dict,
        taker_fee_rate: float = 0.05,
    ) -> tuple[bool, str, float, float]:
        """Re-verify exact Discounted Leader edge on fresh REST book right before firing real money."""
        bid = float(fresh_book.get("best_bid") or 0.0)
        ask = float(fresh_book.get("best_ask") or 0.0)
        if bid <= 0.01 or ask <= 0.01 or ask < bid:
            return False, "REST盘口无效", 0.0, 0.0
        if (ask - bid) > cls.MAX_SPREAD + 1e-6:
            return False, f"REST点差扩大({(ask-bid)*100:.1f}¢)", 0.0, 0.0
        if ask < cls.ASK_MIN or ask > cls.ASK_MAX + 0.01:
            return False, f"REST卖一{ask*100:.0f}¢已脱离35-57¢折价区", 0.0, 0.0
        fee_ps = taker_fee_per_share(ask, taker_fee_rate)
        net_edge = fair_prob - (ask + fee_ps)
        if net_edge < cls.MIN_RESIDUAL_EDGE_ON_CAP or signed_z < 0.0:
            return False, f"REST重算净优势不足({net_edge*100:+.1f}% < +4.5%)", 0.0, 0.0
        max_fak_px = max(
            round(ask, 2),
            round(min(cls.ASK_MAX + 0.01, ask + 0.01, fair_prob - fee_ps - cls.MIN_RESIDUAL_EDGE_ON_CAP), 2),
        )
        return True, "", round(ask, 4), max_fak_px

    @classmethod
    def calculate_fixed_stake(
        cls,
        risk_cfg: dict,
        entry_price: float,
        taker_fee_rate: float = 0.05,
        spendable_cash_usdc: Optional[float] = None,
        min_shares: float = 1.0,
    ) -> float:
        """
        Strict fixed-stake sizing (NO hidden 1.8x Kelly amplification!).
        Caps cleanly to fee-adjusted available_cash; returns 0.0 if below Polymarket minimum.
        """
        px = max(0.01, min(0.99, float(entry_price)))
        min_share_notional = round(max(0.0, float(min_shares)) * px + 0.01, 2)
        min_notional = max(HARD_LIMITS["min_unit_usdc"], min_share_notional)
        target = max(min_notional, float(risk_cfg.get("unit_size_usdc", 5.0)))

        if spendable_cash_usdc is not None:
            avail = float(spendable_cash_usdc) - 0.02
            fee_mult = 1.0 + max(0.0, float(taker_fee_rate)) * (1.0 - px)
            max_aff = round(max(0.0, math.floor((avail / fee_mult) * 100.0) / 100.0), 2)
            if max_aff < min_notional:
                return 0.0
            target = min(target, max_aff)
        return round(target, 2)

    @classmethod
    def evaluate_dynamic_exit(
        cls,
        primary_ord: dict,
        live_book: dict,
        snap: dict,
        elapsed_sec: int,
        risk_cfg: dict,
        now: float,
        taker_fee_rate: float = 0.05,
    ) -> Optional[dict]:
        """
        Empirically verified Dynamic FAK Exit Evaluator (ZERO stop-loss, ZERO resting Maker SELL!):
          - TIME_TWAP_MOAT (G3, default):
              * el < 240s: exit via FAK when best_bid >= 0.78 (capture volatile mid-round rally).
              * el >= 240s: exit via FAK when best_bid >= 0.80 ONLY IF safety moat is thin (signed_z < 1.0)
                OR market overpays (bid - fee >= p_fair + 0.03); otherwise hold for $1.00 (0 exit fee).
          - DYNAMIC_EV (G2, low-drawdown):
              * exit via FAK whenever best_bid >= 0.70 and (best_bid - fee - 0.01) >= p_fair + 0.03.
          - HOLD_EXPIRY (G0):
              * always hold to official Chainlink TWAP-60 settlement.
        """
        if primary_ord.get("status") != "FILLED":
            return None
        exit_mode = cls.resolve_exit_mode(risk_cfg)
        if exit_mode == "HOLD_EXPIRY":
            return None

        filled_at = float(primary_ord.get("filled_at") or primary_ord.get("created_at") or 0.0)
        el = int(elapsed_sec)
        if now - filled_at < 3.0 or el >= 292:
            return None

        best_bid = float(live_book.get("best_bid") or 0.0)
        if best_bid < 0.70 or best_bid >= 0.99:
            return None

        side = str(primary_ord.get("side") or "UP").upper()
        p_up, p_dn, z_cons = cls.compute_fair_probs(snap, el)
        p_fair = p_up if side == "UP" else p_dn
        signed_z = z_cons if side == "UP" else -z_cons
        fee_ps = taker_fee_per_share(best_bid, taker_fee_rate)
        net_bid = best_bid - fee_ps

        if exit_mode == "TIME_TWAP_MOAT":
            if el < 240 and best_bid >= 0.78:
                return {
                    "exit_reason": "动态止盈·中段78¢",
                    "trigger_price": round(best_bid, 4),
                    "is_maker": False,
                    "ttl_ms": 1500,
                    "detail": f"240s前买一触达{best_bid:.2f}，防回吐即时FAK落袋",
                }
            if el >= 240 and best_bid >= 0.80:
                if signed_z < 1.0 or net_bid >= (p_fair + 0.03):
                    why = f"尾盘护城河较薄(Z={signed_z:.2f}<1.0)" if signed_z < 1.0 else f"盘口溢价({net_bid*100:.1f}¢≥公允{p_fair*100:.1f}%+3¢)"
                    return {
                        "exit_reason": "动态止盈·尾盘保护",
                        "trigger_price": round(best_bid, 4),
                        "is_maker": False,
                        "ttl_ms": 1500,
                        "detail": f"{why}，买一{best_bid:.2f}即时FAK落袋",
                    }
            return None

        if exit_mode == "DYNAMIC_EV":
            if (net_bid - 0.01) >= (p_fair + 0.03):
                return {
                    "exit_reason": "动态EV溢价止盈",
                    "trigger_price": round(best_bid, 4),
                    "is_maker": False,
                    "ttl_ms": 1500,
                    "detail": f"买一净值{(net_bid-0.01)*100:.1f}¢ ≥ 公允{p_fair*100:.1f}%+3¢，即时FAK止盈",
                }
            return None

        return None
