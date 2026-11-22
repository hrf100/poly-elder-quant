import math
from collections import deque
from statistics import NormalDist, median
from typing import Optional

_NORM = NormalDist()


def _norm_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _norm_pdf(z: float) -> float:
    return math.exp(-0.5 * z * z) / math.sqrt(2.0 * math.pi)


class QuantElderFeatureEngine:
    """
    Quant Elder High-Frequency Momentum + Binary Option Time-Distance & IV/RV Mismatch Engine (v10):
    - Computes dimensionless EMA13 / Bull / Bear / Net Power (bps), ATR, and Kaufman Path Efficiency (ER).
    - Models exact impact of (Current Price - Target Price) AND Remaining Time (T_rem) under Polymarket's 60s TWAP rule.
    - Closed-form inverts Polymarket L2 orderbook (bid/ask/mid) into 1s Implied Volatility (IV in bps/s)
      and compares against Binance Realized Volatility (RV) + rolling VRP baseline median to detect
      volatility mispricing (IV_OVERPRICED / IV_UNDERPRICED / DRIFT_INVERSION).
    """

    def __init__(self, ema_period: int = 13, hl_window: int = 5, atr_window: int = 20):
        self.ema_period = ema_period
        self.hl_window = hl_window
        self.atr_window = atr_window

        # 1-second sampled spot prices & returns (rolling 180s)
        self.sec_prices: deque[float] = deque(maxlen=180)
        self.sec_abs_returns: deque[float] = deque(maxlen=180)
        self.sec_signed_returns: deque[float] = deque(maxlen=180)
        self.ema_val: Optional[float] = None
        self.prev_ema_val: Optional[float] = None

        # Cross-round Implied Volatility EWMA & Rolling VRP (IV/RV) baseline
        self.iv_1s_ema: float = 0.0
        self.vrp_ratio_history: deque[float] = deque(maxlen=400)

        # v19 Sustained Energy Accumulation state (continuous across rounds)
        self.energy_ema: float = 0.0
        self.recent_net_powers: deque[float] = deque(maxlen=20)
        # v20 Rolling 10-minute ATR baseline for session-adaptive volatility gating
        self.atr_baseline_hist: deque[float] = deque(maxlen=600)

        # Round-specific trajectory state
        self.active_window_ts: int = 0
        self.round_prices: list[float] = []
        self.round_open_price: float = 0.0
        self._round_max_p: float = 0.0
        self._round_min_p: float = 0.0
        self._round_total_path: float = 0.0
        self._atr_med_tick: int = 0
        self._cached_atr_base: float = 1.0
        self._vrp_med_tick: int = 0
        self._cached_vrp_base: float = 1.15
        self.final_60s_oracle_prices: list[float] = []

        self.latest_snapshot: dict = {
            "ema13": 0.0,
            "ema13_slope_bps": 0.0,
            "bull_power_bps": 0.0,
            "bear_power_bps": 0.0,
            "net_power_bps": 0.0,
            "atr_bps": 1.5,
            "atr_baseline_bps": 1.2,
            "vol_regime_ratio": 1.0,
            "sigma_1s_bps": 1.0,
            "sigma_rem_bps": 16.0,
            "remaining_sec": 300,
            "path_efficiency": 0.5,
            "mfe_bps": 0.0,
            "mae_bps": 0.0,
            "dist_to_strike_bps": 0.0,
            "dist_to_strike_usd": 0.0,
            "eff_dist_bps": 0.0,
            "dist_to_strike_atr": 0.0,
            "moneyness_z": 0.0,
            "bs_up_prob": 0.5,
            "delta_10bps": 0.25,
            "required_flip_bps": 0.0,
            "locked_twap_ratio": 0.0,
            # v10 IV / RV metrics
            "iv_valid": False,
            "iv_1s_bps": 0.0,
            "iv_1s_ema": 0.0,
            "iv_low_bps": 0.0,
            "iv_high_bps": 0.0,
            "iv_rv_ratio": 1.0,
            "vrp_baseline": 1.15,
            "rel_iv_rv": 1.0,
            "log_iv_rv": 0.0,
            "iv_state": "ATM_CARRY",
            # v14 Leading Orderflow & Final-60s TWAP Sniper features
            "cvd_3s_imbalance": 0.0,
            "cvd_10s_imbalance": 0.0,
            "cvd_3s_usd": 0.0,
            "vol_3s_usd": 0.0,
            "vol_3s_baseline_usd": 60000.0,
            "vol_3s_rel": 1.0,
            "adaptive_cvd_3s_min_usd": 65000.0,
            "perp_basis_jump_bps": 0.0,
            "clob_liquidity_pull": 0.0,
            "orderflow_score": 0.0,
            "energy_accum_raw": 0.0,
            "energy_consistency": 0.0,
            "energy_accum_score": 0.0,
            "final_60s_count": 0,
            "locked_avg_diff_bps": 0.0,
            "spot_vs_locked_diverged": False,
            "is_outlier_vol": False,
            "regime": "WARMING_UP",
        }

    def set_hyperparams(self, ema_period: int = 13, hl_window: int = 5):
        self.ema_period = max(5, min(30, int(ema_period)))
        self.hl_window = max(3, min(15, int(hl_window)))

    def on_new_round(self, window_ts: int, open_price: float):
        self.active_window_ts = window_ts
        self.round_prices = [open_price] if open_price > 0 else []
        self.round_open_price = open_price
        self._round_max_p = open_price if open_price > 0 else 0.0
        self._round_min_p = open_price if open_price > 0 else 0.0
        self._round_total_path = 0.0
        self.final_60s_oracle_prices = []

    def _compute_iv_metrics(
        self,
        eff_dist_bps: float,
        remaining_sec: int,
        rv_1s_bps: float,
        up_bid: float,
        up_ask: float,
    ) -> dict:
        """
        Closed-form inversion of Polymarket L2 UP bid/ask into 1s Implied Volatility (bps/s),
        strictly aligned with Polymarket's 60s TWAP variance bridge.
        v24 Fix:
        - Disables raw inversion inside final 65s where sqrt_t_eff collapses (prevents 11x+ IV explosions).
        - Clamps single-tick IV & smoothly decays iv_1s_ema toward (RV * VRP) during ATM/late carry states.
        """
        self._vrp_med_tick += 1
        if len(self.vrp_ratio_history) >= 20:
            if self._vrp_med_tick % 10 == 1:
                self._cached_vrp_base = max(0.85, min(1.65, round(float(median(self.vrp_ratio_history)), 3)))
            vrp_base = self._cached_vrp_base
        else:
            vrp_base = 1.15
        rv_safe = max(0.35, float(rv_1s_bps))

        def _carry(state_label: str) -> dict:
            target_iv = max(0.30, min(6.0, rv_safe * vrp_base))
            if self.iv_1s_ema <= 0:
                self.iv_1s_ema = target_iv
            else:
                # Smoothly decay stale cross-round IV spikes back toward current RV * VRP baseline
                self.iv_1s_ema = max(0.25, min(6.0, 0.97 * self.iv_1s_ema + 0.03 * target_iv))
            iv_est = min(self.iv_1s_ema, rv_safe * 4.0)
            ratio = max(0.25, min(4.0, iv_est / rv_safe))
            rel = ratio / vrp_base
            return {
                "iv_valid": False,
                "iv_1s_bps": round(iv_est, 3),
                "iv_1s_ema": round(self.iv_1s_ema, 3),
                "iv_low_bps": round(iv_est, 3),
                "iv_high_bps": round(iv_est, 3),
                "iv_rv_ratio": round(ratio, 3),
                "vrp_baseline": vrp_base,
                "rel_iv_rv": round(rel, 3),
                "log_iv_rv": round(math.log(max(0.35, min(3.0, rel))), 4),
                "iv_state": state_label,
            }

        # Guard 1: Only invert IV when remaining_sec >= 65 (before final-60s TWAP denominator collapse) & spread tight
        if remaining_sec < 65 or up_bid <= 0.01 or up_ask <= 0.01 or up_ask < up_bid or (up_ask - up_bid) > 0.045:
            return _carry("SPREAD_OR_LATE")

        mid = 0.5 * (up_bid + up_ask)

        # Guard 2: Filter ATM division-by-zero singularity & extreme 1-cent tick noise
        if abs(eff_dist_bps) < 4.0 or (0.43 < mid < 0.57) or mid < 0.14 or mid > 0.86:
            return _carry("ATM_CARRY")

        # Guard 3: Sign inversion between basis-aligned spot lead and market mid -> pure drift/latency mispricing
        if (eff_dist_bps > 0 and mid < 0.50) or (eff_dist_bps < 0 and mid > 0.50):
            return _carry("DRIFT_INVERSION")

        # Effective remaining time sqrt factor under 60s TWAP rule (remaining_sec >= 65 => sqrt_t_eff >= 5.0)
        sqrt_t_eff = math.sqrt(max(25.0, remaining_sec - 40.0))

        def _inv_1s(prob: float) -> float:
            p_clamped = max(0.02, min(0.98, prob))
            z = _NORM.inv_cdf(p_clamped)
            if abs(z) < 0.05:
                return rv_safe
            return max(0.25, min(6.0, abs(eff_dist_bps / (z * sqrt_t_eff))))

        iv_mid_raw = _inv_1s(mid)
        iv_mid = min(iv_mid_raw, rv_safe * 4.0)
        iv_a = min(_inv_1s(up_bid), rv_safe * 4.0)
        iv_b = min(_inv_1s(up_ask), rv_safe * 4.0)
        iv_low, iv_high = min(iv_a, iv_b), max(iv_a, iv_b)

        # Update cross-round EWMA IV with outlier cap & rolling VRP ratio baseline
        if self.iv_1s_ema <= 0:
            self.iv_1s_ema = iv_mid
        else:
            self.iv_1s_ema = max(0.25, min(6.0, 0.85 * self.iv_1s_ema + 0.15 * iv_mid))

        raw_ratio = max(0.25, min(4.0, iv_mid / rv_safe))
        if 0.35 <= raw_ratio <= 3.5:
            self.vrp_ratio_history.append(raw_ratio)
        vrp_base = round(float(median(self.vrp_ratio_history)), 3) if len(self.vrp_ratio_history) >= 20 else 1.15
        vrp_base = max(0.85, min(1.65, vrp_base))
        rel_iv_rv = raw_ratio / vrp_base
        log_rel = math.log(max(0.35, min(3.0, rel_iv_rv)))

        # Corridor-verified mispricing classification:
        # Must break outside the bid/ask IV corridor AND deviate meaningfully from rolling VRP baseline
        iv_state = "FAIR"
        if rv_safe < iv_low * 0.85 and rel_iv_rv >= 1.22:
            iv_state = "IV_OVERPRICED"    # Panic vol premium -> leading (ITM) side underpriced
        elif rv_safe > iv_high * 1.12 and rel_iv_rv <= 0.84:
            iv_state = "IV_UNDERPRICED"   # Market lagging Binance RV surge -> ITM overpriced / OTM cheap

        return {
            "iv_valid": True,
            "iv_1s_bps": round(iv_mid, 3),
            "iv_1s_ema": round(self.iv_1s_ema, 3),
            "iv_low_bps": round(iv_low, 3),
            "iv_high_bps": round(iv_high, 3),
            "iv_rv_ratio": round(raw_ratio, 3),
            "vrp_baseline": vrp_base,
            "rel_iv_rv": round(rel_iv_rv, 3),
            "log_iv_rv": round(log_rel, 4),
            "iv_state": iv_state,
        }

    def update_1s(
        self,
        spot_price: float,
        strike_price: float,
        twap60_price: float,
        basis_offset: float,
        elapsed_sec: int,
        up_bid: float = 0.0,
        up_ask: float = 0.0,
        orderflow: Optional[dict] = None,
        oracle_spot: float = 0.0,
        clob_pull: float = 0.0,
    ) -> dict:
        if spot_price <= 0:
            return self.latest_snapshot

        elapsed_sec = max(0, min(300, int(elapsed_sec)))
        remaining_sec = max(1, 300 - elapsed_sec)

        if self.sec_prices:
            prev_p = self.sec_prices[-1]
            ret_bps = (spot_price - prev_p) / prev_p * 10000.0
            self.sec_signed_returns.append(ret_bps)
            self.sec_abs_returns.append(abs(ret_bps))
        self.sec_prices.append(spot_price)

        if self.round_open_price <= 0:
            self.round_open_price = spot_price
        if self.round_prices:
            self._round_total_path += abs(spot_price - self.round_prices[-1])
            if spot_price > self._round_max_p:
                self._round_max_p = spot_price
            if spot_price < self._round_min_p:
                self._round_min_p = spot_price
        else:
            self._round_max_p = spot_price
            self._round_min_p = spot_price
            self._round_total_path = 0.0
        self.round_prices.append(spot_price)

        # 1. EMA calculation
        k = 2.0 / (self.ema_period + 1.0)
        if self.ema_val is None:
            self.ema_val = spot_price
            self.prev_ema_val = spot_price
        else:
            self.prev_ema_val = self.ema_val
            self.ema_val = spot_price * k + self.ema_val * (1.0 - k)

        ema_slope_bps = (self.ema_val - self.prev_ema_val) / self.ema_val * 10000.0 if self.ema_val else 0.0

        # 2. Dimensionless Elder Ray (in bps relative to EMA13)
        recent = list(self.sec_prices)[-self.hl_window:]
        high_n = max(recent)
        low_n = min(recent)
        bull_power_bps = (high_n - self.ema_val) / self.ema_val * 10000.0
        bear_power_bps = (low_n - self.ema_val) / self.ema_val * 10000.0
        net_power_bps = bull_power_bps + bear_power_bps

        # 3. Realized 1s ATR, 10m Adaptive ATR Baseline & RMS 1s Volatility + Black-Swan Detector
        atr_slice = list(self.sec_abs_returns)[-self.atr_window:]
        atr_bps = max(0.20, sum(atr_slice) / len(atr_slice)) if atr_slice else 1.0
        self.atr_baseline_hist.append(atr_bps)
        self._atr_med_tick += 1
        if len(self.atr_baseline_hist) >= 20:
            if self._atr_med_tick % 10 == 1 or len(self.atr_baseline_hist) <= 30:
                self._cached_atr_base = max(0.25, min(8.0, float(median(self.atr_baseline_hist))))
            atr_baseline_bps = self._cached_atr_base
        else:
            atr_baseline_bps = max(0.35, min(8.0, atr_bps))
        vol_regime_ratio = atr_bps / max(0.25, atr_baseline_bps)
        long_vol = sum(self.sec_abs_returns) / len(self.sec_abs_returns) if len(self.sec_abs_returns) >= 30 else atr_bps
        is_outlier_vol = (len(self.sec_abs_returns) >= 30 and atr_bps > max(6.0, long_vol * 4.0))

        if len(self.sec_signed_returns) >= 10:
            short_r = list(self.sec_signed_returns)[-25:]
            med_r = list(self.sec_signed_returns)[-120:]
            v_short = sum(r * r for r in short_r) / len(short_r)
            v_med = sum(r * r for r in med_r) / len(med_r)
            sigma_1s_bps = math.sqrt(0.45 * v_short + 0.55 * v_med)
            sigma_1s_bps = max(0.45, min(20.0, sigma_1s_bps))
        else:
            sigma_1s_bps = max(0.8, atr_bps * 1.25)

        # 4. Kaufman Path Efficiency Ratio (O(1) incremental)
        if len(self.round_prices) >= 6:
            net_move = abs(self.round_prices[-1] - self.round_prices[0])
            total_path = self._round_total_path
            path_efficiency = min(1.0, net_move / total_path) if total_path > 1e-9 else 0.0
        else:
            path_efficiency = 0.5

        # 5. Basis-aligned oracle spot & Distance-to-Strike (O(1) MFE/MAE)
        ref_strike = strike_price if strike_price > 0 else self.round_open_price
        oracle_equiv_spot = spot_price - basis_offset
        dist_to_strike_usd = (oracle_equiv_spot - ref_strike) if ref_strike > 0 else 0.0
        dist_to_strike_bps = (dist_to_strike_usd / ref_strike * 10000.0) if ref_strike > 0 else 0.0
        dist_to_strike_atr = dist_to_strike_bps / max(0.5, atr_bps)

        if ref_strike > 0 and self.round_prices:
            mfe_bps = ((self._round_max_p - basis_offset) - ref_strike) / ref_strike * 10000.0
            mae_bps = ((self._round_min_p - basis_offset) - ref_strike) / ref_strike * 10000.0
        else:
            mfe_bps, mae_bps = 0.0, 0.0

        # 6. Exact 60s TWAP Settlement Variance Bridge & Moneyness Z-score
        # Prefer direct official Chainlink oracle_spot inside final 60s for exact TWAP lock accuracy
        twap_sample_spot = oracle_spot if oracle_spot > 0 else oracle_equiv_spot
        locked_twap_ratio = max(0.0, min(1.0, (elapsed_sec - 240) / 60.0))
        if elapsed_sec >= 240 and twap_sample_spot > 0:
            self.final_60s_oracle_prices.append(twap_sample_spot)

        locked_avg_diff_bps = 0.0
        spot_vs_locked_diverged = False
        if remaining_sec > 60:
            eff_dist_bps = dist_to_strike_bps
            sigma_rem_bps = max(0.5, sigma_1s_bps * math.sqrt(max(1.0, remaining_sec - 40.0)))
            required_flip_bps = -dist_to_strike_bps
        else:
            w_locked = (60.0 - remaining_sec) / 60.0
            w_rem = remaining_sec / 60.0
            if self.final_60s_oracle_prices:
                locked_avg = sum(self.final_60s_oracle_prices) / len(self.final_60s_oracle_prices)
            else:
                locked_avg = twap60_price if twap60_price > 0 else twap_sample_spot

            expected_terminal_twap = w_locked * locked_avg + w_rem * twap_sample_spot
            eff_dist_bps = ((expected_terminal_twap - ref_strike) / ref_strike * 10000.0) if ref_strike > 0 else 0.0
            locked_avg_diff_bps = ((locked_avg - ref_strike) / ref_strike * 10000.0) if ref_strike > 0 else 0.0
            sigma_rem_bps = max(0.25, w_rem * sigma_1s_bps * math.sqrt(remaining_sec / 3.0))

            # True retail trap: live spot spiked onto opposite side of strike from already-locked 60s TWAP!
            if (locked_avg_diff_bps < -2.0 and dist_to_strike_bps > 1.0) or (locked_avg_diff_bps > 2.0 and dist_to_strike_bps < -1.0):
                spot_vs_locked_diverged = True

            if ref_strike > 0:
                req_spot = (60.0 * ref_strike - (60.0 - remaining_sec) * locked_avg) / max(1.0, float(remaining_sec))
                required_flip_bps = (req_spot - twap_sample_spot) / ref_strike * 10000.0
            else:
                required_flip_bps = 0.0

        # 6b. v20 Session-Adaptive Leading Orderflow Composite Score
        of = orderflow or {}
        cvd_3s_imb = float(of.get("cvd_3s_imbalance", 0.0))
        cvd_10s_imb = float(of.get("cvd_10s_imbalance", 0.0))
        cvd_3s_usd = float(of.get("cvd_3s_usd", 0.0))
        vol_3s_usd = float(of.get("vol_3s_usd", 0.0))
        vol_10s_usd = float(of.get("vol_10s_usd", 0.0))
        vol_3s_base = max(12000.0, float(of.get("vol_3s_baseline_usd", 60000.0)))
        vol_10s_base = max(35000.0, float(of.get("vol_10s_baseline_usd", 180000.0)))
        vol_3s_rel = float(of.get("vol_3s_rel", vol_3s_usd / vol_3s_base if vol_3s_base > 0 else 0.0))
        adaptive_cvd_3s_min = float(of.get("adaptive_cvd_3s_min_usd", max(25000.0, min(160000.0, 1.35 * vol_3s_base))))
        basis_jump = float(of.get("perp_basis_jump_bps", 0.0))

        # Adaptive volume activity weight relative to rolling 10m baseline (never starves in quiet sessions)
        ref_3s_act = max(25000.0, vol_3s_base * 1.35)
        ref_10s_act = max(70000.0, vol_10s_base * 1.35)
        activity_weight_3s = min(1.0, vol_3s_usd / ref_3s_act) if vol_3s_usd > 0 else 0.0
        activity_weight_10s = min(1.0, vol_10s_usd / ref_10s_act) if vol_10s_usd > 0 else 0.0
        basis_denom = max(0.85, min(2.2, atr_bps * 1.35))
        raw_of = (
            0.50 * (cvd_3s_imb * 2.5 * activity_weight_3s)
            + 0.25 * (cvd_10s_imb * 2.0 * activity_weight_10s)
            + 0.20 * max(-2.5, min(2.5, basis_jump / basis_denom))
            + 0.05 * max(-1.5, min(1.5, clob_pull * 1.5))
        )
        orderflow_score = max(-3.0, min(3.0, raw_of))

        # 6c. v19 Sustained Energy Accumulation Factor (anti-EMA-lag + climax-damped)
        # 1) Decaying ATR-normalized momentum area
        norm_np = max(-3.0, min(3.0, net_power_bps / max(0.45, atr_bps)))
        self.energy_ema = 0.88 * self.energy_ema + norm_np
        self.recent_net_powers.append(net_power_bps)

        # 2) Rolling 20s directional consistency (-1.0 to +1.0)
        pos_cnt = sum(1 for v in self.recent_net_powers if v > 0.15)
        neg_cnt = sum(1 for v in self.recent_net_powers if v < -0.15)
        energy_consistency = (pos_cnt - neg_cnt) / max(1, len(self.recent_net_powers))

        # 3) Anti-EMA-lag real price progress check (filters flat-consolidation ghost bars)
        e_sign = 1.0 if self.energy_ema > 0 else (-1.0 if self.energy_ema < 0 else 0.0)
        if e_sign != 0.0 and len(self.sec_prices) >= 6:
            p_12 = self.sec_prices[-13] if len(self.sec_prices) >= 13 else self.sec_prices[0]
            p_5 = self.sec_prices[-6]
            move_12s_bps = (spot_price - p_12) / spot_price * 10000.0
            move_5s_bps = (spot_price - p_5) / spot_price * 10000.0
            aligned_12s = e_sign * move_12s_bps
            aligned_5s = e_sign * move_5s_bps

            if aligned_12s <= 0.35 * atr_bps:
                progress_mult = 0.22
            elif aligned_5s < -0.50 * atr_bps:
                progress_mult = 0.35
            else:
                progress_mult = 0.50 + 0.50 * min(1.0, aligned_12s / max(1.0, 2.5 * atr_bps))
                if twap60_price > 0 and ref_strike > 0:
                    twap_press_bps = (oracle_equiv_spot - twap60_price) / ref_strike * 10000.0
                    if e_sign * twap_press_bps > 1.0:
                        progress_mult = min(1.15, progress_mult + 0.15)
        else:
            progress_mult = 0.50

        # 4) Climax spike damping (prevents score from spiking on single-second exhaustion wicks)
        last_ret_abs = abs(self.sec_signed_returns[-1]) if self.sec_signed_returns else 0.0
        climax_damp = 0.65 if (abs(net_power_bps) > atr_bps * 3.8 or last_ret_abs > atr_bps * 3.5) else 1.0

        # 5) Final standardized dimensionless score in [-2.5, +2.5]
        raw_energy_scaled = math.tanh(self.energy_ema / 8.0) * 2.5
        consistency_weight = 0.40 + 0.60 * abs(energy_consistency)
        energy_accum_score = max(
            -2.5,
            min(2.5, raw_energy_scaled * consistency_weight * progress_mult * climax_damp),
        )

        moneyness_z = max(-6.0, min(6.0, eff_dist_bps / max(0.25, sigma_rem_bps)))
        bs_up_prob = max(0.005, min(0.995, _norm_cdf(moneyness_z)))
        delta_10bps = min(0.95, (10.0 / max(0.5, sigma_rem_bps)) * _norm_pdf(moneyness_z))

        # 7. Closed-form Polymarket IV inversion & IV/RV mismatch detection
        iv_info = self._compute_iv_metrics(eff_dist_bps, remaining_sec, sigma_1s_bps, up_bid, up_ask)

        # 8. Regime classification
        if len(self.sec_prices) < self.ema_period:
            regime = "WARMING_UP"
        elif is_outlier_vol:
            regime = "EXTREME_SPIKE"
        elif elapsed_sec >= 245:
            regime = "LATE_TWAP_LOCK"
        elif path_efficiency >= 0.38 and abs(net_power_bps) >= atr_bps * 1.2:
            regime = "TRENDING"
        else:
            regime = "CHOPPY"

        self.latest_snapshot = {
            "ema13": round(self.ema_val, 2),
            "ema13_slope_bps": round(ema_slope_bps, 3),
            "bull_power_bps": round(bull_power_bps, 3),
            "bear_power_bps": round(bear_power_bps, 3),
            "net_power_bps": round(net_power_bps, 3),
            "atr_bps": round(atr_bps, 3),
            "atr_baseline_bps": round(atr_baseline_bps, 3),
            "vol_regime_ratio": round(vol_regime_ratio, 2),
            "sigma_1s_bps": round(sigma_1s_bps, 3),
            "sigma_rem_bps": round(sigma_rem_bps, 2),
            "remaining_sec": remaining_sec,
            "path_efficiency": round(path_efficiency, 4),
            "mfe_bps": round(mfe_bps, 3),
            "mae_bps": round(mae_bps, 3),
            "dist_to_strike_bps": round(dist_to_strike_bps, 3),
            "dist_to_strike_usd": round(dist_to_strike_usd, 2),
            "eff_dist_bps": round(eff_dist_bps, 3),
            "dist_to_strike_atr": round(dist_to_strike_atr, 3),
            "moneyness_z": round(moneyness_z, 3),
            "bs_up_prob": round(bs_up_prob, 4),
            "delta_10bps": round(delta_10bps, 3),
            "required_flip_bps": round(required_flip_bps, 2),
            "locked_twap_ratio": round(locked_twap_ratio, 4),
            **iv_info,
            "cvd_3s_imbalance": round(cvd_3s_imb, 3),
            "cvd_10s_imbalance": round(cvd_10s_imb, 3),
            "cvd_3s_usd": round(cvd_3s_usd, 0),
            "vol_3s_usd": round(vol_3s_usd, 0),
            "vol_3s_baseline_usd": round(vol_3s_base, 0),
            "vol_3s_rel": round(vol_3s_rel, 2),
            "adaptive_cvd_3s_min_usd": round(adaptive_cvd_3s_min, 0),
            "perp_basis_jump_bps": round(basis_jump, 2),
            "clob_liquidity_pull": round(clob_pull, 3),
            "orderflow_score": round(orderflow_score, 3),
            "energy_accum_raw": round(self.energy_ema, 3),
            "energy_consistency": round(energy_consistency, 3),
            "energy_accum_score": round(energy_accum_score, 3),
            "final_60s_count": len(self.final_60s_oracle_prices),
            "locked_avg_diff_bps": round(locked_avg_diff_bps, 2),
            "spot_vs_locked_diverged": spot_vs_locked_diverged,
            "is_outlier_vol": is_outlier_vol,
            "regime": regime,
        }
        return self.latest_snapshot
