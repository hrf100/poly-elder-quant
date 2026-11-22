import json
import math
import os
import pickle
import time
from pathlib import Path
from typing import Optional
import numpy as np

from backend.config import MODELS_DIR
from backend.storage.db import Database

TRAIN_FEATURES = [
    "elapsed_sec",
    "remaining_sec",
    "dist_to_strike_bps",
    "moneyness_z",
    "bs_up_prob",
    "sigma_rem_bps",
    "ema13_slope_bps",
    "net_power_bps",
    "bull_power_bps",
    "bear_power_bps",
    "atr_bps",
    "path_efficiency",
    "mfe_bps",
    "mae_bps",
    "locked_twap_ratio",
    "pm_spread",
    "book_imbalance",
    "iv_1s_bps",
    "iv_rv_ratio",
    "log_iv_rv",
    "cvd_3s_imbalance",
    "perp_basis_jump_bps",
    "orderflow_score",
    "clob_liquidity_pull",
    "energy_accum_score",
    "energy_consistency",
]


class OfflineModelWrapper:
    """Unified inference wrapper supporting LightGBM or scikit-learn/NumPy Logistic models."""

    def __init__(self, model_type: str, model_obj, mean: list[float], std: list[float]):
        self.model_type = model_type
        self.model_obj = model_obj
        self.mean = np.array(mean, dtype=np.float64)
        self.std = np.array(std, dtype=np.float64)

    def predict_prob_from_snap(self, snap: dict, book_imbalance: float, elapsed_sec: int = 150, pm_spread: float = 0.02) -> float:
        rem_sec = float(snap.get("remaining_sec", max(1, 300 - int(elapsed_sec))))
        x = np.array([[
            float(elapsed_sec),
            rem_sec,
            float(snap.get("dist_to_strike_bps", 0.0)),
            float(snap.get("moneyness_z", 0.0)),
            float(snap.get("bs_up_prob", 0.5)),
            float(snap.get("sigma_rem_bps", 15.0)),
            float(snap.get("ema13_slope_bps", 0.0)),
            float(snap.get("net_power_bps", 0.0)),
            float(snap.get("bull_power_bps", 0.0)),
            float(snap.get("bear_power_bps", 0.0)),
            float(snap.get("atr_bps", 1.5)),
            float(snap.get("path_efficiency", 0.5)),
            float(snap.get("mfe_bps", 0.0)),
            float(snap.get("mae_bps", 0.0)),
            float(snap.get("locked_twap_ratio", 0.0)),
            float(pm_spread),
            float(book_imbalance),
            float(snap.get("iv_1s_bps", 0.0)),
            float(snap.get("iv_rv_ratio", 1.0)),
            float(snap.get("log_iv_rv", 0.0)),
            float(snap.get("cvd_3s_imbalance", 0.0)),
            float(snap.get("perp_basis_jump_bps", 0.0)),
            float(snap.get("orderflow_score", 0.0)),
            float(snap.get("clob_liquidity_pull", 0.0)),
            float(snap.get("energy_accum_score", 0.0)),
            float(snap.get("energy_consistency", 0.0)),
        ]], dtype=np.float64)
        # Backward compatibility: gracefully slice feature vector if loading a pre-v19 24-feature champion .pkl
        expected_dim = int(self.mean.shape[0]) if hasattr(self, "mean") and self.mean is not None else x.shape[1]
        if expected_dim < x.shape[1]:
            x = x[:, :expected_dim]
        if self.model_type == "lightgbm":
            try:
                return float(self.model_obj.predict(x, num_threads=1)[0])
            except TypeError:
                return float(self.model_obj.predict(x)[0])
        x_norm = (x - self.mean) / np.where(self.std < 1e-6, 1.0, self.std)
        return float(self.model_obj.predict_proba(x_norm)[0, 1])


def _brier_score(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    return float(np.mean((y_prob - y_true) ** 2))


def _roc_auc_simple(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    pos = y_prob[y_true == 1]
    neg = y_prob[y_true == 0]
    if len(pos) == 0 or len(neg) == 0:
        return 0.5
    wins = 0.0
    total = len(pos) * len(neg)
    # Fast Mann-Whitney via ranking
    order = np.argsort(y_prob)
    ranks = np.empty_like(order, dtype=np.float64)
    ranks[order] = np.arange(1, len(y_prob) + 1)
    pos_rank_sum = np.sum(ranks[y_true == 1])
    auc = (pos_rank_sum - len(pos) * (len(pos) + 1) / 2.0) / total
    return float(max(0.0, min(1.0, auc)))


def run_walk_forward_training(db: Database, min_rounds: int = 24) -> dict:
    """
    Executes strict Purged Walk-Forward training grouped by window_ts (ZERO data leakage across slices).
    Designed to run safely inside low-priority background thread/process on 1C1G/2C2G VPS.
    """
    try:
        os.nice(15)
    except Exception:
        pass

    rows = db.get_training_dataset(max_rounds=864)
    if not rows:
        return {"ok": False, "reason": "暂无已结算的整场轨迹数据"}

    # Group strictly by chronological window_ts
    windows = sorted({r["window_ts"] for r in rows})
    if len(windows) < min_rounds:
        return {"ok": False, "reason": f"已结算场次不足 ({len(windows)}/{min_rounds})，继续录制中"}

    # Strict 80% / 20% chronological split by window_ts with 1-round purge gap
    split_idx = max(10, int(len(windows) * 0.8))
    train_windows = set(windows[:split_idx - 1])
    val_windows = set(windows[split_idx:])

    train_rows = [r for r in rows if r["window_ts"] in train_windows]
    val_rows = [r for r in rows if r["window_ts"] in val_windows]
    if len(train_rows) < 50 or len(val_rows) < 20:
        return {"ok": False, "reason": "切片样本量不足"}

    def to_xyw(subset):
        # Normalize sample weight so every 5m round sums to 1.0
        counts = {}
        for r in subset:
            counts[r["window_ts"]] = counts.get(r["window_ts"], 0) + 1
        def _row_feats(r):
            el = float(r.get("elapsed_sec") or 0.0)
            rem = max(1.0, 300.0 - el)
            dist = float(r.get("dist_to_strike_bps") or 0.0)
            atr = max(0.5, float(r.get("atr_bps") or 1.2))
            sig_rem = float(r.get("sigma_rem_bps") or 0.0)
            mz = float(r.get("moneyness_z") or 0.0)
            bsp = float(r.get("bs_up_prob") or 0.0)
            if sig_rem <= 0.0:
                # Derive time-scaled option moneyness for legacy slices seamlessly
                sig_1s = atr * 1.25
                sig_rem = (sig_1s * math.sqrt(max(1.0, rem - 40.0))) if rem > 60 else max(0.25, (rem / 60.0) * sig_1s * math.sqrt(rem / 3.0))
                mz = max(-6.0, min(6.0, dist / max(0.25, sig_rem)))
                bsp = 0.5 * (1.0 + math.erf(mz / math.sqrt(2.0)))
            vals = {
                **r,
                "remaining_sec": rem,
                "sigma_rem_bps": sig_rem,
                "moneyness_z": mz,
                "bs_up_prob": bsp,
                "iv_1s_bps": float(r.get("iv_1s_bps") or 0.0),
                "iv_rv_ratio": float(r.get("iv_rv_ratio") or 1.0),
                "log_iv_rv": float(r.get("log_iv_rv") or 0.0),
                "cvd_3s_imbalance": float(r.get("cvd_3s_imbalance") or 0.0),
                "perp_basis_jump_bps": float(r.get("perp_basis_jump_bps") or 0.0),
                "orderflow_score": float(r.get("orderflow_score") or 0.0),
                "clob_liquidity_pull": float(r.get("clob_liquidity_pull") or 0.0),
                "energy_accum_score": float(r.get("energy_accum_score") or 0.0),
                "energy_consistency": float(r.get("energy_consistency") or 0.0),
            }
            return [float(vals.get(f) or 0.0) for f in TRAIN_FEATURES]

        X = np.array([_row_feats(r) for r in subset], dtype=np.float64)
        y = np.array([int(r["label"]) for r in subset], dtype=np.float64)
        w = np.array([1.0 / counts[r["window_ts"]] for r in subset], dtype=np.float64)
        return X, y, w

    X_tr, y_tr, w_tr = to_xyw(train_rows)
    X_va, y_va, _ = to_xyw(val_rows)

    mean = np.mean(X_tr, axis=0)
    std = np.std(X_tr, axis=0)
    model_type = "logistic"
    model_obj = None
    val_preds = None

    # Prefer LightGBM (single-threaded for low-spec VPS); fallback to sklearn LogisticRegression
    try:
        import lightgbm as lgb
        dtrain = lgb.Dataset(X_tr, label=y_tr, weight=w_tr)
        dval = lgb.Dataset(X_va, label=y_va, reference=dtrain)
        params = {
            "objective": "binary",
            "metric": "binary_logloss",
            "learning_rate": 0.04,
            "num_leaves": 15,
            "max_depth": 4,
            "min_data_in_leaf": 25,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 1,
            "lambda_l2": 5.0,
            "n_jobs": 1,
            "num_threads": 1,
            "verbose": -1,
        }
        bst = lgb.train(params, dtrain, num_boost_round=120, valid_sets=[dval], callbacks=[lgb.early_stopping(15, verbose=False)])
        val_preds = bst.predict(X_va)
        model_type = "lightgbm"
        model_obj = bst
    except Exception:
        from sklearn.linear_model import LogisticRegression
        X_tr_n = (X_tr - mean) / np.where(std < 1e-6, 1.0, std)
        X_va_n = (X_va - mean) / np.where(std < 1e-6, 1.0, std)
        clf = LogisticRegression(C=0.5, max_iter=300)
        clf.fit(X_tr_n, y_tr, sample_weight=w_tr)
        val_preds = clf.predict_proba(X_va_n)[:, 1]
        model_type = "logistic"
        model_obj = clf

    val_brier = round(_brier_score(y_va, val_preds), 5)
    val_auc = round(_roc_auc_simple(y_va, val_preds), 4)

    version_id = f"v_{int(time.time())}_{model_type}"
    artifact_path = str(MODELS_DIR / f"{version_id}.pkl")
    wrapper = OfflineModelWrapper(model_type, model_obj, mean.tolist(), std.tolist())
    with open(artifact_path, "wb") as f:
        pickle.dump(wrapper, f)

    # Check existing champion: auto-promote if first model or validation Brier strictly improves
    existing = db.list_model_versions(limit=10)
    champions = [m for m in existing if m["role"] == "CHAMPION"]
    role = "CHALLENGER"
    if not champions or val_brier < float(champions[0]["val_brier"]):
        role = "CHAMPION"

    # Walk-Forward offline optimization of proportional Maker TP ratio on validation slices
    best_tp_ratio = 0.48
    try:
        val_by_win = {}
        for r in val_rows:
            val_by_win.setdefault(r["window_ts"], []).append(r)
        best_score = -1e9
        for cand_r in (0.38, 0.45, 0.52, 0.60):
            tot_roi = 0.0
            cnt = 0
            for w_ts, w_slices in val_by_win.items():
                w_slices = sorted(w_slices, key=lambda x: x["elapsed_sec"])
                if len(w_slices) < 6:
                    continue
                entry_s = w_slices[2]
                bid_u = float(entry_s.get("pm_up_bid") or 0.0)
                if not (0.25 <= bid_u <= 0.72):
                    continue
                target_px = min(0.95, bid_u + max(0.08, min(0.28, cand_r * (1.0 - bid_u))))
                hit = False
                for fut in w_slices[3:]:
                    if float(fut.get("pm_up_bid") or 0.0) >= target_px:
                        tot_roi += (target_px - bid_u) / bid_u
                        hit = True
                        break
                if not hit:
                    won = int(entry_s["label"]) == 1
                    tot_roi += ((1.0 if won else 0.0) - bid_u) / bid_u
                cnt += 1
            if cnt >= 4 and (tot_roi / cnt) > best_score:
                best_score = tot_roi / cnt
                best_tp_ratio = cand_r
    except Exception:
        best_tp_ratio = 0.48

    params_out = {"ema_period": 13, "hl_window": 5, "atr_min_bps": 0.30, "prop_tp_ratio": best_tp_ratio}
    record = {
        "version_id": version_id,
        "created_at": time.time(),
        "model_type": model_type,
        "role": role,
        "train_rounds": len(windows),
        "val_brier": val_brier,
        "val_auc": val_auc,
        "shadow_rounds": 0,
        "shadow_brier": None,
        "shadow_pnl": 0.0,
        "params_json": json.dumps(params_out),
        "artifact_path": artifact_path,
    }
    db.save_model_version(record)
    if role == "CHAMPION":
        db.set_champion_version(version_id)

    return {
        "ok": True,
        "version_id": version_id,
        "model_type": model_type,
        "role": role,
        "train_rounds": len(windows),
        "val_brier": val_brier,
        "val_auc": val_auc,
    }


def load_model_artifact(artifact_path: Optional[str]) -> Optional[OfflineModelWrapper]:
    if not artifact_path or not Path(artifact_path).exists():
        return None
    try:
        with open(artifact_path, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None
