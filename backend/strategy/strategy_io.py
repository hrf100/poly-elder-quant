import base64
import copy
import hashlib
import json
import math
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.config import HARD_LIMITS, MODELS_DIR
from backend.strategy.offline_trainer import load_model_artifact

STRATEGY_SCHEMA = "poly-elder-quant-strategy/v1"
APP_VERSION = "v53"
MAX_ARTIFACT_BYTES = 10 * 1024 * 1024  # 10 MB hard cap
BACKUP_KV_KEY = "strategy_pre_import_backup"


def _sanitize_id(raw: str, fallback: str = "imported") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(raw or "").strip())
    cleaned = cleaned.strip("._-")[:48]
    return cleaned or fallback


def _assert_finite_numbers(obj: Any, path: str = "root") -> None:
    """Recursively reject NaN / Infinity in imported JSON numbers."""
    if isinstance(obj, bool):
        return
    if isinstance(obj, (int, float)):
        if not math.isfinite(obj):
            raise ValueError(f"策略文件包含非法数值 ({path})")
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            _assert_finite_numbers(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for idx, v in enumerate(obj):
            _assert_finite_numbers(v, f"{path}[{idx}]")


def _find_active_champion_row(db) -> Optional[dict]:
    for v in db.list_model_versions(limit=30):
        if v.get("role") == "CHAMPION":
            return v
    return None


def export_strategy_bundle(engine, include_model: bool = True, name: str = "") -> dict:
    """
    Export a portable, zero-secret strategy package containing:
    - Online self-learning state (weights, execution arms, exit arms, sub-strategies, edge curve, auto_params, time buckets)
    - Risk baseline config (unit_size_usdc, max_daily_loss_usdc, consecutive_loss_limit)
    - Offline auto-train schedule config
    - Optional active CHAMPION offline model artifact (base64 + SHA-256 checksum)

    Explicitly NEVER exports private keys, API credentials, login password hash, webhooks,
    trading mode, account balances, or order history.
    """
    now = time.time()
    db = engine.db
    learner = engine.learner

    online_state = copy.deepcopy(learner.state)
    risk_cfg = db.get_json("risk_config", {
        "unit_size_usdc": 10.0,
        "max_daily_loss_usdc": 100.0,
        "consecutive_loss_limit": 3,
    })
    auto_train_cfg = db.get_json("auto_train_config", {
        "enabled": True,
        "interval_hours": 12,
        "min_rounds": 16,
    })

    champ_payload = None
    if include_model:
        champ_row = _find_active_champion_row(db)
        if champ_row:
            art_path = champ_row.get("artifact_path")
            if art_path and Path(art_path).is_file():
                raw_bytes = Path(art_path).read_bytes()
                if len(raw_bytes) <= MAX_ARTIFACT_BYTES:
                    champ_payload = {
                        "version_id": str(champ_row.get("version_id") or "champion"),
                        "created_at": float(champ_row.get("created_at") or now),
                        "model_type": str(champ_row.get("model_type") or "lightgbm"),
                        "train_rounds": int(champ_row.get("train_rounds") or 0),
                        "val_brier": float(champ_row.get("val_brier") or 0.25),
                        "val_auc": float(champ_row.get("val_auc") or 0.5),
                        "params_json": str(champ_row.get("params_json") or "{}"),
                        "artifact_filename": Path(art_path).name,
                        "artifact_sha256": hashlib.sha256(raw_bytes).hexdigest(),
                        "artifact_size_bytes": len(raw_bytes),
                        "artifact_b64": base64.b64encode(raw_bytes).decode("ascii"),
                    }

    dash = learner.get_dashboard_summary()
    clean_name = str(name or "").strip()[:64] or f"PolyQuant_{learner.active_version_id}"

    return {
        "schema": STRATEGY_SCHEMA,
        "app_version": APP_VERSION,
        "exported_at": round(now, 3),
        "exported_at_utc": datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "name": clean_name,
        "summary": {
            "active_version_id": learner.active_version_id,
            "updates_count": int(online_state.get("updates_count", 0)),
            "rolling_brier": float(dash.get("rolling_brier", 0.20)),
            "best_exit_arm": str(dash.get("best_exit_arm", "MAKER_FAIR_TP")),
            "has_champion_model": champ_payload is not None,
            "champion_model_type": champ_payload["model_type"] if champ_payload else None,
        },
        "self_learning_state": online_state,
        "risk_config": {
            "unit_size_usdc": float(risk_cfg.get("unit_size_usdc", 5.0)),
            "max_daily_loss_usdc": float(risk_cfg.get("max_daily_loss_usdc", 50.0)),
            "consecutive_loss_limit": int(risk_cfg.get("consecutive_loss_limit", 4)),
            "strategy_engine": str(risk_cfg.get("strategy_engine") or "FV_DISCOUNT_LEADER"),
            "fv_exit_mode": str(risk_cfg.get("fv_exit_mode") or "TIME_TWAP_MOAT"),
            "fv_strict_5s_confirm": bool(risk_cfg.get("fv_strict_5s_confirm", False)),
            "sizing_mode": str(risk_cfg.get("sizing_mode") or "FIXED"),
            "dynamic_pct": float(risk_cfg.get("dynamic_pct", 3.0)),
            "dynamic_min_usdc": float(risk_cfg.get("dynamic_min_usdc", HARD_LIMITS["min_unit_usdc"])),
            "dynamic_max_usdc": float(risk_cfg.get("dynamic_max_usdc", 50.0)),
        },
        "auto_train_config": {
            "enabled": bool(auto_train_cfg.get("enabled", True)),
            "interval_hours": int(auto_train_cfg.get("interval_hours", 12)),
            "min_rounds": int(auto_train_cfg.get("min_rounds", 16)),
        },
        "champion_model": champ_payload,
    }


def _save_pre_import_backup(engine) -> None:
    """Save a lightweight automatic snapshot right before importing so user can 1-click rollback."""
    db = engine.db
    champ_row = _find_active_champion_row(db)
    backup = {
        "saved_at": round(time.time(), 3),
        "active_version_id": engine.learner.active_version_id,
        "champion_version_id": champ_row["version_id"] if champ_row else None,
        "self_learning_state": copy.deepcopy(engine.learner.state),
        "risk_config": db.get_json("risk_config", {}),
        "auto_train_config": db.get_json("auto_train_config", {}),
    }
    db.set_json(BACKUP_KV_KEY, backup)


def get_backup_status(db) -> dict:
    b = db.get_json(BACKUP_KV_KEY)
    if not isinstance(b, dict) or not b.get("saved_at"):
        return {"has_backup": False, "saved_at": None, "active_version_id": None}
    return {
        "has_backup": True,
        "saved_at": float(b.get("saved_at") or 0.0),
        "active_version_id": b.get("active_version_id") or "online_baseline_v24",
    }


def _normalize_incoming_bundle(bundle: dict) -> dict:
    """Accept both full `poly-elder-quant-strategy/v1` packages and raw `self_learning_state` JSON."""
    if not isinstance(bundle, dict):
        raise ValueError("策略文件格式无效：根节点必须是 JSON 对象")
    _assert_finite_numbers(bundle)

    if bundle.get("schema") == STRATEGY_SCHEMA:
        return bundle

    # Allow importing a raw self_learning_state dictionary if weights & arms exist
    if "weights" in bundle and "arms" in bundle and isinstance(bundle.get("weights"), list):
        return {
            "schema": STRATEGY_SCHEMA,
            "name": str(bundle.get("name") or "raw_online_state"),
            "self_learning_state": bundle,
            "risk_config": None,
            "auto_train_config": None,
            "champion_model": None,
        }

    if "self_learning_state" in bundle and isinstance(bundle["self_learning_state"], dict):
        return bundle

    raise ValueError("无法识别的策略文件格式（缺少 schema 或 self_learning_state）")


def _import_champion_artifact(engine, champ: dict) -> tuple[bool, str, Optional[str]]:
    """Verify SHA-256, safely write .pkl into MODELS_DIR, validate loader, and promote to CHAMPION."""
    if not isinstance(champ, dict):
        return False, "策略包未包含离线主模型", None
    b64 = champ.get("artifact_b64")
    expected_sha = str(champ.get("artifact_sha256") or "").strip().lower()
    if not b64 or not isinstance(b64, str):
        return False, "离线模型缺少权重数据", None

    try:
        raw_bytes = base64.b64decode(b64, validate=True)
    except Exception:
        return False, "离线模型 Base64 解码失败", None

    if len(raw_bytes) == 0 or len(raw_bytes) > MAX_ARTIFACT_BYTES:
        return False, f"离线模型体积超限 ({len(raw_bytes)} bytes)", None

    actual_sha = hashlib.sha256(raw_bytes).hexdigest().lower()
    if expected_sha and actual_sha != expected_sha:
        return False, "离线模型 SHA-256 校验不匹配，文件可能已损坏", None

    orig_vid = _sanitize_id(champ.get("version_id") or "model", fallback="model")
    existing_ids = {v["version_id"] for v in engine.db.list_model_versions(limit=200)}
    version_id = orig_vid if orig_vid not in existing_ids else f"{orig_vid}_imp_{int(time.time()) % 100000}"

    target_dir = MODELS_DIR.resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = (target_dir / f"model_{version_id}.pkl").resolve()
    if target_path.parent != target_dir:
        return False, "非法的模型文件路径", None

    target_path.write_bytes(raw_bytes)
    loaded = load_model_artifact(str(target_path))
    if loaded is None or not hasattr(loaded, "predict_prob_from_snap"):
        try:
            target_path.unlink(missing_ok=True)
        except OSError:
            pass
        return False, "离线模型文件无法反序列化或结构不兼容，已跳过离线模型导入", None

    # Sanitize params_json
    raw_params = champ.get("params_json") or "{}"
    if isinstance(raw_params, dict):
        params_dict = raw_params
    else:
        try:
            params_dict = json.loads(str(raw_params))
            if not isinstance(params_dict, dict):
                params_dict = {}
        except Exception:
            params_dict = {}

    record = {
        "version_id": version_id,
        "created_at": float(champ.get("created_at") or time.time()),
        "model_type": str(champ.get("model_type") or getattr(loaded, "model_type", "lightgbm"))[:32],
        "role": "CHAMPION",
        "train_rounds": max(0, int(champ.get("train_rounds") or 0)),
        "val_brier": round(max(0.0, min(1.0, float(champ.get("val_brier") or 0.25))), 5),
        "val_auc": round(max(0.0, min(1.0, float(champ.get("val_auc") or 0.50))), 4),
        "shadow_rounds": 0,
        "shadow_brier": None,
        "shadow_pnl": 0.0,
        "params_json": json.dumps(params_dict),
        "artifact_path": str(target_path),
    }
    engine.db.save_model_version(record)
    engine.db.set_champion_version(version_id)
    engine._load_champion_model()
    return True, f"已导入并激活离线主模型 {version_id}", version_id


def import_strategy_bundle(
    engine,
    bundle: dict,
    apply_online_state: bool = True,
    apply_risk_config: bool = True,
    apply_auto_train: bool = True,
    apply_champion_model: bool = True,
    save_backup: bool = True,
) -> dict:
    """
    Validate, sanitize, backup current state, and hot-apply an imported strategy package.
    Never alters wallet credentials, passwords, or trading_mode (PAPER/PAUSED/LIVE).
    """
    if engine.training_status.get("running"):
        raise RuntimeError("离线训练正在运行中，请等待训练完成后再导入策略")

    norm = _normalize_incoming_bundle(bundle)
    if save_backup:
        _save_pre_import_backup(engine)

    applied: list[str] = []
    warnings: list[str] = []

    # 1. Online self-learning state (weights, arms, exit_arms, edge_curve, auto_params, sub_strategies, time_buckets)
    if apply_online_state:
        st = norm.get("self_learning_state")
        if not isinstance(st, dict) or not isinstance(st.get("weights"), list) or not isinstance(st.get("arms"), list):
            raise ValueError("策略文件缺少有效的在线权重数据 (self_learning_state.weights / arms)")
        if len(st["weights"]) < 8 or len(st["weights"]) > 16:
            raise ValueError(f"特征权重维度不受支持 ({len(st['weights'])})")
        engine.db.set_json("self_learning_state", copy.deepcopy(st))
        # Official migration + HARD_LIMITS clamping inside SelfLearningCoordinator._load_state()
        engine.learner._load_state()
        ema_p = max(5, min(60, int(engine.learner.state.get("ema_period", 13))))
        hl_w = max(2, min(30, int(engine.learner.state.get("hl_window", 5))))
        engine.features.set_hyperparams(ema_p, hl_w)
        applied.append("在线权重与进化参数")

    # 2. Risk baseline config (clamped safely)
    if apply_risk_config and isinstance(norm.get("risk_config"), dict):
        rc = norm["risk_config"]
        min_u = float(HARD_LIMITS["min_unit_usdc"])
        sm = str(rc.get("sizing_mode") or "FIXED").upper()
        if sm not in ("FIXED", "DYNAMIC_PCT"):
            sm = "FIXED"
        em = str(rc.get("fv_exit_mode") or "TIME_TWAP_MOAT").upper()
        if em not in ("TIME_TWAP_MOAT", "DYNAMIC_EV", "HOLD_EXPIRY"):
            em = "TIME_TWAP_MOAT"
        d_min = round(max(min_u, min(1000.0, float(rc.get("dynamic_min_usdc", min_u)))), 2)
        d_max = round(max(d_min, min(5000.0, float(rc.get("dynamic_max_usdc", 50.0)))), 2)
        sanitized_rc = {
            "unit_size_usdc": round(max(min_u, min(5000.0, float(rc.get("unit_size_usdc", 5.0)))), 2),
            "max_daily_loss_usdc": round(max(3.0, min(100000.0, float(rc.get("max_daily_loss_usdc", 50.0)))), 2),
            "consecutive_loss_limit": max(2, min(20, int(rc.get("consecutive_loss_limit", 4)))),
            "live_taker_only": bool(rc.get("live_taker_only", True)),
            "live_enable_arb_lock": bool(rc.get("live_enable_arb_lock", False)),
            "strategy_engine": str(rc.get("strategy_engine") or "FV_DISCOUNT_LEADER"),
            "fv_exit_mode": em,
            "fv_strict_5s_confirm": bool(rc.get("fv_strict_5s_confirm", False)),
            "sizing_mode": sm,
            "dynamic_pct": round(max(0.5, min(15.0, float(rc.get("dynamic_pct", 3.0)))), 2),
            "dynamic_min_usdc": d_min,
            "dynamic_max_usdc": d_max,
        }
        engine.db.set_json("risk_config", sanitized_rc)
        applied.append("风控底线")

    # 3. Auto-train config
    if apply_auto_train and isinstance(norm.get("auto_train_config"), dict):
        atc = norm["auto_train_config"]
        engine.update_auto_train_config(
            bool(atc.get("enabled", True)),
            int(atc.get("interval_hours", 12)),
            int(atc.get("min_rounds", 16)),
        )
        applied.append("自动训练计划")

    # 4. Optional offline champion model artifact
    imported_model_id = None
    if apply_champion_model and norm.get("champion_model"):
        ok_m, msg_m, vid = _import_champion_artifact(engine, norm["champion_model"])
        if ok_m:
            imported_model_id = vid
            applied.append(f"离线主模型({vid})")
        else:
            warnings.append(msg_m)

    if not applied:
        raise ValueError("未勾选任何可导入的策略模块")

    return {
        "ok": True,
        "name": str(norm.get("name") or "imported_strategy"),
        "applied": applied,
        "warnings": warnings,
        "active_version_id": engine.learner.active_version_id,
        "imported_model_id": imported_model_id,
        "backup": get_backup_status(engine.db),
        "risk_config": engine.db.get_json("risk_config", {}),
        "auto_train_config": engine.db.get_json("auto_train_config", {}),
    }


def restore_pre_import_backup(engine) -> dict:
    """One-click rollback to the exact state snapshot saved before the last strategy import."""
    if engine.training_status.get("running"):
        raise RuntimeError("离线训练正在运行中，请稍后再撤回")

    db = engine.db
    backup = db.get_json(BACKUP_KV_KEY)
    if not isinstance(backup, dict) or not isinstance(backup.get("self_learning_state"), dict):
        raise ValueError("当前没有可撤回的导入前备份")

    db.set_json("self_learning_state", copy.deepcopy(backup["self_learning_state"]))
    engine.learner._load_state()
    ema_p = max(5, min(60, int(engine.learner.state.get("ema_period", 13))))
    hl_w = max(2, min(30, int(engine.learner.state.get("hl_window", 5))))
    engine.features.set_hyperparams(ema_p, hl_w)

    if isinstance(backup.get("risk_config"), dict) and backup["risk_config"]:
        db.set_json("risk_config", backup["risk_config"])

    if isinstance(backup.get("auto_train_config"), dict) and backup["auto_train_config"]:
        atc = backup["auto_train_config"]
        engine.update_auto_train_config(
            bool(atc.get("enabled", True)),
            int(atc.get("interval_hours", 12)),
            int(atc.get("min_rounds", 16)),
        )

    prev_champ = backup.get("champion_version_id")
    if prev_champ and db.set_champion_version(str(prev_champ)):
        engine._load_champion_model()
    else:
        engine.learner.offline_model = None
        engine.learner.active_version_id = str(backup.get("active_version_id") or "online_baseline_v24")

    return {
        "ok": True,
        "active_version_id": engine.learner.active_version_id,
        "restored_from_ts": float(backup.get("saved_at") or 0.0),
        "risk_config": db.get_json("risk_config", {}),
        "auto_train_config": db.get_json("auto_train_config", {}),
    }
