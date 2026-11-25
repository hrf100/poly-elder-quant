import os

# CRITICAL v29: Force single-threaded passive OpenMP/BLAS BEFORE numpy/lightgbm/sklearn import
# Eliminates 30-80% idle OpenMP spin-wait CPU burn on 1-2 vCPU VPS instances!
for _env_k, _env_v in (
    ("OMP_NUM_THREADS", "1"),
    ("OPENBLAS_NUM_THREADS", "1"),
    ("MKL_NUM_THREADS", "1"),
    ("NUMEXPR_NUM_THREADS", "1"),
    ("VECLIB_MAXIMUM_THREADS", "1"),
    ("OMP_WAIT_POLICY", "PASSIVE"),
):
    os.environ.setdefault(_env_k, _env_v)

import asyncio
import csv
import io
import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from backend.auth_crypto import (
    check_ip_locked,
    clear_login_failures,
    create_access_token,
    hash_password,
    record_login_failure,
    verify_access_token,
    verify_password,
)
from backend.config import HARD_LIMITS
from backend.fast_json import dumps_str as fast_dumps_str
from backend.execution.engine import QuantTradingEngine
from backend.storage.db import Database
from backend.strategy.strategy_io import (
    export_strategy_bundle,
    get_backup_status,
    import_strategy_bundle,
    restore_pre_import_backup,
)

STATIC_DIR = Path(__file__).resolve().parent / "static"
db = Database()
engine = QuantTradingEngine(db)


_ws_clients: set[WebSocket] = set()
_ws_broadcast_task: Optional[asyncio.Task] = None
_ws_latest_frame: str = ""


async def _ws_broadcast_loop():
    """Single shared 1 Hz broadcaster: zero serialization CPU when no browser is connected."""
    global _ws_latest_frame
    while True:
        try:
            if not _ws_clients:
                await asyncio.sleep(1.0)
                continue
            frame = fast_dumps_str(engine.get_live_state_payload())
            _ws_latest_frame = frame
            clients = tuple(_ws_clients)

            async def _push(ws: WebSocket):
                try:
                    await asyncio.wait_for(ws.send_text(frame), timeout=2.5)
                except Exception:
                    _ws_clients.discard(ws)

            await asyncio.gather(*(_push(c) for c in clients), return_exceptions=True)
        except asyncio.CancelledError:
            break
        except Exception:
            pass
        await asyncio.sleep(1.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _ws_broadcast_task
    await engine.start()
    _ws_broadcast_task = asyncio.create_task(_ws_broadcast_loop())
    yield
    if _ws_broadcast_task:
        _ws_broadcast_task.cancel()
        await asyncio.gather(_ws_broadcast_task, return_exceptions=True)
    await engine.stop()


app = FastAPI(title="Polymarket BTC 5m Quant Elder Self-Learning Terminal", lifespan=lifespan)


def _extract_token(request: Request) -> Optional[str]:
    cookie_tok = request.cookies.get("pq_token")
    if cookie_tok:
        return cookie_tok
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    return None


def require_auth(request: Request):
    tok = _extract_token(request)
    if not tok or not verify_access_token(tok):
        raise HTTPException(status_code=401, detail="未登录或会话已过期")
    return True


# -------- Auth Models & Routes --------
class LoginReq(BaseModel):
    password: str


class ChangePasswordReq(BaseModel):
    old_password: str
    new_password: str


class ModeReq(BaseModel):
    mode: str  # PAPER | LIVE (legacy PAUSED mapped to pause toggle)
    confirm_live: bool = False


class PauseReq(BaseModel):
    paused: bool
    confirm_live: bool = False


class RiskConfigReq(BaseModel):
    unit_size_usdc: float = 5.0
    max_daily_loss_usdc: float = 50.0
    consecutive_loss_limit: int = 4
    live_taker_only: bool = True
    live_enable_arb_lock: bool = False
    strategy_engine: str = "FV_DISCOUNT_LEADER"
    fv_exit_mode: str = "TIME_TWAP_MOAT"
    fv_strict_5s_confirm: bool = False
    sizing_mode: str = "FIXED"
    dynamic_pct: float = 3.0
    dynamic_min_usdc: float = 1.05
    dynamic_max_usdc: float = 50.0


class WalletConfigReq(BaseModel):
    private_key: Optional[str] = None
    signature_type: int = 3
    funder_address: str = ""
    wallet_type: str = ""


class WalletDetectReq(BaseModel):
    private_key: Optional[str] = None
    auto_apply: bool = True


class WebhookReq(BaseModel):
    enabled: bool = False
    feishu_url: str = ""
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""


class PaperResetReq(BaseModel):
    initial_balance: float = 1000.0


class ResetDatasetReq(BaseModel):
    clear_models: bool = True


class AutoTrainConfigReq(BaseModel):
    enabled: bool = True
    interval_hours: int = 12
    min_rounds: int = 16


class StrategyImportReq(BaseModel):
    bundle: dict
    apply_online_state: bool = True
    apply_risk_config: bool = True
    apply_auto_train: bool = True
    apply_champion_model: bool = True


@app.post("/api/auth/login")
async def login(req: LoginReq, request: Request, response: Response):
    ip = request.client.host if request.client else "unknown"
    locked = check_ip_locked(ip)
    if locked:
        raise HTTPException(status_code=429, detail=f"登录失败次数过多，请 {locked} 秒后再试")

    stored_hash = db.get_kv("auth_password_hash", "")
    if not verify_password(req.password, stored_hash):
        record_login_failure(ip)
        raise HTTPException(status_code=401, detail="密码错误")

    clear_login_failures(ip)
    token = create_access_token("admin")
    response.set_cookie("pq_token", token, httponly=True, samesite="lax", max_age=72 * 3600)
    return {
        "ok": True,
        "token": token,
        "is_default_password": db.get_kv("is_default_password", "true") == "true",
    }


@app.post("/api/auth/logout")
async def logout(response: Response):
    response.delete_cookie("pq_token")
    return {"ok": True}


@app.get("/api/auth/me")
async def auth_me(request: Request):
    tok = _extract_token(request)
    if not tok or not verify_access_token(tok):
        return {"authenticated": False}
    return {
        "authenticated": True,
        "is_default_password": db.get_kv("is_default_password", "true") == "true",
    }


@app.post("/api/auth/change-password")
async def change_password(req: ChangePasswordReq, _=Depends(require_auth)):
    stored_hash = db.get_kv("auth_password_hash", "")
    if not verify_password(req.old_password, stored_hash):
        raise HTTPException(status_code=400, detail="原密码错误")
    if len(req.new_password) < 6:
        raise HTTPException(status_code=400, detail="新密码长度至少 6 位")
    db.set_kv("auth_password_hash", hash_password(req.new_password))
    db.set_kv("is_default_password", "false")
    return {"ok": True}


# -------- Dashboard & Control Routes --------
@app.get("/api/state")
async def get_state(_=Depends(require_auth)):
    return engine.get_live_state_payload()


@app.get("/api/orders")
async def get_orders(mode: str = Query("PAPER"), limit: int = Query(100, le=500), _=Depends(require_auth)):
    m = str(mode or "PAPER").upper()
    if m not in ("PAPER", "LIVE", "SHADOW"):
        m = db.get_trading_mode()
    audit = db.get_account_audit_summary(m)
    return {
        "mode": m,
        "orders": engine.get_enriched_orders(mode=m, limit=limit),
        "stats": audit["stats"],
        "audit": audit,
    }


@app.get("/api/orders/export")
async def export_orders_csv(mode: str = Query("PAPER"), _=Depends(require_auth)):
    rows = db.get_orders(mode=mode, limit=2000)
    buf = io.StringIO()
    if rows:
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=poly_orders_{mode.lower()}.csv"},
    )


@app.get("/api/equity")
async def get_equity(mode: str = Query("PAPER"), _=Depends(require_auth)):
    m = mode.upper()
    curve = db.get_equity_curve(mode=m, limit=500)
    stats = db.get_performance_stats(mode=m)
    if m == "LIVE" and not curve:
        pf = engine.account.cached_portfolio
        live_eq = float(pf.get("total_equity") or pf.get("pusd_balance") or 0.0)
        curve = [{
            "ts": pf.get("updated_at") or time.time(),
            "window_ts": engine.current_window_ts,
            "balance_usdc": round(live_eq, 4),
            "cumulative_pnl": float(stats.get("total_pnl") or 0.0),
            "trade_count": int(stats.get("total_trades") or 0),
        }]
    return {
        "curve": curve,
        "stats": stats,
    }


@app.get("/api/rounds")
async def get_rounds(_=Depends(require_auth)):
    return {"rounds": db.get_recent_settled_rounds(limit=30)}


async def _validate_and_prepare_live_execution(confirm_live: bool) -> dict:
    if not confirm_live:
        raise HTTPException(status_code=400, detail="开启实盘自动交易必须二次确认")
    if engine.oracle.status["geo_check"].get("blocked"):
        raise HTTPException(status_code=400, detail="当前服务器 IP 处于 Polymarket 地区限制范围，禁止开启实盘")
    if engine.is_duplicate_instance:
        raise HTTPException(status_code=409, detail="检测到重复进程，请先清理残留进程后再开启实盘")
    w = engine.account.get_public_wallet_info()
    if not w.get("sdk_available"):
        raise HTTPException(status_code=400, detail="未安装官方 CLOB V2 SDK (polymarket-client)，请执行 bash scripts/deploy.sh 自动安装")
    if not w["has_private_key"] or not w["has_api_creds"] or w["is_observer_only"]:
        raise HTTPException(status_code=400, detail="请先导入实盘私钥并完成 CLOB V2 API 凭证派生")
    pf = await engine.account.refresh_balances_and_portfolio(force=True)
    engine.account.restart_user_stream()
    live_eq = float(pf.get("total_equity") or pf.get("pusd_balance") or 0.0)
    existing_live_curve = db.get_equity_curve(mode="LIVE", limit=1)
    if not existing_live_curve or abs(float(existing_live_curve[-1].get("balance_usdc") or 0.0) - live_eq) >= 0.01:
        live_stats = db.get_performance_stats("LIVE")
        db.record_equity("LIVE", engine.current_window_ts, round(live_eq, 4), live_stats["total_pnl"], live_stats["total_trades"])
    return pf


@app.post("/api/trading/mode")
async def set_trading_mode(req: ModeReq, _=Depends(require_auth)):
    mode = req.mode.upper()
    if mode == "PAUSED":
        db.set_trading_paused(True)
        return {"ok": True, "mode": db.get_trading_mode(), "paused": True}
    if mode not in ("PAPER", "LIVE"):
        raise HTTPException(status_code=400, detail="无效的账户模式")

    paused = db.is_trading_paused()
    if mode == "LIVE":
        if not paused:
            await _validate_and_prepare_live_execution(req.confirm_live)
        else:
            if engine.account.get_public_wallet_info().get("has_private_key"):
                try:
                    await engine.account.refresh_balances_and_portfolio(force=True)
                except Exception:
                    pass
    db.set_kv("trading_mode", mode)
    return {"ok": True, "mode": mode, "paused": paused}


@app.post("/api/trading/pause")
async def set_trading_pause(req: PauseReq, _=Depends(require_auth)):
    mode = db.get_trading_mode()
    if req.paused:
        db.set_trading_paused(True)
        return {"ok": True, "mode": mode, "paused": True}
    if mode == "LIVE":
        await _validate_and_prepare_live_execution(req.confirm_live)
    db.set_trading_paused(False)
    return {"ok": True, "mode": mode, "paused": False}


@app.post("/api/trading/cancel-live")
async def cancel_all_live_orders(_=Depends(require_auth)):
    try:
        return await engine.account.cancel_all_open_orders()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/trading/cancel-order/{order_id}")
async def cancel_single_live_order(order_id: str, _=Depends(require_auth)):
    try:
        return await engine.account.cancel_order(order_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/account")
async def get_polymarket_account(force: bool = Query(False), _=Depends(require_auth)):
    """Polymarket official SDK account snapshot (cash, portfolio, positions, open orders, approvals)."""
    pf = await engine.account.refresh_balances_and_portfolio(
        force=force, cache_key=str(engine.current_condition_id or "")
    )
    return {
        "cash": pf.get("cash", 0.0),
        "available_cash": pf.get("available_cash", pf.get("pusd_balance", 0.0)),
        "portfolio": pf.get("portfolio_value", 0.0),
        "total_equity": pf.get("total_equity", 0.0),
        "open_orders_locked_usdc": pf.get("open_orders_locked_usdc", 0.0),
        "local_reserved_usdc": pf.get("local_reserved_usdc", 0.0),
        "cash_verified": bool(pf.get("cash_verified", False)),
        "approved": bool(pf.get("approved", pf.get("allowance_ok", False))),
        "missing_approvals": pf.get("missing_approvals", []),
        "positions": pf.get("open_positions", []),
        "orders": pf.get("open_orders", []),
        "wallet": engine.account.get_public_wallet_info(),
        "live_channel": engine.account.live_channel_status,
        "error": pf.get("error", ""),
        "updated_at": pf.get("updated_at", 0.0),
    }


@app.post("/api/trading/reconcile-live")
async def reconcile_live_now(_=Depends(require_auth)):
    res = await engine.reconcile_live_orders()
    await engine.account.refresh_balances_and_portfolio(force=True)
    return res


@app.get("/api/positions/lookup")
async def lookup_positions(user: str = Query(...), _=Depends(require_auth)):
    """Channel 2 read-only data-api lookup for any 0x Polymarket address (no private key required)."""
    try:
        cids = [engine.current_condition_id] if engine.current_condition_id else None
        rows = await engine.account.lookup_address_positions(user, condition_ids=cids)
        return {"ok": True, "user": user, "positions": rows}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/trading/reset-paper")
async def reset_paper(req: PaperResetReq, _=Depends(require_auth)):
    if req.initial_balance < 10 or req.initial_balance > 1_000_000:
        raise HTTPException(status_code=400, detail="初始模拟资金需在 10 ~ 1,000,000 USDC 之间")
    db.reset_paper_account(req.initial_balance)
    engine.consecutive_losses = 0
    engine.circuit_breaker_until = 0.0
    return {"ok": True, "balance": req.initial_balance}


@app.post("/api/trading/reset-breaker")
async def reset_breaker(_=Depends(require_auth)):
    engine.consecutive_losses = 0
    engine.circuit_breaker_until = 0.0
    return {"ok": True}


@app.post("/api/system/check-clock")
async def check_clock_now(_=Depends(require_auth)):
    drift = await engine.oracle.check_clock_drift()
    return {"ok": drift is not None, "clock_drift_ms": engine.oracle.status["geo_check"].get("clock_drift_ms", 0.0)}


@app.get("/api/config/risk")
async def get_risk_config(_=Depends(require_auth)):
    cfg = db.get_json("risk_config", {})
    em = str(cfg.get("fv_exit_mode") or "TIME_TWAP_MOAT").upper()
    if em not in ("TIME_TWAP_MOAT", "DYNAMIC_EV", "HOLD_EXPIRY"):
        em = "TIME_TWAP_MOAT"
    sm = str(cfg.get("sizing_mode") or "FIXED").upper()
    if sm not in ("FIXED", "DYNAMIC_PCT"):
        sm = "FIXED"
    min_u = float(HARD_LIMITS["min_unit_usdc"])
    d_min = round(max(min_u, min(1000.0, float(cfg.get("dynamic_min_usdc", min_u)))), 2)
    d_max = round(max(d_min, min(5000.0, float(cfg.get("dynamic_max_usdc", 50.0)))), 2)
    return {
        "unit_size_usdc": round(max(min_u, float(cfg.get("unit_size_usdc", 5.0))), 2),
        "max_daily_loss_usdc": float(cfg.get("max_daily_loss_usdc", 50.0)),
        "consecutive_loss_limit": int(cfg.get("consecutive_loss_limit", 4)),
        "live_taker_only": bool(cfg.get("live_taker_only", True)),
        "live_enable_arb_lock": bool(cfg.get("live_enable_arb_lock", False)),
        "strategy_engine": str(cfg.get("strategy_engine") or "FV_DISCOUNT_LEADER"),
        "fv_exit_mode": em,
        "fv_strict_5s_confirm": bool(cfg.get("fv_strict_5s_confirm", False)),
        "sizing_mode": sm,
        "dynamic_pct": round(max(0.5, min(15.0, float(cfg.get("dynamic_pct", 3.0)))), 2),
        "dynamic_min_usdc": d_min,
        "dynamic_max_usdc": d_max,
    }


@app.post("/api/config/risk")
async def save_risk_config(req: RiskConfigReq, _=Depends(require_auth)):
    eng_mode = str(req.strategy_engine or "FV_DISCOUNT_LEADER").upper()
    if eng_mode not in ("FV_DISCOUNT_LEADER", "LEGACY_ELDER"):
        eng_mode = "FV_DISCOUNT_LEADER"
    em = str(req.fv_exit_mode or "TIME_TWAP_MOAT").upper()
    if em not in ("TIME_TWAP_MOAT", "DYNAMIC_EV", "HOLD_EXPIRY"):
        em = "TIME_TWAP_MOAT"
    sm = str(req.sizing_mode or "FIXED").upper()
    if sm not in ("FIXED", "DYNAMIC_PCT"):
        sm = "FIXED"
    min_u = float(HARD_LIMITS["min_unit_usdc"])
    d_min = round(max(min_u, min(1000.0, float(req.dynamic_min_usdc))), 2)
    d_max = round(max(d_min, min(5000.0, float(req.dynamic_max_usdc))), 2)
    data = {
        "unit_size_usdc": round(max(min_u, min(5000.0, float(req.unit_size_usdc))), 2),
        "max_daily_loss_usdc": round(max(3.0, min(50000.0, float(req.max_daily_loss_usdc))), 2),
        "consecutive_loss_limit": max(2, min(20, int(req.consecutive_loss_limit))),
        "live_taker_only": bool(req.live_taker_only),
        "live_enable_arb_lock": bool(req.live_enable_arb_lock),
        "strategy_engine": eng_mode,
        "fv_exit_mode": em,
        "fv_strict_5s_confirm": bool(req.fv_strict_5s_confirm),
        "sizing_mode": sm,
        "dynamic_pct": round(max(0.5, min(15.0, float(req.dynamic_pct))), 2),
        "dynamic_min_usdc": d_min,
        "dynamic_max_usdc": d_max,
    }
    db.set_json("risk_config", data)
    return {"ok": True, "config": data}


@app.get("/api/config/webhook")
async def get_webhook_config(_=Depends(require_auth)):
    return db.get_json("webhook_config", {})


@app.post("/api/config/webhook")
async def save_webhook_config(req: WebhookReq, _=Depends(require_auth)):
    db.set_json("webhook_config", req.model_dump())
    return {"ok": True}


# -------- Polymarket Wallet & Portfolio Routes --------
@app.post("/api/wallet/save")
async def save_wallet(req: WalletConfigReq, _=Depends(require_auth)):
    try:
        info = engine.account.configure_wallet(
            req.private_key, req.signature_type, req.funder_address,
            is_observer_only=False, wallet_type=req.wallet_type,
        )
        return {"ok": True, "wallet": info}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/wallet/detect")
async def detect_wallet(req: WalletDetectReq, _=Depends(require_auth)):
    try:
        res = await engine.account.detect_wallet(req.private_key, auto_apply=req.auto_apply)
        await engine.account.refresh_balances_and_portfolio(force=True)
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/wallet/derive-api")
async def derive_wallet_api(_=Depends(require_auth)):
    try:
        res = await engine.account.derive_api_credentials_async()
        engine.sync_oracle_credentials()
        await engine.account.refresh_balances_and_portfolio(force=True)
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/wallet/generate-observer")
async def generate_observer_wallet(_=Depends(require_auth)):
    try:
        res = await engine.account.generate_observer_wallet_async()
        engine.sync_oracle_credentials()
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/wallet/refresh-balance")
async def refresh_wallet_balance(_=Depends(require_auth)):
    return await engine.account.refresh_balances_and_portfolio(force=True)


# -------- Self-Learning & Model Evolution Routes --------
@app.get("/api/learning/models")
async def list_models(_=Depends(require_auth)):
    return {
        "versions": db.list_model_versions(limit=15),
        "learning": engine.learner.get_dashboard_summary(),
        "dataset": db.get_slice_counts(),
        "strategy_backup": get_backup_status(db),
    }


@app.get("/api/strategy/export")
async def export_strategy(
    include_model: bool = Query(True),
    name: str = Query(""),
    _=Depends(require_auth),
):
    bundle = export_strategy_bundle(engine, include_model=include_model, name=name)
    raw = json.dumps(bundle, ensure_ascii=False, indent=2).encode("utf-8")
    ts_tag = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    fname = f"poly_strategy_v53_{ts_tag}.json"
    return StreamingResponse(
        io.BytesIO(raw),
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.post("/api/strategy/import")
async def import_strategy(req: StrategyImportReq, _=Depends(require_auth)):
    try:
        return import_strategy_bundle(
            engine,
            req.bundle,
            apply_online_state=req.apply_online_state,
            apply_risk_config=req.apply_risk_config,
            apply_auto_train=req.apply_auto_train,
            apply_champion_model=req.apply_champion_model,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"导入策略失败: {str(e)[:80]}")


@app.post("/api/strategy/rollback")
async def rollback_strategy(_=Depends(require_auth)):
    try:
        return restore_pre_import_backup(engine)
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/learning/train-now")
async def train_now(_=Depends(require_auth)):
    return await engine.trigger_offline_training()


@app.post("/api/learning/reset")
async def reset_learning(_=Depends(require_auth)):
    engine.learner.reset_online_state()
    return {"ok": True}


@app.post("/api/learning/reset-dataset")
async def reset_dataset(req: ResetDatasetReq, _=Depends(require_auth)):
    if engine.training_status.get("running"):
        raise HTTPException(status_code=409, detail="离线训练正在运行中，请稍后再重置样本库")
    res = engine.reset_learning_dataset(clear_models=req.clear_models)
    return {"ok": True, **res}


@app.post("/api/learning/auto-train-config")
async def save_auto_train_config(req: AutoTrainConfigReq, _=Depends(require_auth)):
    cfg = engine.update_auto_train_config(req.enabled, req.interval_hours, req.min_rounds)
    return {"ok": True, "config": cfg}


@app.post("/api/learning/promote/{version_id}")
async def promote_model(version_id: str, _=Depends(require_auth)):
    ok = db.set_champion_version(version_id)
    if not ok:
        raise HTTPException(status_code=404, detail="模型版本不存在")
    engine._load_champion_model()
    return {"ok": True, "active_version_id": engine.learner.active_version_id}


# -------- Real-time WebSocket Stream --------
@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    token = websocket.cookies.get("pq_token") or websocket.query_params.get("token")
    if not token or not verify_access_token(token):
        await websocket.close(code=4401)
        return
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        init_frame = _ws_latest_frame or fast_dumps_str(engine.get_live_state_payload())
        await websocket.send_text(init_frame)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        _ws_clients.discard(websocket)


# -------- Serve Zero-Build SPA Frontend --------
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(
        str(STATIC_DIR / "index.html"),
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )
