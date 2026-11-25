import asyncio
import dataclasses
import inspect
import json
import logging
import re
import threading
import time
from typing import Any, Callable, Iterable, Optional, Union
import aiohttp

from backend.auth_crypto import decrypt_secret, encrypt_secret, mask_address
from backend.config import (
    DATA_API_BASE,
    GAMMA_API_BASE,
    POLYGON_RPC_URLS,
    POLY_HEADERS,
    POLY_PROXY,
    PUSD_CONTRACT,
    USDCE_CONTRACT,
)
from backend.feeds.clock import ServerClock
from backend.storage.db import Database

log = logging.getLogger("poly.live_clob")

PUSD_DECIMALS = 6
_SLUG_TS_RE = re.compile(r"updown-5m-(\d{9,})")
_TERMINAL_CANCEL_STATUSES = {"CANCELLED", "CANCELED", "CANCELLATION", "EXPIRED", "REJECTED", "INVALID", "UNMATCHED"}
_FAILED_TRADE_STATUSES = {"FAILED", "REJECTED", "INVALID", "REVERTED"}
_CONFIRMED_TRADE_STATUSES = {"MINED", "CONFIRMED", "SETTLED"}


def _get_field(obj: Any, *names: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        for n in names:
            if n in obj and obj[n] is not None:
                return obj[n]
        return default
    for n in names:
        val = getattr(obj, n, None)
        if val is not None:
            return val
    return default


def _to_float(val: Any, default: float = 0.0) -> float:
    if val in (None, ""):
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _parse_collateral_usdc(raw: Any) -> float:
    """Strictly parse 6-decimal base-unit collateral integers from `get_balance_allowance`."""
    if raw in (None, ""):
        return 0.0
    if isinstance(raw, int):
        return round(raw / (10 ** PUSD_DECIMALS), 4)
    s = str(raw).strip()
    if not s:
        return 0.0
    v = _to_float(s, 0.0)
    if v <= 0.0:
        return 0.0
    return round(v, 4) if "." in s else round(v / (10 ** PUSD_DECIMALS), 4)


def _parse_order_amount(raw: Any) -> float:
    """
    Safely parse `making_amount` / `taking_amount` from order submission responses:
    - Human-readable decimals ("5.12", "8", 8.0) stay as-is (NEVER divide "8" shares by 10^6!).
    - Only raw 6-decimal integer strings/ints >= 100,000 (e.g. "5120000") are scaled by 10^6.
    """
    if raw in (None, ""):
        return 0.0
    s = str(raw).strip()
    if not s:
        return 0.0
    v = _to_float(s, 0.0)
    if v <= 0.0:
        return 0.0
    if "." in s:
        return round(v, 4)
    return round(v / (10 ** PUSD_DECIMALS), 4) if v >= 100_000 else round(v, 4)


def _extract_page_items(page: Any) -> list:
    if page is None:
        return []
    if isinstance(page, list):
        return page
    for attr in ("items", "data", "results", "orders", "trades"):
        val = getattr(page, attr, None)
        if isinstance(val, list):
            return val
        if isinstance(page, dict) and isinstance(page.get(attr), list):
            return page[attr]
    return []


def _format_missing_approvals(approvals_state: Any) -> tuple[bool, list[str]]:
    if approvals_state is None:
        return False, []
    fully = bool(_get_field(approvals_state, "is_fully_approved", "isFullyApproved", "approved", default=False))
    missing = _get_field(approvals_state, "missing", default=None)
    items: list[str] = []
    if missing is not None:
        erc20 = _get_field(missing, "erc20", default=[]) or []
        erc1155 = _get_field(missing, "erc1155", default=[]) or []
        if erc20:
            items.append(f"pUSD 额度授权 {len(erc20)} 项")
        if erc1155:
            items.append(f"持仓代币授权 {len(erc1155)} 项")
    if items:
        fully = False
    return fully, items


def _parse_session_start_from_slug(slug: str) -> Optional[int]:
    if not slug:
        return None
    m = _SLUG_TS_RE.search(str(slug))
    return int(m.group(1)) if m else None


def _unwrap_stream_event(ev: Any) -> tuple[str, Any]:
    """
    Unwrap `polymarket-client` UserSpec stream wrapper objects such as:
      - UserTradeEvent(trade=...) / UserOrderEvent(order=...)
      - StreamEvent(event_type=..., data=...) / dict({"type": ..., "payload": ...})
    """
    if ev is None:
        return "", None
    hint = str(_get_field(ev, "event_type", "type", default=type(ev).__name__) or "").lower()
    for attr in ("trade", "order", "data", "payload", "event"):
        inner = _get_field(ev, attr, default=None)
        if inner is not None and not isinstance(inner, (str, int, float, bool)):
            inner_hint = str(_get_field(inner, "event_type", "type", default=attr) or attr).lower()
            combined = f"{hint}:{inner_hint}" if hint and hint != inner_hint else inner_hint
            return combined, inner
    return hint, ev


async def _call_sdk_order_method(
    client: Any,
    method_names: tuple[str, ...],
    order_id: str,
) -> tuple[bool, Any, str]:
    """
    Signature-aware SDK invoker for `cancel_order` / `get_order` / `cancel_orders`:
    Detects keyword-only vs positional signatures (`order_id=`, `id=`, `order_ids=[...]`)
    and NEVER swallows errors silently. Returns (ok, result, error_msg).
    """
    last_err = ""
    found_any = False
    for name in method_names:
        fn = getattr(client, name, None)
        if not callable(fn):
            continue
        found_any = True

        attempts: list[tuple[tuple, dict]] = []
        try:
            sig = inspect.signature(fn)
            params = list(sig.parameters.values())
            pnames = [p.name for p in params]
            if "order_ids" in pnames:
                attempts.append(((), {"order_ids": [order_id]}))
            if "order_id" in pnames:
                p = sig.parameters["order_id"]
                if p.kind == inspect.Parameter.POSITIONAL_ONLY:
                    attempts.append(((order_id,), {}))
                else:
                    attempts.append(((), {"order_id": order_id}))
            elif "id" in pnames:
                p = sig.parameters["id"]
                if p.kind == inspect.Parameter.POSITIONAL_ONLY:
                    attempts.append(((order_id,), {}))
                else:
                    attempts.append(((), {"id": order_id}))
            elif params and params[0].kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                attempts.append(((order_id,), {}))
        except Exception:
            pass

        for fallback in (
            ((), {"order_id": order_id}),
            ((order_id,), {}),
            ((), {"id": order_id}),
            ((), {"order_ids": [order_id]}),
        ):
            if fallback not in attempts:
                attempts.append(fallback)

        for args, kwargs in attempts:
            try:
                res = fn(*args, **kwargs)
                if asyncio.iscoroutine(res):
                    res = await res
                return True, res, ""
            except TypeError as te:
                last_err = f"{name} TypeError: {te}"
                continue
            except Exception as e:
                last_err = f"{name}({type(e).__name__}): {str(e)[:120]}"
                break

    if not found_any:
        last_err = f"SDK 缺少方法 {method_names}"
    return False, None, last_err


def _normalize_wallet_set(wallet_addrs: Union[str, Iterable[str], None]) -> set[str]:
    if not wallet_addrs:
        return set()
    if isinstance(wallet_addrs, str):
        w = wallet_addrs.strip().lower()
        return {w} if w else set()
    return {str(a).strip().lower() for a in wallet_addrs if a and str(a).strip()}


def extract_fills_from_trade(
    trade: Any,
    api_key: str,
    wallet_addrs: Union[str, Iterable[str], None],
) -> list[dict]:
    """
    Exact Polymarket CLOB V2 trade fill extractor:
    - Matches `api_key` OR ANY of our known addresses (EOA + Funder + SDK wallet).
    - Taker side: size-weighted average price across all maker fills.
    - Mint / Merge cross-outcome matching: `1.0 - maker.price` when `maker.asset_id != trade.asset_id`.
    - Maker side: extracts EVERY resting order of ours matched in the trade.
    """
    t_id = str(_get_field(trade, "id", "trade_id", "tradeId", default="") or "")
    t_asset = str(_get_field(trade, "asset_id", "assetId", default="") or "")
    t_side = str(_get_field(trade, "side", default="") or "").upper()
    t_status = str(_get_field(trade, "status", default="MATCHED") or "MATCHED").upper()
    t_owner = str(_get_field(trade, "owner", "api_key", default="") or "")
    t_taker_addr = str(_get_field(trade, "taker_address", "takerAddress", default="") or "").lower()
    t_taker_ord = str(_get_field(trade, "taker_order_id", "takerOrderId", default="") or "")
    t_price = _to_float(_get_field(trade, "price", default=0.0))
    t_size = _to_float(_get_field(trade, "size", default=0.0))
    t_fee_bps = _to_float(_get_field(trade, "fee_rate_bps", "feeRateBps", default=0.0))
    t_ts = _to_float(_get_field(trade, "match_time", "matchTime", "timestamp", default=time.time()))
    if t_ts > 1e12:
        t_ts /= 1000.0

    makers = _get_field(trade, "maker_orders", "makerOrders", default=[]) or []
    addr_set = _normalize_wallet_set(wallet_addrs)
    ak = (api_key or "").strip()
    fills: list[dict] = []

    is_our_taker = bool(
        (ak and t_owner and t_owner == ak)
        or (t_owner and t_owner.lower() in addr_set)
        or (t_taker_addr and t_taker_addr in addr_set)
    )
    if is_our_taker and (t_taker_ord or t_size > 0):
        matched_sz = 0.0
        cost = 0.0
        for m in makers:
            m_sz = _to_float(_get_field(m, "matched_amount", "matchedAmount", "size", default=0.0))
            m_px = _to_float(_get_field(m, "price", default=0.0))
            m_asset = str(_get_field(m, "asset_id", "assetId", default=t_asset) or t_asset)
            if m_sz <= 0.0 or m_px <= 0.0:
                continue
            eff_px = m_px if (not t_asset or m_asset == t_asset) else (1.0 - m_px)
            matched_sz += m_sz
            cost += m_sz * eff_px

        fill_sz = matched_sz if matched_sz > 0 else t_size
        fill_px = (cost / matched_sz) if matched_sz > 0 else t_price
        if fill_sz > 0 and 0.0 < fill_px < 1.0:
            fee_usdc = fill_sz * (t_fee_bps / 10000.0) * fill_px * (1.0 - fill_px) if t_fee_bps > 0 else 0.0
            fills.append({
                "fill_key": f"{t_id}:taker:{t_taker_ord}",
                "trade_id": t_id,
                "order_id": t_taker_ord,
                "asset_id": t_asset,
                "side": t_side,
                "role": "TAKER",
                "shares": round(fill_sz, 4),
                "price": round(fill_px, 4),
                "usdc": round(fill_sz * fill_px, 4),
                "fee_usdc": round(fee_usdc, 4),
                "status": t_status,
                "is_failed": t_status in _FAILED_TRADE_STATUSES,
                "is_confirmed": t_status in _CONFIRMED_TRADE_STATUSES,
                "ts": t_ts,
            })

    for idx, m in enumerate(makers):
        m_owner = str(_get_field(m, "owner", "api_key", default="") or "")
        m_addr = str(_get_field(m, "maker_address", "makerAddress", default="") or "").lower()
        is_our_maker = bool(
            (ak and m_owner and m_owner == ak)
            or (m_owner and m_owner.lower() in addr_set)
            or (m_addr and m_addr in addr_set)
        )
        if not is_our_maker:
            continue
        m_ord = str(_get_field(m, "order_id", "orderId", default="") or "")
        m_sz = _to_float(_get_field(m, "matched_amount", "matchedAmount", "size", default=0.0))
        m_px = _to_float(_get_field(m, "price", default=0.0))
        m_asset = str(_get_field(m, "asset_id", "assetId", default=t_asset) or t_asset)
        m_side = str(_get_field(m, "side", default="") or "").upper()
        if not m_side and t_side:
            if m_asset and t_asset and m_asset != t_asset:
                m_side = t_side
            else:
                m_side = "SELL" if t_side == "BUY" else "BUY"
        if m_sz > 0 and 0.0 < m_px < 1.0:
            fills.append({
                "fill_key": f"{t_id}:maker:{m_ord or idx}",
                "trade_id": t_id,
                "order_id": m_ord,
                "asset_id": m_asset,
                "side": m_side,
                "role": "MAKER",
                "shares": round(m_sz, 4),
                "price": round(m_px, 4),
                "usdc": round(m_sz * m_px, 4),
                "fee_usdc": 0.0,
                "status": t_status,
                "is_failed": t_status in _FAILED_TRADE_STATUSES,
                "is_confirmed": t_status in _CONFIRMED_TRADE_STATUSES,
                "ts": t_ts,
            })

    return fills


class PolymarketAccountManager:
    """
    Hardened Official Polymarket CLOB V2 SDK Account, Asset & Live Execution Manager (v42):
    - Signature-aware SDK invoker (`_call_sdk_order_method`) fixing `cancel_order(order_id=...)` &
      `get_order(order_id=...)` keyword/positional mismatch — ZERO silent `except Exception: pass`!
    - Authoritative triple-check TTL reconciliation (`cancel_order` + `get_order` + `list_account_trades`
      + `list_open_orders` proof): NEVER marks an order `CANCELLED` unless exchange proves it is gone
      with 0 matched shares! If cancel fails, emits `cancel_failed` so the round stays locked.
    - Unwrapped `UserSpec` WS stream (supports both context-manager & direct async-iterator handles,
      unwraps `UserTradeEvent` / `UserOrderEvent` / `StreamEvent` payloads, matches EOA + Funder + SDK wallet).
    - Periodic `reconcile_pending_orders()` and pre-flight `check_round_exchange_exposure()` for
      hard per-round capital circuit breaking on live orders.
    """

    ACCOUNT_CACHE_SEC = 2.0
    FILLS_SYNC_SEC = 20.0

    def __init__(
        self,
        db: Database,
        clock: Optional[ServerClock] = None,
        on_live_order_update: Optional[Callable[[dict], None]] = None,
    ):
        self.db = db
        self.clock = clock or ServerClock()
        self.on_live_order_update = on_live_order_update
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None

        self._sdk_loop: Optional[asyncio.AbstractEventLoop] = None
        self._sdk_thread: Optional[threading.Thread] = None
        self._sdk_ready = threading.Event()
        self._client: Any = None
        self._client_lock: Optional[asyncio.Lock] = None
        self._user_ws_task: Any = None
        self._stop_flag = False

        self._fills: dict[str, dict] = {}
        self._last_positions: list[dict] = []
        self._last_account_refresh_ts: float = 0.0
        self._last_cache_key: str = ""
        self._local_reserved_usdc: float = 0.0
        self._conditioned_approved_tokens: set[str] = set()

        self.live_channel_status: dict = {
            "sdk_available": self._check_sdk_installed(),
            "user_ws_connected": False,
            "last_event_ts": 0.0,
            "open_orders_count": 0,
            "last_error": "",
            "last_cancel_error": "",
            "last_trade_sync_error": "",
            "sdk_methods": {},
        }

        self.cached_portfolio: dict = {
            "wallet_address": "",
            "funder_address": "",
            "wallet_type": "DEPOSIT_WALLET",
            "cash": 0.0,
            "available_cash": 0.0,
            "pusd_balance": 0.0,
            "usdc_balance": 0.0,
            "legacy_usdce_balance": 0.0,
            "portfolio_value": 0.0,
            "total_equity": 0.0,
            "open_orders_locked_usdc": 0.0,
            "local_reserved_usdc": 0.0,
            "cash_verified": False,
            "allowance_usdc": 0.0,
            "allowance_ok": False,
            "sell_allowance_ok": False,
            "approved": False,
            "missing_approvals": [],
            "open_orders": [],
            "open_positions": [],
            "updated_at": 0.0,
            "error": "",
        }

    @staticmethod
    def _check_sdk_installed() -> bool:
        try:
            import polymarket  # noqa: F401
            return True
        except Exception:
            return False

    def _inspect_sdk_methods(self, client: Any) -> dict[str, str]:
        out: dict[str, str] = {}
        for m in (
            "place_limit_order",
            "place_market_order",
            "cancel_order",
            "cancel_orders",
            "cancel_all",
            "cancel_all_orders",
            "get_order",
            "list_open_orders",
            "list_account_trades",
            "subscribe",
        ):
            fn = getattr(client, m, None)
            if callable(fn):
                try:
                    out[m] = str(inspect.signature(fn))
                except Exception:
                    out[m] = "(...)"
        return out

    def start_background(self, main_loop: asyncio.AbstractEventLoop):
        self._main_loop = main_loop
        self._stop_flag = False
        if self._sdk_thread and self._sdk_thread.is_alive():
            return
        self._sdk_ready.clear()
        self._sdk_thread = threading.Thread(target=self._run_sdk_thread, name="poly-sdk-v2", daemon=True)
        self._sdk_thread.start()
        self._sdk_ready.wait(timeout=5.0)
        self.restart_user_stream()

    def stop_background(self):
        self._stop_flag = True
        if self._sdk_loop and not self._sdk_loop.is_closed():
            async def _shutdown():
                if self._user_ws_task:
                    self._user_ws_task.cancel()
                if self._client is not None:
                    try:
                        close_fn = getattr(self._client, "close", None) or getattr(self._client, "aclose", None)
                        if close_fn:
                            res = close_fn()
                            if asyncio.iscoroutine(res):
                                await res
                    except Exception:
                        pass
                    self._client = None
                self._sdk_loop.stop()
            try:
                asyncio.run_coroutine_threadsafe(_shutdown(), self._sdk_loop)
            except Exception:
                pass

    def _run_sdk_thread(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._sdk_loop = loop
        self._client_lock = asyncio.Lock()
        self._sdk_ready.set()
        try:
            loop.run_forever()
        finally:
            try:
                pending = asyncio.all_tasks(loop)
                for t in pending:
                    t.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            loop.close()

    async def _run_on_sdk(self, coro, timeout: float = 20.0):
        if not self._sdk_loop or self._sdk_loop.is_closed():
            self.start_background(asyncio.get_running_loop())
        fut = asyncio.run_coroutine_threadsafe(coro, self._sdk_loop)
        return await asyncio.wait_for(asyncio.wrap_future(fut), timeout=timeout)

    def _emit_live_update(self, payload: dict):
        if not self.on_live_order_update:
            return
        if self._main_loop and not self._main_loop.is_closed():
            self._main_loop.call_soon_threadsafe(self.on_live_order_update, payload)
        else:
            try:
                self.on_live_order_update(payload)
            except Exception:
                pass

    # ---------------- Wallet Config & Cross-Platform AES-256-GCM Storage ----------------

    def get_public_wallet_info(self) -> dict:
        cfg = self.db.get_json("wallet_config", {})
        return {
            "signature_type": int(cfg.get("signature_type", 3)),
            "wallet_type": str(cfg.get("wallet_type") or "DEPOSIT_WALLET"),
            "funder_address": cfg.get("funder_address", ""),
            "wallet_address": mask_address(cfg.get("wallet_address", "")),
            "wallet_address_full": cfg.get("wallet_address", ""),
            "has_private_key": bool(cfg.get("has_private_key", False)),
            "has_api_creds": bool(cfg.get("has_api_creds", False)),
            "is_observer_only": bool(cfg.get("is_observer_only", False)),
            "sdk_available": self.live_channel_status["sdk_available"],
            "user_ws_connected": self.live_channel_status["user_ws_connected"],
            "live_error": self.live_channel_status.get("last_cancel_error") or self.live_channel_status.get("last_error") or "",
        }

    def get_decrypted_api_creds(self) -> Optional[dict]:
        enc = self.db.get_kv("enc_api_creds")
        if not enc:
            return None
        try:
            return json.loads(decrypt_secret(enc))
        except Exception:
            return None

    def _get_decrypted_pk(self) -> str:
        enc = self.db.get_kv("enc_private_key")
        return decrypt_secret(enc) if enc else ""

    def _effective_trading_wallet(self, cfg: dict) -> str:
        funder = (cfg.get("funder_address") or "").strip()
        eoa = (cfg.get("wallet_address") or "").strip()
        wtype = str(cfg.get("wallet_type") or "").upper()
        if wtype == "EOA" or int(cfg.get("signature_type", 3)) == 0:
            return eoa
        return funder or eoa

    def _all_known_wallet_addrs(self, cfg: Optional[dict] = None, client: Any = None) -> set[str]:
        c = cfg if cfg is not None else self.db.get_json("wallet_config", {})
        addrs = {
            str(c.get("wallet_address") or "").strip().lower(),
            str(c.get("funder_address") or "").strip().lower(),
        }
        if client is not None:
            for attr in ("wallet", "wallet_address", "address", "funder", "signer_address"):
                val = getattr(client, attr, None)
                if isinstance(val, str) and val.startswith("0x"):
                    addrs.add(val.strip().lower())
        addrs.discard("")
        return addrs

    def configure_wallet(
        self,
        private_key: Optional[str],
        signature_type: int = 3,
        funder_address: str = "",
        is_observer_only: bool = False,
        wallet_type: str = "",
    ) -> dict:
        from eth_account import Account

        cfg = self.db.get_json("wallet_config", {})
        pk = (private_key or "").strip()
        if pk:
            if not pk.startswith("0x"):
                pk = "0x" + pk
            acct = Account.from_key(pk)
            wallet_addr = acct.address
            self.db.set_kv("enc_private_key", encrypt_secret(pk))
            cfg["wallet_address"] = wallet_addr
            cfg["has_private_key"] = True
            cfg["is_observer_only"] = is_observer_only
        elif not cfg.get("has_private_key"):
            raise ValueError("首次配置必须提供私钥，或点击生成零余额观察凭证")

        sig_map = {0: "EOA", 1: "POLY_PROXY", 2: "GNOSIS_SAFE", 3: "DEPOSIT_WALLET"}
        cfg["signature_type"] = int(signature_type)
        cfg["wallet_type"] = wallet_type or sig_map.get(int(signature_type), "DEPOSIT_WALLET")
        cfg["funder_address"] = (funder_address or "").strip()
        self.db.set_json("wallet_config", cfg)
        self._invalidate_client()
        return self.get_public_wallet_info()

    def _invalidate_client(self):
        self._conditioned_approved_tokens.clear()
        self._last_account_refresh_ts = 0.0
        if self._sdk_loop and not self._sdk_loop.is_closed():
            async def _drop():
                if self._client is not None:
                    try:
                        close_fn = getattr(self._client, "close", None) or getattr(self._client, "aclose", None)
                        if close_fn:
                            r = close_fn()
                            if asyncio.iscoroutine(r):
                                await r
                    except Exception:
                        pass
                    self._client = None
            try:
                asyncio.run_coroutine_threadsafe(_drop(), self._sdk_loop)
            except Exception:
                pass
        else:
            self._client = None

    # ---------------- Channel 3: Multi-RPC On-Chain pUSD & 5-Wallet Auto-Detect ----------------

    async def _rpc_eth_call(self, session: aiohttp.ClientSession, to: str, data: str) -> int:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [{"to": to, "data": data}, "latest"],
        }
        last_exc: Optional[Exception] = None
        for rpc_url in POLYGON_RPC_URLS:
            try:
                async with session.post(
                    rpc_url,
                    json=payload,
                    proxy=POLY_PROXY or None,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as r:
                    if r.status != 200:
                        continue
                    res = await r.json(content_type=None)
                    if "error" in res and "result" not in res:
                        continue
                    hex_val = res.get("result") or "0x0"
                    return int(hex_val, 16) if hex_val and hex_val != "0x" else 0
            except Exception as e:
                last_exc = e
                continue
        if last_exc:
            raise last_exc
        return 0

    async def _query_erc20_balance(self, session: aiohttp.ClientSession, token_contract: str, address: str) -> float:
        if not address or not address.startswith("0x") or len(address) != 42:
            return 0.0
        try:
            calldata = "0x70a08231" + address[2:].lower().rjust(64, "0")
            raw = await self._rpc_eth_call(session, token_contract, calldata)
            return round(raw / (10 ** PUSD_DECIMALS), 4)
        except Exception:
            return 0.0

    def _derive_candidate_wallets(self, private_key: str, manual_funder: str = "") -> list[dict]:
        from eth_account import Account
        eoa = Account.from_key(private_key).address
        candidates: list[dict] = []
        seen: set[str] = set()

        def _add(addr: str, wtype: str, sig_type: int):
            if addr and addr.startswith("0x") and len(addr) == 42:
                k = addr.lower()
                if k not in seen:
                    seen.add(k)
                    candidates.append({"address": addr, "wallet_type": wtype, "signature_type": sig_type})

        if manual_funder:
            _add(manual_funder.strip(), "CUSTOM_FUNDER", 3)

        try:
            import polymarket as pm
            for mod_name in ("wallets", "wallet", "auth", "signer", "crypto"):
                mod = getattr(pm, mod_name, None)
                if mod is None:
                    continue
                for fn_name, wt, st in (
                    ("derive_deposit_wallet", "DEPOSIT_WALLET", 3),
                    ("derive_deposit_address", "DEPOSIT_WALLET", 3),
                    ("derive_poly_proxy", "POLY_PROXY", 1),
                    ("derive_proxy_wallet", "POLY_PROXY", 1),
                    ("derive_safe_wallet", "GNOSIS_SAFE", 2),
                    ("derive_gnosis_safe", "GNOSIS_SAFE", 2),
                ):
                    fn = getattr(mod, fn_name, None)
                    if callable(fn):
                        try:
                            addr = fn(eoa)
                            if isinstance(addr, str):
                                _add(addr, wt, st)
                        except Exception:
                            pass
        except Exception:
            pass

        _add(eoa, "EOA", 0)
        return candidates

    async def detect_wallet(self, private_key: Optional[str] = None, auto_apply: bool = True) -> dict:
        pk = (private_key or "").strip()
        if pk and not pk.startswith("0x"):
            pk = "0x" + pk
        if not pk:
            pk = self._get_decrypted_pk()
        if not pk:
            raise ValueError("请先输入或保存私钥后再自动识别钱包")

        cfg = self.db.get_json("wallet_config", {})
        candidates = self._derive_candidate_wallets(pk, cfg.get("funder_address", ""))

        async with aiohttp.ClientSession(headers=POLY_HEADERS) as session:
            async def _score_one(c: dict) -> dict:
                addr = c["address"]
                pusd_bal, usdce_bal = await asyncio.gather(
                    self._query_erc20_balance(session, PUSD_CONTRACT, addr),
                    self._query_erc20_balance(session, USDCE_CONTRACT, addr),
                )
                has_profile = False
                has_activity = False
                discovered_proxy = ""
                try:
                    async with session.get(
                        f"{GAMMA_API_BASE}/public-profile",
                        params={"address": addr},
                        proxy=POLY_PROXY or None,
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as r:
                        if r.status == 200:
                            prof = await r.json()
                            if isinstance(prof, dict) and (prof.get("proxyWallet") or prof.get("name") or prof.get("pseudonym")):
                                has_profile = True
                                pw = str(prof.get("proxyWallet") or "").strip()
                                if pw.startswith("0x") and len(pw) == 42 and pw.lower() != addr.lower():
                                    discovered_proxy = pw
                except Exception:
                    pass
                try:
                    async with session.get(
                        f"{DATA_API_BASE}/positions",
                        params={"user": addr, "sizeThreshold": "0"},
                        proxy=POLY_PROXY or None,
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as r:
                        if r.status == 200:
                            pos = await r.json()
                            if isinstance(pos, list) and len(pos) > 0:
                                has_activity = True
                except Exception:
                    pass

                score = (4 if pusd_bal > 0 else 0) + (2 if has_profile else 0) + (1 if has_activity else 0)
                return {
                    **c,
                    "pusd_balance": pusd_bal,
                    "legacy_usdce_balance": usdce_bal,
                    "has_profile": has_profile,
                    "has_activity": has_activity,
                    "profile_proxy_wallet": discovered_proxy,
                    "score": score,
                }

            scored = list(await asyncio.gather(*(_score_one(c) for c in candidates)))
            extra_addrs = {
                s["profile_proxy_wallet"] for s in scored
                if s.get("profile_proxy_wallet") and s["profile_proxy_wallet"].lower() not in {x["address"].lower() for x in scored}
            }
            for ea in extra_addrs:
                scored.append(await _score_one({"address": ea, "wallet_type": "DEPOSIT_WALLET", "signature_type": 3}))

        best = max(scored, key=lambda x: (x["score"], x["pusd_balance"], x["legacy_usdce_balance"])) if scored else None

        if auto_apply and best:
            funder = "" if best["wallet_type"] == "EOA" else best["address"]
            self.configure_wallet(
                private_key=pk if private_key else None,
                signature_type=int(best["signature_type"]),
                funder_address=funder,
                is_observer_only=False,
                wallet_type=best["wallet_type"],
            )

        return {
            "ok": True,
            "selected": best,
            "candidates": scored,
            "wallet": self.get_public_wallet_info(),
        }

    # ---------------- Channel 2: Read-Only Address Positions Lookup ----------------

    async def lookup_address_positions(self, user_address: str, condition_ids: Optional[list[str]] = None) -> list[dict]:
        addr = (user_address or "").strip()
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", addr):
            raise ValueError("钱包地址格式错误，应为 0x 开头的 40 位十六进制")
        params: dict[str, Any] = {"user": addr, "sizeThreshold": "0"}
        if condition_ids:
            params["market"] = ",".join(c for c in condition_ids if c)
        async with aiohttp.ClientSession(headers=POLY_HEADERS) as session:
            async with session.get(
                f"{DATA_API_BASE}/positions",
                params=params,
                proxy=POLY_PROXY or None,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"data-api 查询失败 (HTTP {resp.status})")
                rows = await resp.json()
                return rows if isinstance(rows, list) else []

    # ---------------- SDK Client Lifecycle (Runs on Dedicated SDK Thread) ----------------

    async def _ensure_sdk_client(self):
        if not self.live_channel_status["sdk_available"]:
            raise RuntimeError("未安装官方 CLOB V2 SDK (polymarket-client>=0.12.0)，请执行 bash scripts/deploy.sh 自动安装")
        async with self._client_lock:
            if self._client is not None:
                return self._client

            pk = self._get_decrypted_pk()
            if not pk:
                raise RuntimeError("尚未配置钱包私钥")
            cfg = self.db.get_json("wallet_config", {})
            saved_creds = self.get_decrypted_api_creds()
            wallet_addr = self._effective_trading_wallet(cfg)

            from polymarket import AsyncSecureClient
            try:
                from polymarket import ApiKeyCreds
            except ImportError:
                from polymarket.types import ApiKeyCreds  # type: ignore

            creds_obj = None
            if saved_creds and saved_creds.get("apiKey"):
                try:
                    creds_obj = ApiKeyCreds.model_validate({
                        "api_key": saved_creds["apiKey"],
                        "apiKey": saved_creds["apiKey"],
                        "secret": saved_creds["secret"],
                        "api_secret": saved_creds["secret"],
                        "passphrase": saved_creds["passphrase"],
                        "api_passphrase": saved_creds["passphrase"],
                    })
                except Exception:
                    try:
                        creds_obj = ApiKeyCreds(
                            api_key=saved_creds["apiKey"],
                            secret=saved_creds["secret"],
                            passphrase=saved_creds["passphrase"],
                        )
                    except Exception:
                        creds_obj = None

            create_kwargs: dict[str, Any] = {"private_key": pk}
            if wallet_addr:
                create_kwargs["wallet"] = wallet_addr
            if creds_obj is not None:
                create_kwargs["credentials"] = creds_obj

            try:
                client = await AsyncSecureClient.create(**create_kwargs)
            except Exception:
                create_kwargs.pop("credentials", None)
                client = await AsyncSecureClient.create(**create_kwargs)

            self._client = client
            self.live_channel_status["sdk_methods"] = self._inspect_sdk_methods(client)
            return client

    async def derive_api_credentials_async(self) -> dict:
        async def _do():
            self._client = None
            client = await self._ensure_sdk_client()
            raw_creds = _get_field(client, "credentials", "api_creds", "creds")
            if raw_creds is None:
                derive_fn = getattr(client, "create_or_derive_api_creds", None)
                if callable(derive_fn):
                    res = derive_fn()
                    raw_creds = await res if asyncio.iscoroutine(res) else res

            api_key = str(_get_field(raw_creds, "api_key", "apiKey", "key", default="") or "")
            secret = str(_get_field(raw_creds, "secret", "api_secret", "apiSecret", default="") or "")
            passphrase = str(_get_field(raw_creds, "passphrase", "api_passphrase", "apiPassphrase", default="") or "")
            if not api_key or not secret or not passphrase:
                raise RuntimeError("派生 CLOB V2 API 凭证失败，请检查私钥、代理或地区限制")
            return {"apiKey": api_key, "secret": secret, "passphrase": passphrase}

        creds = await self._run_on_sdk(_do(), timeout=25.0)
        self.db.set_kv("enc_api_creds", encrypt_secret(json.dumps(creds)))
        cfg = self.db.get_json("wallet_config", {})
        cfg["has_api_creds"] = True
        self.db.set_json("wallet_config", cfg)
        self.restart_user_stream()
        return {
            "ok": True,
            "api_key_masked": f"{creds['apiKey'][:8]}...{creds['apiKey'][-4:]}",
            "wallet": self.get_public_wallet_info(),
        }

    async def generate_observer_wallet_async(self) -> dict:
        from eth_account import Account
        acct = Account.create()
        self.configure_wallet(acct.key.hex(), signature_type=0, funder_address="", is_observer_only=True, wallet_type="EOA")
        return await self.derive_api_credentials_async()

    # ---------------- Real-Time SDK UserSpec WebSocket & REST Trade/Order Reconciliation ----------------

    def restart_user_stream(self):
        if self._stop_flag or not self._sdk_loop or self._sdk_loop.is_closed():
            return
        cfg = self.db.get_json("wallet_config", {})
        if not cfg.get("has_private_key") or cfg.get("is_observer_only") or not self.live_channel_status["sdk_available"]:
            return

        async def _restart():
            if self._user_ws_task and not self._user_ws_task.done():
                self._user_ws_task.cancel()
            self._user_ws_task = asyncio.create_task(self._user_ws_loop())

        asyncio.run_coroutine_threadsafe(_restart(), self._sdk_loop)

    def _merge_fills(self, new_fills: list[dict]):
        touched_orders: set[str] = set()
        failed_orders: dict[str, dict] = {}
        for f in new_fills:
            oid = str(f.get("order_id") or "")
            fkey = f.get("fill_key")
            if not fkey:
                continue
            if oid and not str(fkey).startswith("local:"):
                self._fills.pop(f"local:{oid}", None)
            if f.get("is_failed") or str(f.get("status") or "").upper() in _FAILED_TRADE_STATUSES:
                self._fills.pop(fkey, None)
                if oid:
                    failed_orders[oid] = f
                continue
            self._fills[fkey] = f
            if oid:
                touched_orders.add(oid)

        for oid, ff in failed_orders.items():
            agg_rem = self.get_order_fill_summary(oid)
            if agg_rem["shares"] <= 0:
                self._emit_live_update({
                    "event": "trade_failed",
                    "order_id": oid,
                    "trade_id": ff.get("trade_id") or "",
                    "side": ff.get("side") or "",
                    "status": ff.get("status") or "FAILED",
                    "reason": f"链上结算失败({ff.get('status') or 'FAILED'})",
                    "ts": time.time(),
                })

        for oid in touched_orders:
            agg = self.get_order_fill_summary(oid)
            if agg["shares"] > 0:
                self._emit_live_update({
                    "event": "fill",
                    "order_id": oid,
                    "shares": agg["shares"],
                    "fill_price": agg["avg_price"],
                    "size_usdc": agg["usdc"],
                    "fee_usdc": agg["fee_usdc"],
                    "side": agg["side"],
                    "status": agg["status"],
                    "confirmed": agg["confirmed"],
                    "ts": agg["ts"],
                })

    def get_order_fill_summary(self, order_id: str) -> dict:
        matched = [
            f for f in self._fills.values()
            if f.get("order_id") == order_id
            and not f.get("is_failed")
            and str(f.get("status") or "").upper() not in _FAILED_TRADE_STATUSES
        ]
        if not matched:
            return {"shares": 0.0, "avg_price": 0.0, "usdc": 0.0, "fee_usdc": 0.0, "side": "", "status": "", "confirmed": False, "ts": 0.0}
        real = [f for f in matched if not str(f.get("fill_key", "")).startswith("local:")]
        use = real if real else matched
        tot_sh = sum(float(f["shares"]) for f in use)
        tot_usdc = sum(float(f["usdc"]) for f in use)
        tot_fee = sum(float(f.get("fee_usdc") or 0.0) for f in use)
        avg_px = (tot_usdc / tot_sh) if tot_sh > 0 else 0.0
        latest = max(use, key=lambda x: float(x.get("ts") or 0.0))
        confirmed = any(bool(f.get("is_confirmed")) or str(f.get("status") or "").upper() in _CONFIRMED_TRADE_STATUSES for f in real)
        return {
            "shares": round(tot_sh, 4),
            "avg_price": round(avg_px, 4),
            "usdc": round(tot_usdc, 4),
            "fee_usdc": round(tot_fee, 4),
            "side": latest.get("side") or "",
            "status": latest.get("status") or "MATCHED",
            "confirmed": confirmed,
            "ts": float(latest.get("ts") or time.time()),
        }

    def _process_user_stream_event(self, ev: Any, api_key: str, wallet_addrs: set[str]):
        self.live_channel_status["last_event_ts"] = time.time()
        etype, payload = _unwrap_stream_event(ev)
        if payload is None:
            return

        if "trade" in etype or _get_field(payload, "maker_orders", "makerOrders", "taker_order_id", "takerOrderId") is not None:
            fills = extract_fills_from_trade(payload, api_key, wallet_addrs)
            if fills:
                self._merge_fills(fills)
            return

        if "order" in etype or _get_field(payload, "original_size", "size_matched", "sizeMatched") is not None:
            oid = str(_get_field(payload, "id", "order_id", "orderID", default="") or "")
            if not oid:
                return
            ost = str(_get_field(payload, "status", "type", default="") or "").upper()
            sz_m = _to_float(_get_field(payload, "size_matched", "sizeMatched", default=0.0))
            px = _to_float(_get_field(payload, "price", default=0.0))
            side_s = str(_get_field(payload, "side", default="") or "").upper()
            if sz_m > 0.0:
                agg = self.get_order_fill_summary(oid)
                self._emit_live_update({
                    "event": "fill",
                    "order_id": oid,
                    "shares": max(round(sz_m, 4), agg["shares"]),
                    "fill_price": agg["avg_price"] or round(px, 4),
                    "size_usdc": round(max(sz_m, agg["shares"]) * (agg["avg_price"] or px), 4),
                    "fee_usdc": agg["fee_usdc"],
                    "side": side_s or agg["side"],
                    "status": ost or "MATCHED",
                    "ts": time.time(),
                })
            elif ost in _TERMINAL_CANCEL_STATUSES:
                self._emit_live_update({
                    "event": "order_closed",
                    "order_id": oid,
                    "exchange_status": ost,
                    "size_matched": 0.0,
                    "price": round(px, 4),
                    "cancel_confirmed": True,
                    "ts": time.time(),
                })

    async def _user_ws_loop(self):
        backoff = 1.5
        while not self._stop_flag:
            cfg = self.db.get_json("wallet_config", {})
            if not cfg.get("has_private_key") or cfg.get("is_observer_only"):
                self.live_channel_status["user_ws_connected"] = False
                await asyncio.sleep(3.0)
                continue
            try:
                client = await self._ensure_sdk_client()
                try:
                    from polymarket.streams import UserSpec
                except ImportError:
                    from polymarket import UserSpec  # type: ignore

                creds = self.get_decrypted_api_creds() or {}
                api_key = creds.get("apiKey", "")
                wallet_addrs = self._all_known_wallet_addrs(cfg, client)

                sub_res = client.subscribe(UserSpec())
                handle = await sub_res if asyncio.iscoroutine(sub_res) else sub_res

                async def _consume(stream_obj):
                    self.live_channel_status.update({"user_ws_connected": True, "last_error": ""})
                    nonlocal backoff
                    backoff = 1.5
                    it = stream_obj
                    if not hasattr(it, "__aiter__"):
                        for m_name in ("events", "stream", "messages"):
                            cand = getattr(stream_obj, m_name, None)
                            if callable(cand):
                                it = cand()
                                break
                    async for ev in it:
                        if self._stop_flag:
                            break
                        self._process_user_stream_event(ev, api_key, wallet_addrs)

                if hasattr(handle, "__aenter__") and not hasattr(handle, "__aiter__"):
                    async with handle as entered:
                        await _consume(entered)
                else:
                    try:
                        await _consume(handle)
                    finally:
                        close_h = getattr(handle, "close", None) or getattr(handle, "aclose", None)
                        if callable(close_h):
                            try:
                                r = close_h()
                                if asyncio.iscoroutine(r):
                                    await r
                            except Exception:
                                pass
            except asyncio.CancelledError:
                self.live_channel_status["user_ws_connected"] = False
                return
            except Exception as e:
                self.live_channel_status["user_ws_connected"] = False
                self.live_channel_status["last_error"] = f"UserWS: {str(e)[:80]}"
                log.warning("UserSpec WS error: %s", e)
                await asyncio.sleep(backoff)
                backoff = min(15.0, backoff * 1.6)

    async def _fetch_account_trades_sdk(self, client: Any, condition_id: Optional[str] = None) -> list:
        fn = getattr(client, "list_account_trades", None) or getattr(client, "get_trades", None)
        if not callable(fn):
            self.live_channel_status["last_trade_sync_error"] = "SDK 缺少 list_account_trades"
            return []

        attempts: list[dict] = []
        if condition_id:
            try:
                pnames = set(inspect.signature(fn).parameters.keys())
                if "condition_id" in pnames:
                    attempts.append({"condition_id": condition_id})
                elif "market" in pnames:
                    attempts.append({"market": condition_id})
            except Exception:
                attempts.append({"market": condition_id})
        attempts.append({})

        last_err = ""
        for kw in attempts:
            try:
                res = fn(**kw)
                if hasattr(res, "first_page"):
                    page = await res.first_page()
                    self.live_channel_status["last_trade_sync_error"] = ""
                    return _extract_page_items(page)
                if asyncio.iscoroutine(res):
                    out = await res
                    self.live_channel_status["last_trade_sync_error"] = ""
                    return _extract_page_items(out)
                self.live_channel_status["last_trade_sync_error"] = ""
                return _extract_page_items(res)
            except TypeError as te:
                last_err = str(te)
                continue
            except Exception as e:
                last_err = f"{type(e).__name__}: {str(e)[:90]}"
                break
        if last_err:
            self.live_channel_status["last_trade_sync_error"] = last_err
        return []

    async def _fetch_open_orders_sdk(self, client: Any) -> tuple[bool, list, str]:
        fn = getattr(client, "list_open_orders", None) or getattr(client, "get_orders", None)
        if not callable(fn):
            return False, [], "SDK 缺少 list_open_orders"
        try:
            res = fn()
            if hasattr(res, "first_page"):
                page = await res.first_page()
                return True, _extract_page_items(page), ""
            if asyncio.iscoroutine(res):
                out = await res
                return True, _extract_page_items(out), ""
            return True, _extract_page_items(res), ""
        except Exception as e:
            return False, [], f"list_open_orders({type(e).__name__}): {str(e)[:90]}"

    async def sync_account_trades(self, condition_ids: Optional[list[str]] = None) -> int:
        cfg = self.db.get_json("wallet_config", {})
        if not cfg.get("has_private_key") or cfg.get("is_observer_only") or not self.live_channel_status["sdk_available"]:
            return 0
        creds = self.get_decrypted_api_creds() or {}
        api_key = creds.get("apiKey", "")

        async def _fetch():
            client = await self._ensure_sdk_client()
            cid = condition_ids[0] if condition_ids else None
            trades = await self._fetch_account_trades_sdk(client, condition_id=cid)
            return trades, self._all_known_wallet_addrs(cfg, client)

        try:
            trades, wallet_addrs = await self._run_on_sdk(_fetch(), timeout=12.0)
            all_fills: list[dict] = []
            for t in (trades or []):
                all_fills.extend(extract_fills_from_trade(t, api_key, wallet_addrs))
            if all_fills:
                self._merge_fills(all_fills)
            return len(all_fills)
        except Exception as e:
            self.live_channel_status["last_trade_sync_error"] = str(e)[:90]
            return 0

    async def reconcile_pending_orders(
        self,
        pending_orders: list[dict],
        condition_id: Optional[str] = None,
    ) -> dict:
        cfg = self.db.get_json("wallet_config", {})
        if not cfg.get("has_private_key") or cfg.get("is_observer_only") or not self.live_channel_status["sdk_available"]:
            return {"ok": False, "checked": 0, "reason": "未配置实盘SDK"}

        creds = self.get_decrypted_api_creds() or {}
        api_key = creds.get("apiKey", "")

        async def _run():
            client = await self._ensure_sdk_client()
            wallet_addrs = self._all_known_wallet_addrs(cfg, client)

            trades = await self._fetch_account_trades_sdk(client, condition_id=condition_id)
            fresh_fills: list[dict] = []
            for t in (trades or []):
                fresh_fills.extend(extract_fills_from_trade(t, api_key, wallet_addrs))
            if fresh_fills:
                self._merge_fills(fresh_fills)

            oo_ok, oo_items, oo_err = await self._fetch_open_orders_sdk(client)
            open_map: dict[str, Any] = {}
            if oo_ok:
                for o in oo_items:
                    oid_o = str(_get_field(o, "id", "order_id", "orderID", default="") or "")
                    if oid_o:
                        open_map[oid_o] = o

            now_ts = time.time()
            summary = {"ok": True, "checked": 0, "filled": 0, "cancelled": 0, "still_open": 0, "errors": []}
            if not oo_ok and oo_err:
                summary["errors"].append(oo_err)

            for po in pending_orders:
                oid = str(po.get("order_id") or "")
                if not oid:
                    continue
                summary["checked"] += 1
                side_s = str(po.get("side") or "BUY")
                fallback_px = float(po.get("limit_price") or po.get("trigger_px") or 0.5)
                age_sec = now_ts - float(po.get("created_at") or now_ts)
                ttl_sec = max(2.0, float(po.get("ttl_ms") or 8000) / 1000.0)

                still_open = (oid in open_map) if oo_ok else None
                cancel_ok = False
                cancel_err = ""

                if still_open is True and age_sec >= ttl_sec:
                    cancel_ok, _, cancel_err = await _call_sdk_order_method(
                        client, ("cancel_order", "cancel", "cancel_orders"), oid
                    )
                    if cancel_ok:
                        await asyncio.sleep(0.35)
                        oo_ok2, oo_items2, _ = await self._fetch_open_orders_sdk(client)
                        if oo_ok2:
                            open_ids2 = {str(_get_field(o, "id", "order_id", "orderID", default="") or "") for o in oo_items2}
                            still_open = oid in open_ids2
                    elif cancel_err:
                        summary["errors"].append(cancel_err)
                        self.live_channel_status["last_cancel_error"] = cancel_err

                go_ok, ord_info, _ = await _call_sdk_order_method(client, ("get_order", "get_open_order"), oid)
                size_matched = 0.0
                final_px = fallback_px
                ex_status = "LIVE" if still_open else "UNKNOWN"
                if go_ok and ord_info is not None:
                    size_matched = _to_float(_get_field(ord_info, "size_matched", "sizeMatched", default=0.0))
                    p_val = _to_float(_get_field(ord_info, "price", default=fallback_px))
                    if 0.0 < p_val < 1.0:
                        final_px = p_val
                    st_raw = str(_get_field(ord_info, "status", default="") or "").upper()
                    if st_raw:
                        ex_status = st_raw
                elif still_open and oid in open_map:
                    size_matched = _to_float(_get_field(open_map[oid], "size_matched", "sizeMatched", default=0.0))

                agg = self.get_order_fill_summary(oid)
                if agg["shares"] > size_matched:
                    size_matched = agg["shares"]
                    final_px = agg["avg_price"] or final_px

                if size_matched > 0.0:
                    summary["filled"] += 1
                    self._emit_live_update({
                        "event": "fill",
                        "order_id": oid,
                        "side": side_s,
                        "shares": round(size_matched, 4),
                        "fill_price": round(final_px, 4),
                        "size_usdc": round(size_matched * final_px, 4),
                        "fee_usdc": agg["fee_usdc"],
                        "status": ex_status,
                        "ts": time.time(),
                    })
                elif still_open is False and (ex_status in _TERMINAL_CANCEL_STATUSES or cancel_ok or age_sec >= ttl_sec + 5.0):
                    summary["cancelled"] += 1
                    self._emit_live_update({
                        "event": "ttl_reconciled",
                        "order_id": oid,
                        "side": side_s,
                        "exchange_status": ex_status if ex_status != "UNKNOWN" else "CANCELLED",
                        "size_matched": 0.0,
                        "fill_price": round(final_px, 4),
                        "fee_usdc": 0.0,
                        "cancel_confirmed": True,
                        "ts": time.time(),
                    })
                else:
                    summary["still_open"] += 1
                    if cancel_err:
                        self._emit_live_update({
                            "event": "cancel_failed",
                            "order_id": oid,
                            "side": side_s,
                            "error": cancel_err,
                            "ts": time.time(),
                        })
            return summary

        try:
            return await self._run_on_sdk(_run(), timeout=18.0)
        except Exception as e:
            return {"ok": False, "checked": 0, "error": str(e)[:100]}

    async def check_round_exchange_exposure(
        self,
        condition_id: Optional[str],
        token_ids: list[str],
    ) -> dict:
        tset = {str(t) for t in token_ids if t}
        cid = str(condition_id or "")
        res = {
            "checked": False,
            "open_orders_count": 0,
            "open_order_ids": [],
            "position_shares": 0.0,
            "position_shares_by_token": {},
            "error": "",
        }
        cfg = self.db.get_json("wallet_config", {})
        if not cfg.get("has_private_key") or cfg.get("is_observer_only") or not self.live_channel_status["sdk_available"]:
            return res

        async def _check():
            client = await self._ensure_sdk_client()
            oo_ok, oo_items, oo_err = await self._fetch_open_orders_sdk(client)
            return oo_ok, oo_items, oo_err

        try:
            oo_ok, oo_items, oo_err = await self._run_on_sdk(_check(), timeout=6.0)
            if oo_ok:
                res["checked"] = True
                for o in oo_items:
                    o_aid = str(_get_field(o, "asset_id", "assetId", default="") or "")
                    o_cid = str(_get_field(o, "market", "condition_id", "conditionId", default="") or "")
                    if (tset and o_aid in tset) or (cid and o_cid == cid):
                        oid = str(_get_field(o, "id", "order_id", "orderID", default="") or "")
                        res["open_order_ids"].append(oid)
                res["open_orders_count"] = len(res["open_order_ids"])
            else:
                res["error"] = oo_err
        except Exception as e:
            res["error"] = str(e)[:80]

        for p in (self.cached_portfolio.get("open_positions") or []):
            p_aid = str(p.get("asset_id") or "")
            p_cid = str(p.get("condition_id") or "")
            sh = float(p.get("current_size") or p.get("size") or 0.0)
            if sh > 0.01 and ((tset and p_aid in tset) or (cid and p_cid == cid)):
                res["position_shares"] += sh
                if p_aid:
                    res["position_shares_by_token"][p_aid] = res["position_shares_by_token"].get(p_aid, 0.0) + sh
        res["position_shares"] = round(res["position_shares"], 4)
        return res

    # ---------------- Real Spendable Available Cash Helpers (v45) ----------------

    def get_available_cash(self) -> float:
        pf = self.cached_portfolio or {}
        if "available_cash" in pf and pf["available_cash"] is not None:
            return round(max(0.0, float(pf["available_cash"])), 4)
        cash = float(pf.get("cash") or pf.get("pusd_balance") or 0.0)
        locked = float(pf.get("open_orders_locked_usdc") or 0.0)
        return round(max(0.0, cash - locked - self._local_reserved_usdc), 4)

    def get_total_equity(self) -> float:
        pf = self.cached_portfolio or {}
        eq = float(pf.get("total_equity") or 0.0)
        return round(max(eq, self.get_available_cash()), 4)

    def reserve_local_cash(self, delta_usdc: float) -> float:
        """
        0ms optimistic local deduction right after a LIVE BUY order is accepted/filled,
        preventing double-spending during the short window before the next SDK refresh settles.
        Automatically cleared when `refresh_balances_and_portfolio()` pulls authoritative exchange cash.
        """
        d = max(0.0, float(delta_usdc))
        if d <= 0.0:
            return self.get_available_cash()
        self._local_reserved_usdc = round(self._local_reserved_usdc + d, 4)
        cash = float(self.cached_portfolio.get("cash") or 0.0)
        locked = float(self.cached_portfolio.get("open_orders_locked_usdc") or 0.0)
        avail = round(max(0.0, cash - locked - self._local_reserved_usdc), 4)
        self.cached_portfolio["local_reserved_usdc"] = self._local_reserved_usdc
        self.cached_portfolio["available_cash"] = avail
        self.cached_portfolio["pusd_balance"] = avail
        self.cached_portfolio["usdc_balance"] = avail
        return avail

    async def ensure_fresh_available_cash(self, max_age_sec: float = 4.0, cache_key: str = "") -> dict:
        """Pre-flight real-time cash gate for LIVE BUY orders: refreshes SDK balance if stale or unverified."""
        now = time.time()
        age = now - float(self.cached_portfolio.get("updated_at") or 0.0)
        verified = bool(self.cached_portfolio.get("cash_verified", False))
        if (not verified) or age > max_age_sec or self._local_reserved_usdc > 0.0:
            await self.refresh_balances_and_portfolio(force=True, cache_key=cache_key)
        pf = self.cached_portfolio
        return {
            "available_cash": self.get_available_cash(),
            "cash": float(pf.get("cash") or 0.0),
            "locked": float(pf.get("open_orders_locked_usdc") or 0.0),
            "total_equity": self.get_total_equity(),
            "cash_verified": bool(pf.get("cash_verified", False)),
            "error": str(pf.get("error") or ""),
        }

    # ---------------- Channel 1: SDK 5-Way Concurrent Account Asset Fetch ----------------

    async def refresh_balances_and_portfolio(self, force: bool = False, cache_key: str = "") -> dict:
        now = time.time()
        key_same = (not cache_key) or (cache_key == self._last_cache_key)
        if not force and key_same and (now - self._last_account_refresh_ts) < self.ACCOUNT_CACHE_SEC and self.cached_portfolio.get("updated_at", 0) > 0:
            return self.cached_portfolio

        cfg = self.db.get_json("wallet_config", {})
        wallet_addr = cfg.get("wallet_address", "")
        target_addr = self._effective_trading_wallet(cfg)
        if not target_addr or not target_addr.startswith("0x") or len(target_addr) != 42:
            self.cached_portfolio.update({"error": "未配置有效钱包地址", "updated_at": now})
            return self.cached_portfolio

        pusd_onchain = 0.0
        usdce_onchain = 0.0
        try:
            async with aiohttp.ClientSession(headers=POLY_HEADERS) as session:
                pusd_onchain, usdce_onchain = await asyncio.gather(
                    self._query_erc20_balance(session, PUSD_CONTRACT, target_addr),
                    self._query_erc20_balance(session, USDCE_CONTRACT, target_addr),
                )
        except Exception:
            pass

        prev_cash = float(self.cached_portfolio.get("cash") or 0.0)
        cash = pusd_onchain
        cash_verified = False
        portfolio_val = 0.0
        allowance_usdc = 0.0
        approved = False
        missing_approvals: list[str] = []
        open_orders: list[dict] = []
        open_orders_locked = 0.0
        positions: list[dict] = list(self._last_positions)
        err_parts: list[str] = []

        if self.live_channel_status["sdk_available"] and cfg.get("has_private_key") and not cfg.get("is_observer_only"):
            async def _fetch_sdk():
                c = await self._ensure_sdk_client()

                async def _call_page(fn_name: str, **kw):
                    fn = getattr(c, fn_name, None)
                    if not callable(fn):
                        return None
                    r = fn(**kw)
                    if hasattr(r, "first_page"):
                        return await r.first_page()
                    return await r if asyncio.iscoroutine(r) else r

                async def _call_opt(fn_name: str, **kw):
                    fn = getattr(c, fn_name, None)
                    if not callable(fn):
                        return None
                    r = fn(**kw)
                    return await r if asyncio.iscoroutine(r) else r

                return await asyncio.gather(
                    _call_opt("get_balance_allowance", asset_type="COLLATERAL"),
                    _call_opt("get_portfolio_value"),
                    _call_page("list_open_orders"),
                    _call_opt("get_trading_approvals_state"),
                    _call_page("list_positions", sort_by="TIMESTAMP", sort_direction="DESC", page_size=50),
                    return_exceptions=True,
                )

            try:
                res_bal, res_val, res_oo, res_app, res_pos = await self._run_on_sdk(_fetch_sdk(), timeout=14.0)

                if isinstance(res_bal, Exception):
                    err_parts.append(f"现金获取失败:{str(res_bal)[:45]}")
                    # Keep last known cash on transient error instead of flashing $0, but mark unverified!
                    if pusd_onchain == 0.0 and prev_cash > 0.0:
                        cash = prev_cash
                    cash_verified = False
                elif res_bal is not None:
                    raw_b = _get_field(res_bal, "balance", default=None)
                    cash = _parse_collateral_usdc(raw_b)
                    cash_verified = True
                    allowances = _get_field(res_bal, "allowances", default=None)
                    if isinstance(allowances, dict) and allowances:
                        vals = [_parse_collateral_usdc(v) for v in allowances.values()]
                        allowance_usdc = round(min(max(vals) if vals else 0.0, 1e12), 2)

                if not isinstance(res_val, Exception) and res_val is not None:
                    v_raw = _get_field(res_val, "value", "total_value", "portfolio_value", default=res_val)
                    portfolio_val = round(_to_float(v_raw, 0.0), 4)

                if not isinstance(res_oo, Exception) and res_oo is not None:
                    for o in _extract_page_items(res_oo)[:50]:
                        orig_sz = _to_float(_get_field(o, "original_size", "size", default=0.0))
                        matched_sz = _to_float(_get_field(o, "size_matched", "sizeMatched", default=0.0))
                        px = _to_float(_get_field(o, "price", default=0.0))
                        side_s = str(_get_field(o, "side", default="") or "").upper()
                        rem = max(0.0, orig_sz - matched_sz)
                        if side_s == "BUY":
                            open_orders_locked += rem * px
                        open_orders.append({
                            "order_id": str(_get_field(o, "id", "order_id", default="") or ""),
                            "asset_id": str(_get_field(o, "asset_id", "assetId", default="") or ""),
                            "condition_id": str(_get_field(o, "market", "condition_id", default="") or ""),
                            "outcome": str(_get_field(o, "outcome", default="") or ""),
                            "side": side_s,
                            "price": round(px, 4),
                            "original_size": round(orig_sz, 4),
                            "size_matched": round(matched_sz, 4),
                            "status": str(_get_field(o, "status", default="LIVE") or "LIVE"),
                        })

                if not isinstance(res_app, Exception) and res_app is not None:
                    approved, missing_approvals = _format_missing_approvals(res_app)
                elif allowance_usdc >= 10.0:
                    approved = True

                if isinstance(res_pos, Exception):
                    err_parts.append(f"持仓沿用快照:{str(res_pos)[:30]}")
                    positions = list(self._last_positions)
                elif res_pos is not None:
                    parsed_pos: list[dict] = []
                    pos_val_sum = 0.0
                    for p in _extract_page_items(res_pos)[:50]:
                        size = _to_float(_get_field(p, "current_size", "size", default=0.0))
                        avg_px = _to_float(_get_field(p, "avg_price", "avgPrice", default=0.0))
                        cur_px_raw = _get_field(p, "current_price", "cur_price", "curPrice", default=None)
                        cur_px = round(_to_float(cur_px_raw), 4) if cur_px_raw is not None else None
                        cur_val_raw = _get_field(p, "current_value", "currentValue", default=None)
                        cur_val = round(_to_float(cur_val_raw), 4) if cur_val_raw is not None else (
                            round(size * cur_px, 4) if cur_px is not None else None
                        )
                        if cur_val is not None:
                            pos_val_sum += cur_val
                        pnl_raw = _get_field(p, "unrealized_pnl", "cash_pnl", "cashPnl", "total_pnl", default=None)
                        pnl_val = round(_to_float(pnl_raw), 4) if pnl_raw is not None else (
                            round(cur_val - size * avg_px, 4) if cur_val is not None else 0.0
                        )
                        slug = str(_get_field(p, "slug", "event_slug", "eventSlug", default="") or "")
                        parsed_pos.append({
                            "asset_id": str(_get_field(p, "asset_id", "assetId", "asset", default="") or ""),
                            "condition_id": str(_get_field(p, "condition_id", "conditionId", default="") or ""),
                            "slug": slug,
                            "session_start": _parse_session_start_from_slug(slug),
                            "title": str(_get_field(p, "title", "market_title", default="") or slug),
                            "outcome": str(_get_field(p, "outcome", default="") or ""),
                            "size": round(size, 3),
                            "current_size": round(size, 4),
                            "avg_price": round(avg_px, 4),
                            "cur_price": cur_px,
                            "current_price": cur_px,
                            "current_value": cur_val,
                            "cash_pnl": pnl_val,
                            "redeemable": bool(_get_field(p, "redeemable", default=False)),
                            "status": str(_get_field(p, "status", default="OPEN") or "OPEN"),
                        })
                    positions = parsed_pos
                    self._last_positions = list(parsed_pos)
                    if portfolio_val == 0.0 and pos_val_sum > 0:
                        portfolio_val = round(pos_val_sum, 4)
            except Exception as e:
                err_parts.append(str(e)[:55])
        else:
            if not self.live_channel_status["sdk_available"]:
                err_parts.append("缺少 polymarket-client SDK，请执行 bash scripts/deploy.sh")
            try:
                async with aiohttp.ClientSession(headers=POLY_HEADERS) as session:
                    async with session.get(
                        f"{DATA_API_BASE}/positions",
                        params={"user": target_addr, "sizeThreshold": "0"},
                        proxy=POLY_PROXY or None,
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as resp:
                        if resp.status == 200:
                            pos_list = await resp.json()
                            parsed_pos = []
                            portfolio_val = 0.0
                            for p in (pos_list or [])[:50]:
                                size = _to_float(p.get("size"))
                                avg_px = _to_float(p.get("avgPrice"))
                                cur_px = round(_to_float(p.get("curPrice")), 4) if p.get("curPrice") is not None else None
                                cv = round(_to_float(p.get("currentValue")), 4) if p.get("currentValue") is not None else None
                                if cv is not None:
                                    portfolio_val += cv
                                slug = str(p.get("slug") or p.get("eventSlug") or "")
                                parsed_pos.append({
                                    "asset_id": str(p.get("asset") or ""),
                                    "condition_id": str(p.get("conditionId") or ""),
                                    "slug": slug,
                                    "session_start": _parse_session_start_from_slug(slug),
                                    "title": str(p.get("title") or slug),
                                    "outcome": str(p.get("outcome") or ""),
                                    "size": round(size, 3),
                                    "current_size": round(size, 4),
                                    "avg_price": round(avg_px, 4),
                                    "cur_price": cur_px,
                                    "current_price": cur_px,
                                    "current_value": cv,
                                    "cash_pnl": round(_to_float(p.get("cashPnl")), 4),
                                    "redeemable": bool(p.get("redeemable", False)),
                                    "status": "REDEEMABLE" if p.get("redeemable") else "OPEN",
                                })
                            positions = parsed_pos
                            self._last_positions = list(parsed_pos)
            except Exception:
                pass

        self.live_channel_status["open_orders_count"] = len(open_orders)
        self._last_account_refresh_ts = time.time()
        if cache_key:
            self._last_cache_key = cache_key

        # Authoritative SDK refresh supersedes any prior local optimistic reservation
        if cash_verified:
            self._local_reserved_usdc = 0.0
        avail_cash = round(max(0.0, cash - open_orders_locked - self._local_reserved_usdc), 4)

        self.cached_portfolio = {
            "wallet_address": mask_address(wallet_addr),
            "funder_address": mask_address(target_addr),
            "wallet_type": str(cfg.get("wallet_type") or "DEPOSIT_WALLET"),
            "cash": round(cash, 4),
            "available_cash": avail_cash,
            "pusd_balance": avail_cash,
            "usdc_balance": avail_cash,
            "legacy_usdce_balance": round(usdce_onchain, 4),
            "portfolio_value": round(portfolio_val, 4),
            "total_equity": round(cash + portfolio_val, 4),
            "open_orders_locked_usdc": round(open_orders_locked, 4),
            "local_reserved_usdc": round(self._local_reserved_usdc, 4),
            "cash_verified": cash_verified,
            "allowance_usdc": allowance_usdc,
            "allowance_ok": approved,
            "sell_allowance_ok": approved,
            "approved": approved,
            "missing_approvals": missing_approvals,
            "open_orders": open_orders,
            "open_positions": positions,
            "updated_at": self._last_account_refresh_ts,
            "error": "; ".join(err_parts),
        }
        return self.cached_portfolio

    # ---------------- SDK CLOB V2 Real Order Placement + Authoritative TTL Reconciliation ----------------

    async def _refresh_conditional_allowance_sdk(self, client: Any, token_id: str, market_version: int = 1):
        if not token_id or token_id in self._conditioned_approved_tokens:
            return
        fn = getattr(client, "update_balance_allowance", None) or getattr(client, "refresh_balance_allowance", None)
        asset_type = "CONDITIONAL-V2" if int(market_version) >= 2 else "CONDITIONAL"
        if callable(fn):
            try:
                res = fn(asset_type=asset_type, token_id=token_id)
                if asyncio.iscoroutine(res):
                    await res
                self._conditioned_approved_tokens.add(token_id)
            except Exception:
                pass

    @staticmethod
    def _round_tick(price: float, tick_size: float = 0.01) -> float:
        t = max(0.0001, float(tick_size or 0.01))
        px = round(round(float(price) / t) * t, 4)
        return max(t, min(round(1.0 - t, 4), px))

    @staticmethod
    def _parse_order_response(resp: Any, fallback_px: float, fallback_shares: float, side: str) -> dict:
        """
        Strict CLOB V2 order response parser (v46):
        - Rejects immediately on `ok/success is False`, `status in (REJECTED, FAILED, INVALID)`,
          or non-empty `error_msg / errorMsg / error / message` without an accepted order_id.
        - ONLY treats `making/taking` as instant fills when `status` is explicitly MATCHED/FILLED
          (or blank status accompanied by non-empty `trade_ids` / `transactions_hashes`).
          NEVER misinterprets a `LIVE` or `DELAYED` order's expected amount as an instant fill!
        - Uses `_parse_order_amount()` so integer shares like `"8"` are NEVER divided by 10^6.
        """
        ok_flag = _get_field(resp, "ok", "success", default=True)
        err_msg = str(_get_field(resp, "error_msg", "errorMsg", "error", "message", default="") or "").strip()
        oid = str(_get_field(resp, "order_id", "orderID", "id", default="") or "").strip()
        status = str(_get_field(resp, "status", default="") or "").strip().upper()

        if ok_flag is False or status in ("REJECTED", "FAILED", "INVALID") or (err_msg and not oid):
            raise RuntimeError(f"CLOB V2 拒单({status or 'ERR'}): {err_msg or str(resp)[:100]}")
        if not oid:
            raise RuntimeError(f"CLOB V2 异常响应(无 order_id): {str(resp)[:100]}")

        raw_tids = _get_field(resp, "trade_ids", "tradeIds", "transactions_hashes", "transactionsHashes", default=[]) or []
        trade_ids = [str(x) for x in raw_tids if x] if isinstance(raw_tids, (list, tuple)) else ([str(raw_tids)] if raw_tids else [])

        making = _parse_order_amount(_get_field(resp, "making_amount", "makingAmount", default=0))
        taking = _parse_order_amount(_get_field(resp, "taking_amount", "takingAmount", default=0))

        is_instant_match = (status in ("MATCHED", "FILLED")) or (status == "" and len(trade_ids) > 0)
        is_delayed = (status == "DELAYED")

        matched_shares = 0.0
        matched_price = fallback_px
        if is_instant_match:
            if side == "BUY" and taking > 0:
                matched_shares = round(taking, 4)
                spent_usdc = making if making > 0 else (matched_shares * fallback_px)
                if matched_shares > 0 and spent_usdc > 0:
                    matched_price = round(spent_usdc / matched_shares, 4)
            elif side == "SELL" and making > 0:
                matched_shares = round(making, 4)
                recv_usdc = taking if taking > 0 else (matched_shares * fallback_px)
                if matched_shares > 0 and recv_usdc > 0:
                    matched_price = round(recv_usdc / matched_shares, 4)
            elif status in ("MATCHED", "FILLED"):
                matched_shares = round(fallback_shares, 4)

        raw_summary = f"st={status or 'NONE'},mk={making},tk={taking},tids={len(trade_ids)}"
        return {
            "order_id": oid,
            "exchange_status": status or ("MATCHED" if matched_shares > 0 else "LIVE"),
            "matched_shares": matched_shares,
            "matched_price": matched_price,
            "trade_ids": trade_ids,
            "is_delayed": is_delayed,
            "raw_summary": raw_summary,
            "raw": resp,
        }

    async def _call_sdk_market_order(
        self,
        client: Any,
        *,
        token_id: str,
        side: str,
        amount_str: str,
        shares_str: str,
        limit_px_str: str,
    ) -> Any:
        """Signature-aware FAK market order invoker adapting asset_id/token_id & max_price/min_price/worst_price."""
        fn = getattr(client, "place_market_order", None)
        if callable(fn):
            try:
                pnames = set(inspect.signature(fn).parameters.keys())
            except Exception:
                pnames = set()
            kw: dict[str, Any] = {"side": side.upper()}
            if "token_id" in pnames and "asset_id" not in pnames:
                kw["token_id"] = token_id
            else:
                kw["asset_id"] = token_id
            if side.upper() == "BUY":
                if "amount" in pnames or not pnames:
                    kw["amount"] = amount_str
                elif "size" in pnames:
                    kw["size"] = shares_str
                if "max_price" in pnames or not pnames:
                    kw["max_price"] = limit_px_str
                elif "worst_price" in pnames:
                    kw["worst_price"] = limit_px_str
                elif "price" in pnames:
                    kw["price"] = limit_px_str
            else:
                if "size" in pnames or not pnames:
                    kw["size"] = shares_str
                elif "amount" in pnames:
                    kw["amount"] = shares_str
                if "min_price" in pnames or not pnames:
                    kw["min_price"] = limit_px_str
                elif "worst_price" in pnames:
                    kw["worst_price"] = limit_px_str
                elif "price" in pnames:
                    kw["price"] = limit_px_str
            if "order_type" in pnames:
                kw["order_type"] = "FAK"
            try:
                r = fn(**kw)
                return await r if asyncio.iscoroutine(r) else r
            except TypeError:
                kw.pop("order_type", None)
                r = fn(**kw)
                return await r if asyncio.iscoroutine(r) else r

        if hasattr(client, "create_limit_order") and hasattr(client, "post_order"):
            signed = await client.create_limit_order(asset_id=token_id, side=side.upper(), price=limit_px_str, size=shares_str)
            if dataclasses.is_dataclass(signed):
                signed = dataclasses.replace(signed, order_type="FAK")
            return await client.post_order(signed)

        return await client.place_limit_order(
            asset_id=token_id,
            side=side.upper(),
            price=limit_px_str,
            size=shares_str,
            order_type="FAK",
        )

    async def _verify_fill_settlement_sdk(
        self,
        *,
        order_id: str,
        token_id: str,
        side: str,
        fallback_px: float,
        expected_shares: float,
        expected_cost: float,
        bal_before: Optional[float],
        trade_ids: list[str],
        raw_resp: Any,
        condition_id: Optional[str] = None,
    ):
        """
        v46 Authoritative On-Chain Settlement Verifier (runs on SDK thread after MATCHED / DELAYED):
        - Prevents Ghost Positions! Confirms that a MATCHED/DELAYED fill actually settled on Polygon
          (MINED/CONFIRMED or real exchange shares/cash delta) instead of reverting to FAILED.
        - Emits `settlement_confirmed` on real fill or `trade_failed` on on-chain revert / ghost fill.
        """
        if self._stop_flag or not order_id:
            return
        try:
            client = await self._ensure_sdk_client()
            cfg = self.db.get_json("wallet_config", {})
            creds = self.get_decrypted_api_creds() or {}
            api_key = creds.get("apiKey", "")
            wallet_addrs = self._all_known_wallet_addrs(cfg, client)

            # Fast path: use official SDK `wait_for_order_fill_settlement` if available
            wait_fn = getattr(client, "wait_for_order_fill_settlement", None)
            sdk_settlement_status = ""
            if callable(wait_fn) and raw_resp is not None:
                try:
                    w_res = wait_fn(raw_resp, timeout=12.0)
                    settled_ev = await w_res if asyncio.iscoroutine(w_res) else w_res
                    sdk_settlement_status = str(_get_field(settled_ev, "status", default="") or "").upper()
                except Exception:
                    sdk_settlement_status = ""

            deadline = time.time() + 16.0
            confirmed_shares = 0.0
            confirmed_px = fallback_px
            confirmed_fee = 0.0
            trade_failed_seen = (sdk_settlement_status in _FAILED_TRADE_STATUSES)
            trade_confirmed_seen = (sdk_settlement_status in _CONFIRMED_TRADE_STATUSES)
            ex_pos_shares = 0.0
            live_cash_now: Optional[float] = None
            live_locked_now = 0.0

            while time.time() < deadline and not self._stop_flag:
                if trade_failed_seen:
                    break

                # 1. Check account trades for this order_id / trade_ids
                trades = await self._fetch_account_trades_sdk(client, condition_id=condition_id)
                for t in (trades or []):
                    t_st = str(_get_field(t, "status", default="") or "").upper()
                    t_id = str(_get_field(t, "id", "trade_id", default="") or "")
                    extracted = extract_fills_from_trade(t, api_key, wallet_addrs)
                    hit = [f for f in extracted if f.get("order_id") == order_id or (t_id and t_id in trade_ids)]
                    if not hit:
                        continue
                    if t_st in _FAILED_TRADE_STATUSES:
                        trade_failed_seen = True
                        break
                    self._merge_fills(hit)
                    if t_st in _CONFIRMED_TRADE_STATUSES:
                        trade_confirmed_seen = True

                agg = self.get_order_fill_summary(order_id)
                if agg["shares"] > 0:
                    confirmed_shares = agg["shares"]
                    confirmed_px = agg["avg_price"] or fallback_px
                    confirmed_fee = agg["fee_usdc"]
                    if agg["confirmed"]:
                        trade_confirmed_seen = True

                # 2. Check live collateral cash & exchange positions for physical proof
                try:
                    b_res = await client.get_balance_allowance(asset_type="COLLATERAL")
                    if b_res is not None:
                        live_cash_now = _parse_collateral_usdc(_get_field(b_res, "balance", default=None))
                except Exception:
                    pass

                try:
                    p_fn = getattr(client, "list_positions", None)
                    if callable(p_fn):
                        p_call = p_fn(sort_by="TIMESTAMP", sort_direction="DESC", page_size=25)
                        p_page = await p_call.first_page() if hasattr(p_call, "first_page") else (await p_call if asyncio.iscoroutine(p_call) else p_call)
                        ex_pos_shares = 0.0
                        for p in _extract_page_items(p_page):
                            p_aid = str(_get_field(p, "asset_id", "assetId", "asset", default="") or "")
                            if p_aid == str(token_id):
                                ex_pos_shares += _to_float(_get_field(p, "current_size", "size", default=0.0))
                except Exception:
                    pass

                oo_ok, oo_items, _ = await self._fetch_open_orders_sdk(client)
                if oo_ok:
                    live_locked_now = sum(
                        max(0.0, _to_float(_get_field(o, "original_size", "size", default=0.0)) - _to_float(_get_field(o, "size_matched", "sizeMatched", default=0.0)))
                        * _to_float(_get_field(o, "price", default=0.0))
                        for o in oo_items
                        if str(_get_field(o, "side", default="") or "").upper() == "BUY"
                    )

                cash_dropped = (
                    side == "BUY"
                    and bal_before is not None
                    and live_cash_now is not None
                    and (bal_before - max(0.0, live_cash_now - live_locked_now)) >= max(0.5, expected_cost * 0.45)
                )
                cash_rose = (
                    side == "SELL"
                    and bal_before is not None
                    and live_cash_now is not None
                    and (max(0.0, live_cash_now - live_locked_now) - bal_before) >= 0.05
                )

                if trade_failed_seen:
                    break
                if trade_confirmed_seen or (side == "BUY" and (ex_pos_shares >= 0.5 or cash_dropped)) or (side == "SELL" and cash_rose):
                    final_sh = confirmed_shares if confirmed_shares > 0 else (ex_pos_shares if (side == "BUY" and ex_pos_shares >= 0.5) else expected_shares)
                    avail_after = round(max(0.0, live_cash_now - live_locked_now), 4) if live_cash_now is not None else None
                    self._emit_live_update({
                        "event": "settlement_confirmed",
                        "order_id": order_id,
                        "side": side,
                        "shares": round(final_sh, 4),
                        "fill_price": round(confirmed_px, 4),
                        "size_usdc": round(final_sh * confirmed_px, 4),
                        "fee_usdc": round(confirmed_fee, 4),
                        "bal_after_entry": avail_after,
                        "exchange_pos_shares": round(ex_pos_shares, 4),
                        "trade_status": "CONFIRMED" if trade_confirmed_seen else "EXCHANGE_VERIFIED",
                        "ts": time.time(),
                    })
                    return

                await asyncio.sleep(1.5)

            # Timeout or explicit FAILED verdict:
            avail_after = round(max(0.0, live_cash_now - live_locked_now), 4) if live_cash_now is not None else None
            if trade_failed_seen:
                self._fills.pop(f"local:{order_id}", None)
                self._emit_live_update({
                    "event": "trade_failed",
                    "order_id": order_id,
                    "side": side,
                    "status": "FAILED",
                    "bal_after_entry": avail_after,
                    "reason": "Polymarket 链上结算失败(FAILED)，未产生真实持仓",
                    "ts": time.time(),
                })
                return

            # No trade confirmed, 0 exchange shares, AND cash never moved -> ghost / expired FAK!
            no_cash_move = (
                bal_before is not None
                and avail_after is not None
                and abs(avail_after - bal_before) < 0.25
            )
            if side == "BUY" and ex_pos_shares < 0.1 and no_cash_move and not trade_confirmed_seen:
                self._fills.pop(f"local:{order_id}", None)
                self._emit_live_update({
                    "event": "trade_failed",
                    "order_id": order_id,
                    "side": side,
                    "status": "FAILED",
                    "bal_after_entry": avail_after,
                    "reason": f"确权超时且交易所0持仓/余额未扣(${bal_before:.2f}→${avail_after:.2f})，判定未成交",
                    "ts": time.time(),
                })
        except Exception as e:
            log.warning("LIVE _verify_fill_settlement_sdk error for %s: %s", order_id, e)

    async def _ttl_cancel_and_reconcile_sdk(
        self,
        order_id: str,
        ttl_ms: int,
        side: str,
        fallback_px: float,
        condition_id: Optional[str] = None,
    ):
        await asyncio.sleep(max(1.5, ttl_ms / 1000.0))
        if self._stop_flag:
            return
        try:
            client = await self._ensure_sdk_client()
            cfg = self.db.get_json("wallet_config", {})
            creds = self.get_decrypted_api_creds() or {}
            api_key = creds.get("apiKey", "")
            wallet_addrs = self._all_known_wallet_addrs(cfg, client)

            agg0 = self.get_order_fill_summary(order_id)
            if agg0["shares"] > 0:
                self._emit_live_update({
                    "event": "ttl_reconciled",
                    "order_id": order_id,
                    "side": side,
                    "exchange_status": "MATCHED",
                    "size_matched": agg0["shares"],
                    "fill_price": agg0["avg_price"] or fallback_px,
                    "fee_usdc": agg0["fee_usdc"],
                    "cancel_confirmed": False,
                    "ts": time.time(),
                })
                return

            cancel_ok = False
            cancel_err = ""
            for _ in range(2):
                cancel_ok, _, cancel_err = await _call_sdk_order_method(
                    client, ("cancel_order", "cancel", "cancel_orders"), order_id
                )
                if cancel_ok:
                    self.live_channel_status["last_cancel_error"] = ""
                    break
                await asyncio.sleep(0.35)

            if not cancel_ok and cancel_err:
                self.live_channel_status["last_cancel_error"] = cancel_err
                log.error("LIVE cancel_order failed for %s: %s", order_id, cancel_err)

            await asyncio.sleep(0.45)

            go_ok, ord_info, go_err = await _call_sdk_order_method(
                client, ("get_order", "get_open_order"), order_id
            )
            trades = await self._fetch_account_trades_sdk(client, condition_id=condition_id)
            if trades:
                fresh_fills: list[dict] = []
                for t in trades:
                    fresh_fills.extend(extract_fills_from_trade(t, api_key, wallet_addrs))
                if fresh_fills:
                    self._merge_fills(fresh_fills)

            oo_ok, oo_items, oo_err = await self._fetch_open_orders_sdk(client)
            still_open: Optional[bool] = None
            if oo_ok:
                open_ids = {str(_get_field(o, "id", "order_id", "orderID", default="") or "") for o in oo_items}
                still_open = order_id in open_ids

            if still_open is True:
                c2_ok, _, c2_err = await _call_sdk_order_method(
                    client, ("cancel_order", "cancel", "cancel_orders"), order_id
                )
                if c2_ok:
                    cancel_ok = True
                    cancel_err = ""
                    await asyncio.sleep(0.4)
                    oo_ok2, oo_items2, _ = await self._fetch_open_orders_sdk(client)
                    if oo_ok2:
                        still_open = order_id in {
                            str(_get_field(o, "id", "order_id", "orderID", default="") or "") for o in oo_items2
                        }
                elif c2_err:
                    cancel_err = c2_err
                    self.live_channel_status["last_cancel_error"] = c2_err

            size_matched = 0.0
            final_px = fallback_px
            ex_status = "UNKNOWN"
            if go_ok and ord_info is not None:
                size_matched = _to_float(_get_field(ord_info, "size_matched", "sizeMatched", default=0.0))
                p_val = _to_float(_get_field(ord_info, "price", default=fallback_px))
                if 0.0 < p_val < 1.0:
                    final_px = p_val
                st_raw = str(_get_field(ord_info, "status", default="") or "").upper()
                if st_raw:
                    ex_status = st_raw

            agg = self.get_order_fill_summary(order_id)
            if agg["shares"] > size_matched:
                size_matched = agg["shares"]
                final_px = agg["avg_price"] or final_px

            if size_matched > 0.0:
                self._emit_live_update({
                    "event": "ttl_reconciled",
                    "order_id": order_id,
                    "side": side,
                    "exchange_status": ex_status if ex_status != "UNKNOWN" else "MATCHED",
                    "size_matched": round(size_matched, 4),
                    "fill_price": round(final_px, 4),
                    "fee_usdc": agg["fee_usdc"],
                    "cancel_confirmed": (still_open is False),
                    "ts": time.time(),
                })
            elif still_open is False and (cancel_ok or ex_status in _TERMINAL_CANCEL_STATUSES):
                self._emit_live_update({
                    "event": "ttl_reconciled",
                    "order_id": order_id,
                    "side": side,
                    "exchange_status": ex_status if ex_status != "UNKNOWN" else "CANCELLED",
                    "size_matched": 0.0,
                    "fill_price": round(final_px, 4),
                    "fee_usdc": 0.0,
                    "cancel_confirmed": True,
                    "ts": time.time(),
                })
            else:
                reason = cancel_err or ("订单仍在交易所挂单队列中" if still_open else (oo_err or go_err or "撤单结果未确认"))
                self.live_channel_status["last_cancel_error"] = reason[:120]
                self._emit_live_update({
                    "event": "cancel_failed",
                    "order_id": order_id,
                    "side": side,
                    "error": reason[:120],
                    "still_open": still_open,
                    "ts": time.time(),
                })
        except Exception as e:
            err_msg = f"TTL对账异常: {str(e)[:100]}"
            self.live_channel_status["last_cancel_error"] = err_msg
            log.error("LIVE _ttl_cancel_and_reconcile_sdk failed for %s: %s", order_id, e)
            self._emit_live_update({
                "event": "cancel_failed",
                "order_id": order_id,
                "side": side,
                "error": err_msg,
                "ts": time.time(),
            })

    async def place_live_order(
        self,
        token_id: str,
        price: float,
        size_usdc: float,
        is_taker: bool,
        ttl_ms: int,
        tick_size: float = 0.01,
        min_size: float = 5.0,
        geoblocked: bool = False,
        neg_risk: bool = False,
        market_version: int = 1,
        condition_id: Optional[str] = None,
        max_price_override: Optional[float] = None,
        bal_before: Optional[float] = None,
    ) -> dict:
        if geoblocked:
            raise RuntimeError("当前服务器 IP 处于 Polymarket 地区限制范围内，已禁止实盘下单")
        cfg = self.db.get_json("wallet_config", {})
        if cfg.get("is_observer_only"):
            raise RuntimeError("当前为只读观察钱包，请导入实盘私钥后再开启实盘")
        if not cfg.get("has_private_key") or not cfg.get("has_api_creds"):
            raise RuntimeError("尚未完成钱包私钥与 CLOB V2 API 凭证派生")

        px = self._round_tick(price, tick_size)
        shares = round(size_usdc / px, 2)
        if shares < float(min_size or 1.0):
            raise RuntimeError(f"下单份额 {shares:.2f} 小于本场最小限制 {min_size:.2f}")

        async def _submit():
            client = await self._ensure_sdk_client()
            if not is_taker:
                resp = await client.place_limit_order(
                    asset_id=token_id,
                    side="BUY",
                    price=f"{px:.4f}".rstrip("0").rstrip("."),
                    size=f"{shares:.2f}",
                    post_only=True,
                )
            else:
                if max_price_override is not None:
                    max_px = self._round_tick(min(0.99, max(px, float(max_price_override))), tick_size)
                else:
                    max_px = self._round_tick(min(0.99, px + 2.0 * max(0.01, tick_size)), tick_size)
                max_px_s = f"{max_px:.4f}".rstrip("0").rstrip(".")
                resp = await self._call_sdk_market_order(
                    client,
                    token_id=token_id,
                    side="BUY",
                    amount_str=f"{round(size_usdc, 2):.2f}",
                    shares_str=f"{shares:.2f}",
                    limit_px_str=max_px_s,
                )

            parsed = self._parse_order_response(resp, px, shares, "BUY")
            oid = parsed["order_id"]

            if (parsed["matched_shares"] > 0 or parsed["is_delayed"]) and oid:
                if parsed["matched_shares"] > 0:
                    self._fills[f"local:{oid}"] = {
                        "fill_key": f"local:{oid}",
                        "trade_id": parsed["trade_ids"][0] if parsed["trade_ids"] else "",
                        "order_id": oid,
                        "asset_id": token_id,
                        "side": "BUY",
                        "role": "TAKER" if is_taker else "MAKER",
                        "shares": parsed["matched_shares"],
                        "price": parsed["matched_price"],
                        "usdc": round(parsed["matched_shares"] * parsed["matched_price"], 4),
                        "fee_usdc": 0.0,
                        "status": "MATCHED",
                        "is_failed": False,
                        "is_confirmed": False,
                        "ts": time.time(),
                    }
                # CRITICAL v46: Launch on-chain settlement verification so FAILED/ghost fills are immediately caught!
                asyncio.create_task(
                    self._verify_fill_settlement_sdk(
                        order_id=oid,
                        token_id=token_id,
                        side="BUY",
                        fallback_px=parsed["matched_price"] or px,
                        expected_shares=parsed["matched_shares"] or shares,
                        expected_cost=size_usdc,
                        bal_before=bal_before,
                        trade_ids=parsed["trade_ids"],
                        raw_resp=resp,
                        condition_id=condition_id,
                    )
                )
            elif not is_taker and oid:
                asyncio.create_task(
                    self._ttl_cancel_and_reconcile_sdk(oid, ttl_ms, "BUY", px, condition_id=condition_id)
                )

            return {
                "order_id": oid,
                "shares": shares,
                "matched_shares": parsed["matched_shares"],
                "matched_price": parsed["matched_price"],
                "exchange_status": parsed["exchange_status"],
                "is_delayed": parsed["is_delayed"],
                "trade_ids": parsed["trade_ids"],
                "raw_summary": parsed["raw_summary"],
                "raw": parsed["raw"],
            }

        return await self._run_on_sdk(_submit(), timeout=15.0)

    async def place_live_sell_order(
        self,
        token_id: str,
        price: float,
        shares: float,
        is_taker: bool = False,
        ttl_ms: int = 8000,
        tick_size: float = 0.01,
        geoblocked: bool = False,
        neg_risk: bool = False,
        market_version: int = 1,
        condition_id: Optional[str] = None,
        bal_before: Optional[float] = None,
    ) -> dict:
        if geoblocked:
            raise RuntimeError("地区限制生效中，禁止实盘平仓")
        cfg = self.db.get_json("wallet_config", {})
        if cfg.get("is_observer_only"):
            raise RuntimeError("当前为只读观察钱包，无法实盘平仓")
        if not cfg.get("has_private_key") or not cfg.get("has_api_creds"):
            raise RuntimeError("尚未完成钱包私钥与 CLOB V2 API 凭证派生")

        px = self._round_tick(price, tick_size)
        sell_shares = round(max(0.01, shares), 2)

        async def _submit_sell():
            client = await self._ensure_sdk_client()
            await self._refresh_conditional_allowance_sdk(client, token_id, market_version=market_version)

            if not is_taker:
                resp = await client.place_limit_order(
                    asset_id=token_id,
                    side="SELL",
                    price=f"{px:.4f}".rstrip("0").rstrip("."),
                    size=f"{sell_shares:.2f}",
                    post_only=True,
                )
            else:
                min_px = self._round_tick(max(0.01, px - 2.0 * max(0.01, tick_size)), tick_size)
                min_px_s = f"{min_px:.4f}".rstrip("0").rstrip(".")
                resp = await self._call_sdk_market_order(
                    client,
                    token_id=token_id,
                    side="SELL",
                    amount_str=f"{sell_shares:.2f}",
                    shares_str=f"{sell_shares:.2f}",
                    limit_px_str=min_px_s,
                )

            parsed = self._parse_order_response(resp, px, sell_shares, "SELL")
            oid = parsed["order_id"]

            if (parsed["matched_shares"] > 0 or parsed["is_delayed"]) and oid:
                if parsed["matched_shares"] > 0:
                    self._fills[f"local:{oid}"] = {
                        "fill_key": f"local:{oid}",
                        "trade_id": parsed["trade_ids"][0] if parsed["trade_ids"] else "",
                        "order_id": oid,
                        "asset_id": token_id,
                        "side": "SELL",
                        "role": "TAKER" if is_taker else "MAKER",
                        "shares": parsed["matched_shares"],
                        "price": parsed["matched_price"],
                        "usdc": round(parsed["matched_shares"] * parsed["matched_price"], 4),
                        "fee_usdc": 0.0,
                        "status": "MATCHED",
                        "is_failed": False,
                        "is_confirmed": False,
                        "ts": time.time(),
                    }
                asyncio.create_task(
                    self._verify_fill_settlement_sdk(
                        order_id=oid,
                        token_id=token_id,
                        side="SELL",
                        fallback_px=parsed["matched_price"] or px,
                        expected_shares=parsed["matched_shares"] or sell_shares,
                        expected_cost=sell_shares * px,
                        bal_before=bal_before,
                        trade_ids=parsed["trade_ids"],
                        raw_resp=resp,
                        condition_id=condition_id,
                    )
                )
            elif not is_taker and oid:
                asyncio.create_task(
                    self._ttl_cancel_and_reconcile_sdk(oid, ttl_ms, "SELL", px, condition_id=condition_id)
                )

            return {
                "order_id": oid,
                "exec_price": parsed["matched_price"] if parsed["matched_shares"] > 0 else px,
                "matched_shares": parsed["matched_shares"],
                "exchange_status": parsed["exchange_status"],
                "is_delayed": parsed["is_delayed"],
                "raw_summary": parsed["raw_summary"],
                "is_taker": is_taker,
                "raw": parsed["raw"],
            }

        return await self._run_on_sdk(_submit_sell(), timeout=15.0)

    async def cancel_order(self, order_id: str) -> dict:
        async def _do():
            client = await self._ensure_sdk_client()
            ok, res, err = await _call_sdk_order_method(
                client, ("cancel_order", "cancel", "cancel_orders"), order_id
            )
            if not ok:
                self.live_channel_status["last_cancel_error"] = err
                raise RuntimeError(f"撤单失败: {err}")
            self.live_channel_status["last_cancel_error"] = ""
            return res

        res = await self._run_on_sdk(_do(), timeout=10.0)
        await self.refresh_balances_and_portfolio(force=True)
        return {"ok": True, "order_id": order_id, "result": str(res)[:100] if res is not None else "cancelled"}

    async def cancel_all_open_orders(self) -> dict:
        async def _cancel_all():
            client = await self._ensure_sdk_client()
            for name in ("cancel_all", "cancel_all_orders"):
                fn = getattr(client, name, None)
                if callable(fn):
                    r = fn()
                    return await r if asyncio.iscoroutine(r) else r
            oo_ok, oo_items, oo_err = await self._fetch_open_orders_sdk(client)
            if not oo_ok:
                raise RuntimeError(oo_err or "无法获取挂单列表")
            cancelled = 0
            for o in oo_items:
                oid = str(_get_field(o, "id", "order_id", "orderID", default="") or "")
                if oid:
                    ok, _, _ = await _call_sdk_order_method(client, ("cancel_order", "cancel", "cancel_orders"), oid)
                    if ok:
                        cancelled += 1
            return f"cancelled {cancelled}/{len(oo_items)}"

        res = await self._run_on_sdk(_cancel_all(), timeout=15.0)
        self.live_channel_status["last_cancel_error"] = ""
        await self.refresh_balances_and_portfolio(force=True)
        return {"ok": True, "result": str(res)[:120] if res is not None else "cancelled"}
