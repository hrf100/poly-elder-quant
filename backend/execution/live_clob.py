import asyncio
import json
import time
from typing import Optional
import aiohttp

from backend.auth_crypto import decrypt_secret, encrypt_secret, mask_address
from backend.config import (
    CLOB_API_BASE,
    CTF_EXCHANGE_CONTRACT,
    DATA_API_BASE,
    POLYGON_CHAIN_ID,
    POLYGON_RPC_URL,
    USDCE_CONTRACT,
)
from backend.storage.db import Database


class PolymarketAccountManager:
    """
    Handles Polymarket wallet encrypted storage, L2 API credential derivation,
    zero-balance observer key generation (for PolyBolt paper-mode auth),
    on-chain Polygon USDC.e balance & CTF allowance queries, Portfolio API queries,
    and Post-Only Maker / FOK Taker live execution via py-clob-client.
    """

    def __init__(self, db: Database):
        self.db = db
        self._clob_client = None
        self.cached_portfolio: dict = {
            "wallet_address": "",
            "funder_address": "",
            "usdc_balance": 0.0,
            "portfolio_value": 0.0,
            "total_equity": 0.0,
            "allowance_usdc": 0.0,
            "allowance_ok": False,
            "open_positions": [],
            "updated_at": 0.0,
            "error": "",
        }

    def get_public_wallet_info(self) -> dict:
        cfg = self.db.get_json("wallet_config", {})
        return {
            "signature_type": int(cfg.get("signature_type", 2)),
            "funder_address": cfg.get("funder_address", ""),
            "wallet_address": mask_address(cfg.get("wallet_address", "")),
            "wallet_address_full": cfg.get("wallet_address", ""),
            "has_private_key": bool(cfg.get("has_private_key", False)),
            "has_api_creds": bool(cfg.get("has_api_creds", False)),
            "is_observer_only": bool(cfg.get("is_observer_only", False)),
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

    def _build_clob_client(self, private_key: str, signature_type: int, funder: str, creds: Optional[dict] = None):
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        api_creds = None
        if creds and creds.get("apiKey"):
            api_creds = ApiCreds(
                api_key=creds["apiKey"],
                api_secret=creds["secret"],
                api_passphrase=creds["passphrase"],
            )
        kwargs = {
            "host": CLOB_API_BASE,
            "key": private_key,
            "chain_id": POLYGON_CHAIN_ID,
            "signature_type": signature_type,
        }
        if funder and signature_type in (1, 2):
            kwargs["funder"] = funder
        if api_creds:
            kwargs["creds"] = api_creds
        return ClobClient(**kwargs)

    def configure_wallet(self, private_key: Optional[str], signature_type: int, funder_address: str, is_observer_only: bool = False) -> dict:
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

        cfg["signature_type"] = int(signature_type)
        cfg["funder_address"] = (funder_address or "").strip()
        self.db.set_json("wallet_config", cfg)
        self._clob_client = None
        return self.get_public_wallet_info()

    def generate_observer_wallet(self) -> dict:
        """Creates a fresh zero-balance EOA locally and derives read-only CLOB/PolyBolt API credentials."""
        from eth_account import Account
        acct = Account.create()
        self.configure_wallet(acct.key.hex(), signature_type=0, funder_address="", is_observer_only=True)
        return self.derive_api_credentials()

    def derive_api_credentials(self) -> dict:
        """Uses py-clob-client EIP-712 signing to derive or create Polymarket L2 API Key / Secret / Passphrase."""
        pk = self._get_decrypted_pk()
        if not pk:
            raise ValueError("尚未配置钱包私钥")
        cfg = self.db.get_json("wallet_config", {})
        client = self._build_clob_client(pk, int(cfg.get("signature_type", 2)), cfg.get("funder_address", ""))
        creds_obj = client.create_or_derive_api_creds()
        creds = {
            "apiKey": getattr(creds_obj, "api_key", None) or creds_obj.get("apiKey"),
            "secret": getattr(creds_obj, "api_secret", None) or creds_obj.get("secret"),
            "passphrase": getattr(creds_obj, "api_passphrase", None) or creds_obj.get("passphrase"),
        }
        if not creds["apiKey"]:
            raise RuntimeError("派生 Polymarket API 凭证失败，请检查网络或服务器 IP 地区")

        self.db.set_kv("enc_api_creds", encrypt_secret(json.dumps(creds)))
        cfg["has_api_creds"] = True
        self.db.set_json("wallet_config", cfg)
        self._clob_client = self._build_clob_client(pk, int(cfg.get("signature_type", 2)), cfg.get("funder_address", ""), creds)
        return {
            "ok": True,
            "api_key_masked": f"{creds['apiKey'][:6]}...{creds['apiKey'][-4:]}",
            "wallet": self.get_public_wallet_info(),
        }

    def get_active_client(self):
        if self._clob_client is not None:
            return self._clob_client
        pk = self._get_decrypted_pk()
        creds = self.get_decrypted_api_creds()
        if not pk or not creds:
            return None
        cfg = self.db.get_json("wallet_config", {})
        self._clob_client = self._build_clob_client(pk, int(cfg.get("signature_type", 2)), cfg.get("funder_address", ""), creds)
        return self._clob_client

    async def _rpc_eth_call(self, session: aiohttp.ClientSession, to: str, data: str) -> int:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [{"to": to, "data": data}, "latest"],
        }
        async with session.post(POLYGON_RPC_URL, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as r:
            res = await r.json()
            hex_val = res.get("result", "0x0")
            return int(hex_val, 16) if hex_val and hex_val != "0x" else 0

    async def refresh_balances_and_portfolio(self) -> dict:
        """Fetches Polygon USDC.e balance, CTF Exchange allowance, and Polymarket active positions/portfolio value."""
        cfg = self.db.get_json("wallet_config", {})
        wallet_addr = cfg.get("wallet_address", "")
        funder_addr = cfg.get("funder_address", "")
        sig_type = int(cfg.get("signature_type", 2))
        target_addr = funder_addr if (sig_type in (1, 2) and funder_addr) else wallet_addr

        if not target_addr or not target_addr.startswith("0x") or len(target_addr) != 42:
            self.cached_portfolio.update({"error": "未配置有效钱包地址", "updated_at": time.time()})
            return self.cached_portfolio

        addr_clean = target_addr[2:].lower().rjust(64, "0")
        spender_clean = CTF_EXCHANGE_CONTRACT[2:].lower().rjust(64, "0")
        # balanceOf(address) = 0x70a08231 ; allowance(address,address) = 0xdd62ed3e
        bal_calldata = "0x70a08231" + addr_clean
        allow_calldata = "0xdd62ed3e" + addr_clean + spender_clean

        try:
            async with aiohttp.ClientSession() as session:
                raw_bal, raw_allow = await asyncio.gather(
                    self._rpc_eth_call(session, USDCE_CONTRACT, bal_calldata),
                    self._rpc_eth_call(session, USDCE_CONTRACT, allow_calldata),
                )
                usdc_balance = round(raw_bal / 1e6, 4)
                allowance_usdc = round(min(raw_allow / 1e6, 1e12), 2)

                # Polymarket Data API: positions & portfolio value
                positions = []
                portfolio_val = 0.0
                try:
                    async with session.get(
                        f"{DATA_API_BASE}/positions",
                        params={"user": target_addr, "sizeThreshold": "0.1"},
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as resp:
                        if resp.status == 200:
                            pos_list = await resp.json()
                            for p in (pos_list or [])[:25]:
                                cv = float(p.get("currentValue") or 0.0)
                                portfolio_val += cv
                                positions.append({
                                    "title": p.get("title", ""),
                                    "outcome": p.get("outcome", ""),
                                    "size": round(float(p.get("size") or 0), 3),
                                    "avg_price": round(float(p.get("avgPrice") or 0), 4),
                                    "cur_price": round(float(p.get("curPrice") or 0), 4),
                                    "current_value": round(cv, 4),
                                    "cash_pnl": round(float(p.get("cashPnl") or 0), 4),
                                })
                except Exception:
                    pass

                self.cached_portfolio = {
                    "wallet_address": mask_address(wallet_addr),
                    "funder_address": mask_address(target_addr),
                    "usdc_balance": usdc_balance,
                    "portfolio_value": round(portfolio_val, 4),
                    "total_equity": round(usdc_balance + portfolio_val, 4),
                    "allowance_usdc": allowance_usdc,
                    "allowance_ok": allowance_usdc >= 100.0,
                    "open_positions": positions,
                    "updated_at": time.time(),
                    "error": "",
                }
        except Exception as e:
            self.cached_portfolio["error"] = f"余额查询失败: {str(e)[:80]}"
            self.cached_portfolio["updated_at"] = time.time()

        return self.cached_portfolio

    async def place_live_order(self, token_id: str, price: float, size_usdc: float, is_taker: bool, ttl_ms: int) -> dict:
        """Places a real GTC Post-Only Maker limit order (with auto-cancel TTL) or FOK Taker order."""
        cfg = self.db.get_json("wallet_config", {})
        if cfg.get("is_observer_only"):
            raise RuntimeError("当前为只读观察钱包，请导入实盘私钥后再开启实盘")
        client = self.get_active_client()
        if not client:
            raise RuntimeError("尚未完成钱包私钥与 API Key 派生")

        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        shares = round(size_usdc / price, 2)
        order_args = OrderArgs(price=round(price, 2), size=shares, side=BUY, token_id=token_id)

        def _submit():
            signed = client.create_order(order_args)
            otype = OrderType.FOK if is_taker else OrderType.GTC
            return client.post_order(signed, otype)

        resp = await asyncio.to_thread(_submit)
        order_id = resp.get("orderID") or resp.get("id") or ""

        # For Maker orders, schedule non-blocking auto-cancel after TTL if unfilled
        if not is_taker and order_id:
            async def _auto_cancel():
                await asyncio.sleep(max(1.5, ttl_ms / 1000.0))
                try:
                    await asyncio.to_thread(client.cancel, order_id)
                except Exception:
                    pass
            asyncio.create_task(_auto_cancel())

        return {"order_id": order_id, "raw": resp, "shares": shares}
    async def place_live_sell_order(
        self,
        token_id: str,
        price: float,
        shares: float,
        is_taker: bool = True,
        ttl_ms: int = 8000,
    ) -> dict:
        """
        Executes an intraday early-exit SELL on Polymarket CLOB:
        - is_taker=False: 0%-fee Post-Only GTC Maker limit SELL (with automatic cancel after ttl_ms).
        - is_taker=True:  Emergency FOK Taker SELL into best_bid (used for Stop-Loss).
        """
        cfg = self.db.get_json("wallet_config", {})
        if cfg.get("is_observer_only"):
            raise RuntimeError("当前为只读观察钱包，无法实盘平仓")
        client = self.get_active_client()
        if not client:
            raise RuntimeError("尚未完成钱包私钥与 API Key 派生")

        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import SELL

        exec_px = max(0.01, round(price - 0.01, 2)) if is_taker else max(0.02, min(0.98, round(price, 2)))
        sell_shares = round(max(0.01, shares), 2)
        order_args = OrderArgs(price=exec_px, size=sell_shares, side=SELL, token_id=token_id)

        def _submit_sell():
            signed = client.create_order(order_args)
            otype = OrderType.FOK if is_taker else OrderType.GTC
            return client.post_order(signed, otype)

        resp = await asyncio.to_thread(_submit_sell)
        order_id = resp.get("orderID") or resp.get("id") or ""

        if not is_taker and order_id:
            async def _auto_cancel_sell():
                await asyncio.sleep(max(2.0, ttl_ms / 1000.0))
                try:
                    await asyncio.to_thread(client.cancel, order_id)
                except Exception:
                    pass
            asyncio.create_task(_auto_cancel_sell())

        return {"order_id": order_id, "exec_price": round(price, 2), "is_taker": is_taker, "raw": resp}
