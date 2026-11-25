import inspect
import os
import secrets
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data")))
MODELS_DIR = DATA_DIR / "models"
DB_PATH = DATA_DIR / "poly_quant.db"

DATA_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)

APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", "8080"))
DEFAULT_PASSWORD = os.getenv("DEFAULT_PASSWORD", "qq111111")

# Optional unified proxy (unset => direct connection with zero overhead)
POLY_PROXY = (os.getenv("POLY_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or "").strip()
if POLY_PROXY:
    os.environ.setdefault("HTTP_PROXY", POLY_PROXY)
    os.environ.setdefault("HTTPS_PROXY", POLY_PROXY)

POLY_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    "Origin": "https://polymarket.com",
    "Referer": "https://polymarket.com/",
}


def ws_headers_kwarg(connect_fn) -> dict:
    """Return `additional_headers` (websockets>=13) or `extra_headers` (older websockets) safely."""
    try:
        params = inspect.signature(connect_fn).parameters
        if "additional_headers" in params:
            return {"additional_headers": POLY_HEADERS}
        if "extra_headers" in params:
            return {"extra_headers": POLY_HEADERS}
    except Exception:
        pass
    return {}


_ENV_KEY = os.getenv("MASTER_ENCRYPTION_KEY", "").strip()
_KEY_FILE = DATA_DIR / ".master_key"
if _ENV_KEY:
    MASTER_ENCRYPTION_KEY = _ENV_KEY
elif _KEY_FILE.exists():
    MASTER_ENCRYPTION_KEY = _KEY_FILE.read_text(encoding="utf-8").strip()
else:
    MASTER_ENCRYPTION_KEY = secrets.token_hex(32)
    _KEY_FILE.write_text(MASTER_ENCRYPTION_KEY, encoding="utf-8")
    try:
        os.chmod(_KEY_FILE, 0o600)
    except OSError:
        pass

JWT_SECRET = os.getenv("JWT_SECRET", MASTER_ENCRYPTION_KEY[:32])
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "72"))

# Polymarket REST & WebSocket Endpoints (2026 CLOB V2 & Protocol V2)
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"
POLYMARKET_API_BASE = "https://polymarket.com/api"
CRYPTO_PRICE_API = f"{POLYMARKET_API_BASE}/crypto/crypto-price"
PRICE_HISTORY_API = f"{POLYMARKET_API_BASE}/crypto/price-history"
PAST_RESULTS_API = f"{POLYMARKET_API_BASE}/past-results"
GEOBLOCK_URL = f"{POLYMARKET_API_BASE}/geoblock"

CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CLOB_USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
POLYBOLT_WS_URL = "wss://ws-live-v2.polymarket.com/ws"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
POLYGON_RPC_URL = os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")
POLYGON_RPC_URLS = list(dict.fromkeys([
    POLYGON_RPC_URL,
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon.drpc.org",
]))
POLYGON_CHAIN_ID = 137

# CLOB V2 Collateral & Verified 2026 Polygon Mainnet Exchange Contracts
PUSD_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
USDCE_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_EXCHANGE_V2 = "0xE111180000d2663C0091e4f400237545B87B996B"          # EIP-712 domain version "2"
NEG_RISK_CTF_EXCHANGE_V2 = "0xe2222d279d744050d28e00520010520000310F59"  # EIP-712 domain version "2"
EXCHANGE_V3 = "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa"              # Protocol V2 markets, domain version "3"
CTF_EXCHANGE_CONTRACT = CTF_EXCHANGE_V2
NEG_RISK_CTF_EXCHANGE = NEG_RISK_CTF_EXCHANGE_V2

HARD_LIMITS = {
    "min_unit_usdc": 1.05,
    "max_position_pct": 0.05,
    "max_entry_price": 0.78,
    "min_entry_price": 0.18,
    "min_edge_prob": 0.035,
    "min_ttl_ms": 1500,
    "max_ttl_ms": 10000,
    "cooldown_sec": 15,
    "consecutive_loss_limit": 3,
    "circuit_breaker_minutes": 30,
    "outlier_vol_sigma": 4.0,
    "taker_fee_rate": 0.05,
}
