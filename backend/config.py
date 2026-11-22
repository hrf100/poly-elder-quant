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

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYBOLT_WS_URL = "wss://ws-live-v2.polymarket.com/ws"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
POLYGON_RPC_URL = os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")
POLYGON_CHAIN_ID = 137

USDCE_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_EXCHANGE_CONTRACT = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

HARD_LIMITS = {
    "min_unit_usdc": 5.0,
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
