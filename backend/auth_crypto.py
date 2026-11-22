import base64
import hashlib
import hmac
import os
import time
from collections import defaultdict
from typing import Optional

import jwt
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from backend.config import JWT_EXPIRE_HOURS, JWT_SECRET, MASTER_ENCRYPTION_KEY

_LOGIN_FAILURES: dict[str, list[float]] = defaultdict(list)
_LOCK_UNTIL: dict[str, float] = {}
MAX_ATTEMPTS = 5
LOCK_SECONDS = 900


def _derive_aes_key() -> bytes:
    return hashlib.sha256(MASTER_ENCRYPTION_KEY.encode("utf-8")).digest()


def encrypt_secret(plaintext: str) -> str:
    if not plaintext:
        return ""
    aes = AESGCM(_derive_aes_key())
    nonce = os.urandom(12)
    ct = aes.encrypt(nonce, plaintext.encode("utf-8"), None)
    return base64.b64encode(nonce + ct).decode("ascii")


def decrypt_secret(ciphertext_b64: str) -> str:
    if not ciphertext_b64:
        return ""
    raw = base64.b64decode(ciphertext_b64.encode("ascii"))
    nonce, ct = raw[:12], raw[12:]
    aes = AESGCM(_derive_aes_key())
    return aes.decrypt(nonce, ct, None).decode("utf-8")


def hash_password(password: str, salt: Optional[str] = None) -> str:
    salt_hex = salt or os.urandom(16).hex()
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt_hex), 120_000)
    return f"{salt_hex}${dk.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        salt_hex, _ = stored_hash.split("$", 1)
        candidate = hash_password(password, salt_hex)
        return hmac.compare_digest(candidate, stored_hash)
    except Exception:
        return False


def check_ip_locked(ip: str) -> Optional[int]:
    now = time.time()
    until = _LOCK_UNTIL.get(ip, 0)
    if until > now:
        return int(until - now)
    return None


def record_login_failure(ip: str) -> None:
    now = time.time()
    recent = [t for t in _LOGIN_FAILURES[ip] if now - t < LOCK_SECONDS]
    recent.append(now)
    _LOGIN_FAILURES[ip] = recent
    if len(recent) >= MAX_ATTEMPTS:
        _LOCK_UNTIL[ip] = now + LOCK_SECONDS
        _LOGIN_FAILURES[ip] = []


def clear_login_failures(ip: str) -> None:
    _LOGIN_FAILURES.pop(ip, None)
    _LOCK_UNTIL.pop(ip, None)


def create_access_token(subject: str = "admin") -> str:
    now = int(time.time())
    payload = {
        "sub": subject,
        "iat": now,
        "exp": now + JWT_EXPIRE_HOURS * 3600,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def verify_access_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except Exception:
        return None


def mask_address(addr: str) -> str:
    if not addr or len(addr) < 12:
        return addr or ""
    return f"{addr[:6]}...{addr[-4:]}"
