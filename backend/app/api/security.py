"""Security helpers for API-key hashing and admin session tokens.

- API keys are never stored in plaintext: we keep only a SHA-256 hash and
  a short non-secret prefix for display. The raw key is shown to the admin
  exactly once, at creation time.
- Admin auth uses a short-lived HMAC-signed session token delivered in an
  HttpOnly cookie, so the password never travels in the URL or logs.
"""

import hashlib
import hmac
from datetime import datetime, timedelta, timezone

# ── API key hashing ────────────────────────────────────────────

KEY_PREFIX_LEN = 12


def hash_api_key(raw_key: str) -> str:
    """Return the hex SHA-256 of an API key (what we store and look up by)."""
    return hashlib.sha256(raw_key.encode()).hexdigest()


# ── Admin session tokens ───────────────────────────────────────

ADMIN_COOKIE = "admin_session"
ADMIN_SESSION_TTL = timedelta(hours=12)


def _sign(payload: str, secret: str) -> str:
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def make_admin_session(secret: str, ttl: timedelta = ADMIN_SESSION_TTL) -> str:
    """Create an HMAC-signed `<expiry>.<sig>` session token."""
    expiry = int((datetime.now(timezone.utc) + ttl).timestamp())
    return f"{expiry}.{_sign(str(expiry), secret)}"


def valid_admin_session(token: str | None, secret: str) -> bool:
    """Verify the token's signature and that it has not expired."""
    if not token or "." not in token:
        return False
    expiry_str, sig = token.rsplit(".", 1)
    if not hmac.compare_digest(sig, _sign(expiry_str, secret)):
        return False
    try:
        return int(expiry_str) > int(datetime.now(timezone.utc).timestamp())
    except ValueError:
        return False


def admin_password_ok(supplied: str | None, secret: str) -> bool:
    """Constant-time compare of a supplied admin password against the secret."""
    return bool(supplied) and hmac.compare_digest(supplied, secret)
