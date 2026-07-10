"""Admin alert emails via the Resend API (https://resend.com).

Synchronous by design — call it from a FastAPI BackgroundTask so it never
blocks the request. It silently no-ops when Resend isn't configured, and
never raises to the caller.
"""

import httpx

from app.core.config import Settings
from app.core.logging import get_logger

log = get_logger(__name__)

_RESEND_URL = "https://api.resend.com/emails"


def send_email(settings: Settings, subject: str, body: str) -> None:
    """Send a plaintext email via Resend. No-op if not configured."""
    recipients = [a.strip() for a in (settings.alert_email_to or "").split(",") if a.strip()]
    if not (settings.resend_api_key and settings.email_from_address and recipients):
        return  # not configured — skip silently

    sender = (
        f"{settings.email_from_name} <{settings.email_from_address}>"
        if settings.email_from_name
        else settings.email_from_address
    )

    try:
        resp = httpx.post(
            _RESEND_URL,
            headers={"Authorization": f"Bearer {settings.resend_api_key}"},
            json={"from": sender, "to": recipients, "subject": subject, "text": body},
            timeout=15,
        )
        if resp.status_code >= 300:
            log.error("email_send_failed", status=resp.status_code, detail=resp.text[:300])
    except Exception as exc:  # never let an alert failure break the request
        log.error("email_send_failed", detail=str(exc))
