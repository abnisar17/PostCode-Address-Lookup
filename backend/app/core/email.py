"""Minimal SMTP email sender for admin alerts.

Synchronous by design — call it from a FastAPI BackgroundTask (or
``asyncio.to_thread``) so it never blocks the request. It silently no-ops
when SMTP isn't configured, and never raises to the caller.
"""

import smtplib
import ssl
from email.message import EmailMessage

from app.core.config import Settings
from app.core.logging import get_logger

log = get_logger(__name__)


def send_email(settings: Settings, subject: str, body: str) -> None:
    """Send a plaintext email. No-op if SMTP host/from/recipients aren't set."""
    recipients = [a.strip() for a in (settings.alert_email_to or "").split(",") if a.strip()]
    if not (settings.smtp_host and settings.smtp_from and recipients):
        return  # not configured — skip silently

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = settings.smtp_from
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    try:
        context = ssl.create_default_context()
        if settings.smtp_port == 465:
            with smtplib.SMTP_SSL(
                settings.smtp_host, settings.smtp_port, timeout=15, context=context
            ) as server:
                if settings.smtp_user:
                    server.login(settings.smtp_user, settings.smtp_password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=15) as server:
                server.starttls(context=context)
                if settings.smtp_user:
                    server.login(settings.smtp_user, settings.smtp_password)
                server.send_message(msg)
    except Exception as exc:  # never let an alert failure break the request
        log.error("email_send_failed", detail=str(exc))
