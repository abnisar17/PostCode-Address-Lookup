"""Admin endpoints — API key management and usage monitoring.

Auth: log in via POST /admin/login (X-Admin-Password header) which sets a
short-lived HttpOnly session cookie. Programmatic callers may instead send
the X-Admin-Password header on each request. The password is never accepted
in the URL, so it cannot leak into logs or browser history.
"""

import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, get_settings
from app.api.security import (
    ADMIN_COOKIE,
    ADMIN_SESSION_TTL,
    KEY_PREFIX_LEN,
    admin_password_ok,
    hash_api_key,
    make_admin_session,
    valid_admin_session,
)
from app.core.db.models import ApiKey, ApiUsage

router = APIRouter(prefix="/admin", tags=["Admin"])


# ── Auth ───────────────────────────────────────────────────────


def _check_admin(request: Request) -> None:
    """Authorize via a valid session cookie or the X-Admin-Password header."""
    secret = get_settings().admin_password
    if valid_admin_session(request.cookies.get(ADMIN_COOKIE), secret):
        return
    if admin_password_ok(request.headers.get("X-Admin-Password"), secret):
        return
    raise HTTPException(status_code=401, detail="Invalid admin password")


def _set_session_cookie(response: JSONResponse, token: str) -> None:
    response.set_cookie(
        ADMIN_COOKIE,
        token,
        max_age=int(ADMIN_SESSION_TTL.total_seconds()),
        httponly=True,
        secure=True,
        samesite="lax",
        path="/api/admin",
    )


@router.post("/login", summary="Admin login — sets a session cookie")
async def admin_login(request: Request):
    secret = get_settings().admin_password
    password = request.headers.get("X-Admin-Password")
    if password is None:
        try:
            password = (await request.json()).get("password")
        except Exception:
            password = None
    if not admin_password_ok(password, secret):
        raise HTTPException(status_code=401, detail="Invalid admin password")
    resp = JSONResponse({"detail": "ok"})
    _set_session_cookie(resp, make_admin_session(secret))
    return resp


@router.post("/logout", summary="Admin logout — clears the session cookie")
async def admin_logout():
    resp = JSONResponse({"detail": "ok"})
    resp.delete_cookie(ADMIN_COOKIE, path="/api/admin")
    return resp


# ── Schemas ────────────────────────────────────────────────────


class CreateKeyRequest(BaseModel):
    user_name: str = Field(min_length=1, max_length=100)
    email: str | None = Field(default=None, max_length=200)
    rate_limit_per_day: int = Field(default=10000, ge=1, le=1000000)


class ApiKeyResponse(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    # Full key is returned ONLY in the create response; elsewhere it is null.
    key: str | None = None
    key_prefix: str | None = None
    user_name: str
    email: str | None
    is_active: bool
    rate_limit_per_day: int
    created_at: datetime


class ApiKeyWithUsageResponse(ApiKeyResponse):
    total_requests: int = 0
    requests_today: int = 0
    last_used: datetime | None = None


class UsageStatsResponse(BaseModel):
    date: str
    requests: int
    unique_endpoints: int


# ── Key Management ─────────────────────────────────────────────


@router.post("/keys", response_model=ApiKeyResponse, summary="Create a new API key")
async def create_key(
    body: CreateKeyRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _check_admin(request)

    raw_key = secrets.token_urlsafe(32)
    new_key = ApiKey(
        key_hash=hash_api_key(raw_key),
        key_prefix=raw_key[:KEY_PREFIX_LEN],
        user_name=body.user_name,
        email=body.email,
        rate_limit_per_day=body.rate_limit_per_day,
    )
    db.add(new_key)
    await db.commit()
    await db.refresh(new_key)

    # Return the raw key exactly once — it is not recoverable afterwards.
    return ApiKeyResponse(
        id=new_key.id,
        key=raw_key,
        key_prefix=new_key.key_prefix,
        user_name=new_key.user_name,
        email=new_key.email,
        is_active=new_key.is_active,
        rate_limit_per_day=new_key.rate_limit_per_day,
        created_at=new_key.created_at,
    )


@router.get("/keys", response_model=list[ApiKeyWithUsageResponse], summary="List all API keys")
async def list_keys(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _check_admin(request)

    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    result = await db.execute(select(ApiKey).order_by(ApiKey.created_at.desc()))
    keys = result.scalars().all()

    response = []
    for k in keys:
        total = await db.scalar(
            select(func.count(ApiUsage.id)).where(ApiUsage.api_key_id == k.id)
        ) or 0
        today = await db.scalar(
            select(func.count(ApiUsage.id))
            .where(ApiUsage.api_key_id == k.id)
            .where(ApiUsage.timestamp >= today_start)
        ) or 0
        last = await db.scalar(
            select(func.max(ApiUsage.timestamp)).where(ApiUsage.api_key_id == k.id)
        )

        response.append(ApiKeyWithUsageResponse(
            id=k.id,
            key=None,  # never expose the stored key in a list
            key_prefix=k.key_prefix or (k.key[:KEY_PREFIX_LEN] if k.key else None),
            user_name=k.user_name,
            email=k.email,
            is_active=k.is_active,
            rate_limit_per_day=k.rate_limit_per_day,
            created_at=k.created_at,
            total_requests=total,
            requests_today=today,
            last_used=last,
        ))

    return response


@router.delete("/keys/{key_id}", summary="Delete an API key")
async def delete_key(
    key_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _check_admin(request)

    result = await db.execute(select(ApiKey).where(ApiKey.id == key_id))
    key = result.scalars().first()
    if not key:
        raise HTTPException(status_code=404, detail="API key not found")

    await db.execute(delete(ApiUsage).where(ApiUsage.api_key_id == key_id))
    await db.delete(key)
    await db.commit()
    return {"detail": f"Key for '{key.user_name}' deleted"}


@router.patch("/keys/{key_id}/toggle", response_model=ApiKeyResponse, summary="Activate/deactivate a key")
async def toggle_key(
    key_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _check_admin(request)

    result = await db.execute(select(ApiKey).where(ApiKey.id == key_id))
    key = result.scalars().first()
    if not key:
        raise HTTPException(status_code=404, detail="API key not found")

    key.is_active = not key.is_active
    await db.commit()
    await db.refresh(key)
    return ApiKeyResponse(
        id=key.id,
        key=None,
        key_prefix=key.key_prefix or (key.key[:KEY_PREFIX_LEN] if key.key else None),
        user_name=key.user_name,
        email=key.email,
        is_active=key.is_active,
        rate_limit_per_day=key.rate_limit_per_day,
        created_at=key.created_at,
    )


# ── Usage Stats ────────────────────────────────────────────────


@router.get("/usage/{key_id}", response_model=list[UsageStatsResponse], summary="Usage stats for a key")
async def key_usage(
    key_id: int,
    request: Request,
    days: int = Query(default=30, ge=1, le=90),
    db: AsyncSession = Depends(get_db),
):
    _check_admin(request)

    since = datetime.now(timezone.utc) - timedelta(days=days)
    date_col = func.date(ApiUsage.timestamp)

    result = await db.execute(
        select(
            date_col.label("date"),
            func.count(ApiUsage.id).label("requests"),
            func.count(func.distinct(ApiUsage.endpoint)).label("unique_endpoints"),
        )
        .where(ApiUsage.api_key_id == key_id)
        .where(ApiUsage.timestamp >= since)
        .group_by(date_col)
        .order_by(date_col.desc())
    )

    return [
        UsageStatsResponse(date=str(row.date), requests=row.requests, unique_endpoints=row.unique_endpoints)
        for row in result.all()
    ]


# ── Admin Dashboard (HTML) ─────────────────────────────────────


_LOGIN_HTML = """
<!DOCTYPE html>
<html><head><title>Admin Login</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>body{font-family:system-ui;max-width:400px;margin:60px auto;padding:0 16px}
input,button{width:100%;padding:12px;margin:8px 0;border-radius:8px;border:1px solid #ddd;font-size:16px;box-sizing:border-box}
button{background:#3b82f6;color:white;border:none;cursor:pointer}button:hover{background:#2563eb}
#err{color:#991b1b;font-size:14px;display:none}</style>
</head><body>
<h2>Admin Login</h2>
<div id="err">Invalid password</div>
<input type="password" id="pw" placeholder="Admin password" autofocus
  onkeydown="if(event.key==='Enter')login()">
<button onclick="login()">Login</button>
<script>
async function login() {
  const res = await fetch('/api/admin/login', {
    method: 'POST',
    headers: {'X-Admin-Password': document.getElementById('pw').value}
  });
  if (res.ok) { location.reload(); }
  else { document.getElementById('err').style.display = 'block'; }
}
</script>
</body></html>"""


@router.get("/dashboard", response_class=HTMLResponse, summary="Admin dashboard UI")
async def dashboard(request: Request):
    secret = get_settings().admin_password
    if not valid_admin_session(request.cookies.get(ADMIN_COOKIE), secret):
        return HTMLResponse(content=_LOGIN_HTML)

    return HTMLResponse(content="""
<!DOCTYPE html>
<html><head><title>API Key Admin</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
*{box-sizing:border-box}
body{font-family:system-ui;max-width:900px;margin:0 auto;padding:16px;background:#f8fafc;color:#1e293b}
h1{font-size:1.5rem}
.card{background:white;border-radius:12px;padding:20px;margin:16px 0;box-shadow:0 1px 3px rgba(0,0,0,0.1)}
table{width:100%;border-collapse:collapse;font-size:14px}
th,td{text-align:left;padding:10px 8px;border-bottom:1px solid #e2e8f0}
th{color:#64748b;font-weight:600;font-size:12px;text-transform:uppercase}
.badge{display:inline-block;padding:2px 10px;border-radius:999px;font-size:12px;font-weight:600}
.active{background:#dcfce7;color:#166534}.inactive{background:#fee2e2;color:#991b1b}
input,button,select{padding:10px 14px;border-radius:8px;border:1px solid #d1d5db;font-size:14px}
button{background:#3b82f6;color:white;border:none;cursor:pointer}button:hover{background:#2563eb}
.btn-red{background:#ef4444}.btn-red:hover{background:#dc2626}
.btn-yellow{background:#eab308;color:#1e293b}.btn-yellow:hover{background:#ca8a04}
.btn-gray{background:#64748b}.btn-gray:hover{background:#475569}
.key-text{font-family:monospace;font-size:12px;background:#f1f5f9;padding:4px 8px;border-radius:4px;word-break:break-all}
.form-row{display:flex;gap:8px;flex-wrap:wrap;align-items:end}
.form-row>*{flex:1;min-width:150px}
#msg{padding:12px;border-radius:8px;margin:8px 0;display:none}
.topbar{display:flex;justify-content:space-between;align-items:center}
.stats{display:flex;gap:16px;flex-wrap:wrap}
.stat{text-align:center;padding:12px 20px;background:#f1f5f9;border-radius:8px;flex:1;min-width:120px}
.stat .num{font-size:1.5rem;font-weight:700;color:#3b82f6}
.stat .label{font-size:12px;color:#64748b}
@media(max-width:640px){
  .form-row{flex-direction:column}
  table{font-size:12px}
  th,td{padding:8px 4px}
}
</style></head><body>
<div class="topbar"><h1>API Key Management</h1>
<button class="btn-gray" style="flex:0" onclick="logout()">Log out</button></div>

<div id="msg"></div>

<div class="card">
<h3>Create New Key</h3>
<div class="form-row">
<div><label>User Name *</label><br><input id="userName" placeholder="e.g. Praveen" required></div>
<div><label>Email</label><br><input id="email" placeholder="e.g. user@example.com" type="email"></div>
<div><label>Daily Limit</label><br><input id="rateLimit" type="number" value="10000" min="1"></div>
<div><label>&nbsp;</label><br><button onclick="createKey()">Create Key</button></div>
</div>
<p style="font-size:12px;color:#64748b">The full key is shown only once, at creation. Only a prefix is stored for display, so it cannot be recovered later.</p>
<div id="newKey" style="display:none;margin-top:12px;padding:12px;background:#ecfdf5;border:1px solid #a7f3d0;border-radius:8px">
<strong>New key — copy it now, it will NOT be shown again:</strong>
<div style="display:flex;gap:8px;margin-top:8px;align-items:center">
<span id="newKeyVal" class="key-text" style="flex:1"></span>
<button style="flex:0;white-space:nowrap" onclick="copyNewKey()">Copy</button>
</div></div>
</div>

<div class="card">
<h3>API Keys</h3>
<div id="keyStats" class="stats" style="margin-bottom:16px"></div>
<div style="overflow-x:auto">
<table><thead><tr>
<th>User</th><th>Key</th><th>Status</th><th>Today</th><th>Total</th><th>Limit/day</th><th>Last Used</th><th>Actions</th>
</tr></thead><tbody id="keyTable"><tr><td colspan="8">Loading...</td></tr></tbody></table>
</div></div>

<script>
const API = '/api/admin';
const OPTS = { credentials: 'same-origin' };  // send the session cookie
let KEYS = [];  // latest loaded keys, for id->name lookups

function msg(text, ok) {
  const el = document.getElementById('msg');
  el.textContent = text;
  el.style.display = 'block';
  el.style.background = ok ? '#dcfce7' : '#fee2e2';
  el.style.color = ok ? '#166534' : '#991b1b';
  setTimeout(() => el.style.display = 'none', 8000);
}

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g,
    c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

async function logout() {
  await fetch(API + '/logout', { method: 'POST', ...OPTS });
  location.reload();
}

async function loadKeys() {
  const res = await fetch(API + '/keys', OPTS);
  if (res.status === 401) { location.reload(); return; }
  const keys = await res.json();
  KEYS = keys;
  const tbody = document.getElementById('keyTable');
  const stats = document.getElementById('keyStats');

  const totalKeys = keys.length;
  const activeKeys = keys.filter(k => k.is_active).length;
  const totalReqs = keys.reduce((s, k) => s + k.total_requests, 0);
  const todayReqs = keys.reduce((s, k) => s + k.requests_today, 0);

  stats.innerHTML = `
    <div class="stat"><div class="num">${totalKeys}</div><div class="label">Total Keys</div></div>
    <div class="stat"><div class="num">${activeKeys}</div><div class="label">Active</div></div>
    <div class="stat"><div class="num">${todayReqs.toLocaleString()}</div><div class="label">Requests Today</div></div>
    <div class="stat"><div class="num">${totalReqs.toLocaleString()}</div><div class="label">Total Requests</div></div>
  `;

  if (keys.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:#94a3b8">No API keys yet</td></tr>';
    return;
  }

  tbody.innerHTML = keys.map(k => `<tr>
    <td><strong>${esc(k.user_name)}</strong>${k.email ? '<br><small style="color:#94a3b8">' + esc(k.email) + '</small>' : ''}</td>
    <td><span class="key-text">${esc(k.key_prefix || '—')}…</span></td>
    <td><span class="badge ${k.is_active ? 'active' : 'inactive'}">${k.is_active ? 'Active' : 'Inactive'}</span></td>
    <td>${k.requests_today.toLocaleString()}</td>
    <td>${k.total_requests.toLocaleString()}</td>
    <td>${k.rate_limit_per_day.toLocaleString()}</td>
    <td style="font-size:12px;color:#94a3b8">${k.last_used ? new Date(k.last_used).toLocaleDateString() : 'Never'}</td>
    <td>
      <button class="btn-yellow" style="padding:4px 10px;font-size:12px" onclick="toggleKey(${k.id})">${k.is_active ? 'Disable' : 'Enable'}</button>
      <button class="btn-red" style="padding:4px 10px;font-size:12px" onclick="deleteKey(${k.id})">Delete</button>
    </td>
  </tr>`).join('');
}

async function createKey() {
  const userName = document.getElementById('userName').value.trim();
  if (!userName) { msg('User name is required', false); return; }
  const body = {
    user_name: userName,
    email: document.getElementById('email').value.trim() || null,
    rate_limit_per_day: parseInt(document.getElementById('rateLimit').value) || 10000
  };
  const res = await fetch(API + '/keys', {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body), ...OPTS
  });
  if (res.ok) {
    const key = await res.json();
    document.getElementById('newKeyVal').textContent = key.key;
    document.getElementById('newKey').style.display = 'block';
    if (navigator.clipboard) {
      navigator.clipboard.writeText(key.key)
        .then(() => msg('Key created and copied to clipboard', true))
        .catch(() => msg('Key created — copy it from the box above', true));
    } else {
      msg('Key created — copy it from the box above', true);
    }
    document.getElementById('userName').value = '';
    document.getElementById('email').value = '';
    loadKeys();
  } else {
    const err = await res.json().catch(() => ({}));
    msg(err.detail || 'Failed to create key', false);
  }
}

function copyNewKey() {
  const v = document.getElementById('newKeyVal').textContent;
  if (navigator.clipboard) {
    navigator.clipboard.writeText(v).then(() => msg('Copied to clipboard', true));
  }
}

async function deleteKey(id) {
  const k = KEYS.find(x => x.id === id);
  const name = k ? k.user_name : 'this user';
  if (!confirm('Delete key for ' + name + '? This will also delete all usage logs.')) return;
  const res = await fetch(API + '/keys/' + id, { method: 'DELETE', ...OPTS });
  if (res.ok) { msg('Key deleted', true); loadKeys(); }
  else { msg('Failed to delete', false); }
}

async function toggleKey(id) {
  const res = await fetch(API + '/keys/' + id + '/toggle', { method: 'PATCH', ...OPTS });
  if (res.ok) { loadKeys(); }
  else { msg('Failed to toggle', false); }
}

loadKeys();
</script></body></html>""")
