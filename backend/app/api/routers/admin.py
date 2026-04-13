"""Admin endpoints — API key management and usage monitoring."""

import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, get_settings
from app.core.db.models import ApiKey, ApiUsage

router = APIRouter(prefix="/admin", tags=["Admin"])


# ── Auth dependency ────────────────────────────────────────────


def _check_admin(request: Request) -> None:
    """Verify admin password from query param or header."""
    settings = get_settings()
    password = (
        request.query_params.get("password")
        or request.headers.get("X-Admin-Password")
    )
    if password != settings.admin_password:
        raise HTTPException(status_code=401, detail="Invalid admin password")


# ── Schemas ────────────────────────────────────────────────────


class CreateKeyRequest(BaseModel):
    user_name: str = Field(min_length=1, max_length=100)
    email: str | None = Field(default=None, max_length=200)
    rate_limit_per_day: int = Field(default=10000, ge=1, le=1000000)


class ApiKeyResponse(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    key: str
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

    new_key = ApiKey(
        key=secrets.token_urlsafe(32),
        user_name=body.user_name,
        email=body.email,
        rate_limit_per_day=body.rate_limit_per_day,
    )
    db.add(new_key)
    await db.commit()
    await db.refresh(new_key)
    return new_key


@router.get("/keys", response_model=list[ApiKeyWithUsageResponse], summary="List all API keys")
async def list_keys(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _check_admin(request)

    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # Get all keys
    result = await db.execute(select(ApiKey).order_by(ApiKey.created_at.desc()))
    keys = result.scalars().all()

    response = []
    for k in keys:
        # Total requests
        total = await db.scalar(
            select(func.count(ApiUsage.id)).where(ApiUsage.api_key_id == k.id)
        ) or 0

        # Requests today
        today = await db.scalar(
            select(func.count(ApiUsage.id))
            .where(ApiUsage.api_key_id == k.id)
            .where(ApiUsage.timestamp >= today_start)
        ) or 0

        # Last used
        last = await db.scalar(
            select(func.max(ApiUsage.timestamp)).where(ApiUsage.api_key_id == k.id)
        )

        response.append(ApiKeyWithUsageResponse(
            id=k.id,
            key=k.key,
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
    return key


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


@router.get("/dashboard", response_class=HTMLResponse, summary="Admin dashboard UI")
async def dashboard(request: Request):
    settings = get_settings()
    password = request.query_params.get("password", "")

    if password != settings.admin_password:
        return HTMLResponse(content="""
<!DOCTYPE html>
<html><head><title>Admin Login</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>body{font-family:system-ui;max-width:400px;margin:60px auto;padding:0 16px}
input,button{width:100%;padding:12px;margin:8px 0;border-radius:8px;border:1px solid #ddd;font-size:16px;box-sizing:border-box}
button{background:#3b82f6;color:white;border:none;cursor:pointer}button:hover{background:#2563eb}</style>
</head><body>
<h2>Admin Login</h2>
<form method="GET"><input type="password" name="password" placeholder="Admin password" autofocus>
<button type="submit">Login</button></form>
</body></html>""")

    return HTMLResponse(content=f"""
<!DOCTYPE html>
<html><head><title>API Key Admin</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
*{{box-sizing:border-box}}
body{{font-family:system-ui;max-width:900px;margin:0 auto;padding:16px;background:#f8fafc;color:#1e293b}}
h1{{font-size:1.5rem}}
.card{{background:white;border-radius:12px;padding:20px;margin:16px 0;box-shadow:0 1px 3px rgba(0,0,0,0.1)}}
table{{width:100%;border-collapse:collapse;font-size:14px}}
th,td{{text-align:left;padding:10px 8px;border-bottom:1px solid #e2e8f0}}
th{{color:#64748b;font-weight:600;font-size:12px;text-transform:uppercase}}
.badge{{display:inline-block;padding:2px 10px;border-radius:999px;font-size:12px;font-weight:600}}
.active{{background:#dcfce7;color:#166534}}.inactive{{background:#fee2e2;color:#991b1b}}
input,button,select{{padding:10px 14px;border-radius:8px;border:1px solid #d1d5db;font-size:14px}}
button{{background:#3b82f6;color:white;border:none;cursor:pointer}}button:hover{{background:#2563eb}}
.btn-red{{background:#ef4444}}.btn-red:hover{{background:#dc2626}}
.btn-yellow{{background:#eab308;color:#1e293b}}.btn-yellow:hover{{background:#ca8a04}}
.key-text{{font-family:monospace;font-size:12px;background:#f1f5f9;padding:4px 8px;border-radius:4px;word-break:break-all}}
.form-row{{display:flex;gap:8px;flex-wrap:wrap;align-items:end}}
.form-row>*{{flex:1;min-width:150px}}
#msg{{padding:12px;border-radius:8px;margin:8px 0;display:none}}
.stats{{display:flex;gap:16px;flex-wrap:wrap}}
.stat{{text-align:center;padding:12px 20px;background:#f1f5f9;border-radius:8px;flex:1;min-width:120px}}
.stat .num{{font-size:1.5rem;font-weight:700;color:#3b82f6}}
.stat .label{{font-size:12px;color:#64748b}}
@media(max-width:640px){{
  .form-row{{flex-direction:column}}
  table{{font-size:12px}}
  th,td{{padding:8px 4px}}
}}
</style></head><body>
<h1>API Key Management</h1>

<div id="msg"></div>

<div class="card">
<h3>Create New Key</h3>
<div class="form-row">
<div><label>User Name *</label><br><input id="userName" placeholder="e.g. Praveen" required></div>
<div><label>Email</label><br><input id="email" placeholder="e.g. user@example.com" type="email"></div>
<div><label>Daily Limit</label><br><input id="rateLimit" type="number" value="10000" min="1"></div>
<div><label>&nbsp;</label><br><button onclick="createKey()">Create Key</button></div>
</div></div>

<div class="card">
<h3>API Keys</h3>
<div id="keyStats" class="stats" style="margin-bottom:16px"></div>
<div style="overflow-x:auto">
<table><thead><tr>
<th>User</th><th>Key</th><th>Status</th><th>Today</th><th>Total</th><th>Last Used</th><th>Actions</th>
</tr></thead><tbody id="keyTable"><tr><td colspan="7">Loading...</td></tr></tbody></table>
</div></div>

<script>
const P = '?password={password}';
const API = '/api/admin';

function msg(text, ok) {{
  const el = document.getElementById('msg');
  el.textContent = text;
  el.style.display = 'block';
  el.style.background = ok ? '#dcfce7' : '#fee2e2';
  el.style.color = ok ? '#166534' : '#991b1b';
  setTimeout(() => el.style.display = 'none', 4000);
}}

async function loadKeys() {{
  const res = await fetch(API + '/keys' + P);
  const keys = await res.json();
  const tbody = document.getElementById('keyTable');
  const stats = document.getElementById('keyStats');

  const totalKeys = keys.length;
  const activeKeys = keys.filter(k => k.is_active).length;
  const totalReqs = keys.reduce((s, k) => s + k.total_requests, 0);
  const todayReqs = keys.reduce((s, k) => s + k.requests_today, 0);

  stats.innerHTML = `
    <div class="stat"><div class="num">${{totalKeys}}</div><div class="label">Total Keys</div></div>
    <div class="stat"><div class="num">${{activeKeys}}</div><div class="label">Active</div></div>
    <div class="stat"><div class="num">${{todayReqs.toLocaleString()}}</div><div class="label">Requests Today</div></div>
    <div class="stat"><div class="num">${{totalReqs.toLocaleString()}}</div><div class="label">Total Requests</div></div>
  `;

  if (keys.length === 0) {{
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#94a3b8">No API keys yet</td></tr>';
    return;
  }}

  tbody.innerHTML = keys.map(k => `<tr>
    <td><strong>${{k.user_name}}</strong>${{k.email ? '<br><small style="color:#94a3b8">' + k.email + '</small>' : ''}}</td>
    <td><span class="key-text">${{k.key.substring(0,12)}}...</span>
      <button style="padding:4px 8px;font-size:11px;margin-left:4px" onclick="copyKey('${{k.key}}')">Copy</button></td>
    <td><span class="badge ${{k.is_active ? 'active' : 'inactive'}}">${{k.is_active ? 'Active' : 'Inactive'}}</span></td>
    <td>${{k.requests_today.toLocaleString()}}</td>
    <td>${{k.total_requests.toLocaleString()}}</td>
    <td style="font-size:12px;color:#94a3b8">${{k.last_used ? new Date(k.last_used).toLocaleDateString() : 'Never'}}</td>
    <td>
      <button class="btn-yellow" style="padding:4px 10px;font-size:12px" onclick="toggleKey(${{k.id}})">${{k.is_active ? 'Disable' : 'Enable'}}</button>
      <button class="btn-red" style="padding:4px 10px;font-size:12px" onclick="deleteKey(${{k.id}},'${{k.user_name}}')">Delete</button>
    </td>
  </tr>`).join('');
}}

async function createKey() {{
  const userName = document.getElementById('userName').value.trim();
  if (!userName) {{ msg('User name is required', false); return; }}
  const body = {{
    user_name: userName,
    email: document.getElementById('email').value.trim() || null,
    rate_limit_per_day: parseInt(document.getElementById('rateLimit').value) || 10000
  }};
  const res = await fetch(API + '/keys' + P, {{
    method: 'POST', headers: {{'Content-Type': 'application/json'}}, body: JSON.stringify(body)
  }});
  if (res.ok) {{
    const key = await res.json();
    msg('Key created: ' + key.key, true);
    document.getElementById('userName').value = '';
    document.getElementById('email').value = '';
    loadKeys();
  }} else {{
    const err = await res.json();
    msg(err.detail || 'Failed to create key', false);
  }}
}}

async function deleteKey(id, name) {{
  if (!confirm('Delete key for ' + name + '? This will also delete all usage logs.')) return;
  const res = await fetch(API + '/keys/' + id + P, {{ method: 'DELETE' }});
  if (res.ok) {{ msg('Key deleted', true); loadKeys(); }}
  else {{ msg('Failed to delete', false); }}
}}

async function toggleKey(id) {{
  const res = await fetch(API + '/keys/' + id + '/toggle' + P, {{ method: 'PATCH' }});
  if (res.ok) {{ loadKeys(); }}
  else {{ msg('Failed to toggle', false); }}
}}

function copyKey(key) {{
  navigator.clipboard.writeText(key).then(() => msg('Key copied to clipboard', true));
}}

loadKeys();
</script></body></html>""")
