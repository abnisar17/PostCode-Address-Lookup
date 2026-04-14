"""Public API documentation page."""

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["Documentation"])


@router.get("/documentation", response_class=HTMLResponse, summary="API Documentation", include_in_schema=False)
async def api_documentation():
    return HTMLResponse(content="""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>UK Postcode & Address Lookup - API Documentation</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Inter', -apple-system, system-ui, sans-serif; color: #1e293b; background: #f1f5f9; line-height: 1.7; -webkit-font-smoothing: antialiased; }

/* Header */
.hero { background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 50%, #1e40af 100%); color: white; padding: 60px 20px 50px; text-align: center; position: relative; overflow: hidden; }
.hero::before { content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0; background: radial-gradient(circle at 30% 50%, rgba(59,130,246,0.15) 0%, transparent 60%), radial-gradient(circle at 70% 30%, rgba(147,197,253,0.1) 0%, transparent 50%); }
.hero-content { position: relative; z-index: 1; max-width: 700px; margin: 0 auto; }
.hero h1 { font-size: 2.5rem; font-weight: 800; margin-bottom: 12px; letter-spacing: -0.02em; line-height: 1.2; }
.hero .subtitle { font-size: 1.15rem; color: #93c5fd; font-weight: 400; margin-bottom: 24px; }
.hero-stats { display: flex; justify-content: center; gap: 32px; flex-wrap: wrap; margin-top: 28px; }
.hero-stat { text-align: center; }
.hero-stat .num { font-size: 1.6rem; font-weight: 800; color: #60a5fa; }
.hero-stat .label { font-size: 0.8rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 2px; }

/* Navigation */
.nav { background: white; border-bottom: 1px solid #e2e8f0; position: sticky; top: 0; z-index: 100; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
.nav-inner { max-width: 960px; margin: 0 auto; padding: 0 20px; display: flex; gap: 0; overflow-x: auto; -webkit-overflow-scrolling: touch; }
.nav a { padding: 14px 16px; font-size: 0.85rem; font-weight: 500; color: #64748b; text-decoration: none; white-space: nowrap; border-bottom: 2px solid transparent; transition: all 0.2s; }
.nav a:hover { color: #1e40af; }
.nav a.active { color: #1e40af; border-bottom-color: #3b82f6; }

/* Container */
.container { max-width: 960px; margin: 0 auto; padding: 24px 20px; }

/* Sections */
.section { background: white; border-radius: 16px; padding: 32px; margin: 24px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.04), 0 4px 12px rgba(0,0,0,0.03); border: 1px solid #e8ecf1; }
h2 { font-size: 1.5rem; font-weight: 700; color: #0f172a; margin-bottom: 20px; display: flex; align-items: center; gap: 10px; }
h2 .icon { width: 32px; height: 32px; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 1rem; }
h3 { font-size: 1rem; font-weight: 600; color: #334155; margin: 24px 0 8px; }
p { margin: 8px 0; color: #475569; font-size: 0.95rem; }

/* Code */
code { background: #f1f5f9; padding: 2px 7px; border-radius: 5px; font-size: 0.85em; color: #0f172a; font-family: 'JetBrains Mono', monospace; font-weight: 500; }
pre { background: #0f172a; color: #e2e8f0; padding: 20px; border-radius: 12px; overflow-x: auto; margin: 14px 0; font-size: 0.82rem; line-height: 1.6; border: 1px solid #1e293b; }
pre code { background: none; color: inherit; padding: 0; font-weight: 400; }
.code-comment { color: #64748b; }
.code-string { color: #7dd3fc; }
.code-key { color: #fbbf24; }

/* Endpoints */
.endpoint { border: 1px solid #e2e8f0; border-radius: 12px; margin: 20px 0; overflow: hidden; transition: box-shadow 0.2s; }
.endpoint:hover { box-shadow: 0 4px 12px rgba(0,0,0,0.06); }
.endpoint-header { display: flex; align-items: center; gap: 14px; padding: 14px 20px; background: linear-gradient(to right, #f8fafc, #ffffff); border-bottom: 1px solid #e2e8f0; cursor: pointer; }
.method { background: linear-gradient(135deg, #22c55e, #16a34a); color: white; padding: 5px 12px; border-radius: 6px; font-size: 0.75rem; font-weight: 700; letter-spacing: 0.03em; font-family: 'JetBrains Mono', monospace; }
.endpoint-url { font-family: 'JetBrains Mono', monospace; font-size: 0.88rem; color: #1e293b; font-weight: 500; word-break: break-all; }
.endpoint-desc { font-size: 0.82rem; color: #64748b; margin-left: auto; display: none; }
.endpoint-body { padding: 20px; background: #fafbfc; }
.endpoint-body p { font-size: 0.9rem; }

/* Tables */
.params-table { width: 100%; border-collapse: separate; border-spacing: 0; margin: 14px 0; font-size: 0.85rem; border-radius: 8px; overflow: hidden; border: 1px solid #e2e8f0; }
.params-table th { text-align: left; padding: 10px 14px; background: #f8fafc; color: #475569; font-weight: 600; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; }
.params-table td { padding: 10px 14px; border-top: 1px solid #f1f5f9; }
.params-table td:first-child { font-family: 'JetBrains Mono', monospace; color: #0f172a; font-weight: 500; font-size: 0.82rem; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 0.68rem; font-weight: 600; margin-left: 6px; }
.required { background: #fef2f2; color: #dc2626; border: 1px solid #fecaca; }
.optional { background: #eff6ff; color: #2563eb; border: 1px solid #bfdbfe; }

/* Response codes */
.response-codes { width: 100%; border-collapse: separate; border-spacing: 0; margin: 14px 0; border-radius: 8px; overflow: hidden; border: 1px solid #e2e8f0; }
.response-codes td, .response-codes th { padding: 10px 14px; text-align: left; font-size: 0.9rem; }
.response-codes th { background: #f8fafc; color: #475569; font-weight: 600; font-size: 0.75rem; text-transform: uppercase; }
.response-codes td { border-top: 1px solid #f1f5f9; }
.code-200 { color: #16a34a; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
.code-400 { color: #ea580c; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
.code-500 { color: #dc2626; font-weight: 700; font-family: 'JetBrains Mono', monospace; }

/* Enrichment cards */
.enrichment-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 14px; margin-top: 16px; }
.enrichment-card { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; padding: 16px; transition: transform 0.15s, box-shadow 0.15s; }
.enrichment-card:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.06); }
.enrichment-card h4 { font-size: 0.9rem; font-weight: 600; color: #0f172a; margin-bottom: 6px; display: flex; align-items: center; gap: 6px; }
.enrichment-card p { font-size: 0.8rem; color: #64748b; margin: 0; line-height: 1.5; }
.enrichment-icon { font-size: 1.2rem; }

/* Code tabs */
.code-tabs { display: flex; gap: 0; border-bottom: 2px solid #e2e8f0; margin-bottom: 0; }
.code-tab { padding: 10px 18px; cursor: pointer; font-size: 0.82rem; font-weight: 600; color: #64748b; border-bottom: 2px solid transparent; margin-bottom: -2px; transition: all 0.2s; }
.code-tab:hover { color: #1e40af; }
.code-tab.active { color: #1e40af; border-bottom-color: #3b82f6; }
.code-panel { display: none; }
.code-panel.active { display: block; }

/* Try it */
.try-section { background: linear-gradient(135deg, #f0fdf4, #ecfdf5); border: 1px solid #bbf7d0; border-radius: 12px; padding: 24px; margin-top: 16px; }
.try-section label { font-size: 0.85rem; font-weight: 600; color: #166534; display: block; margin-bottom: 4px; margin-top: 12px; }
.try-section label:first-child { margin-top: 0; }
.try-section input { width: 100%; padding: 10px 14px; border: 1px solid #86efac; border-radius: 8px; font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; background: white; transition: border-color 0.2s; }
.try-section input:focus { outline: none; border-color: #22c55e; box-shadow: 0 0 0 3px rgba(34,197,94,0.1); }
.try-btn { padding: 10px 24px; background: linear-gradient(135deg, #22c55e, #16a34a); color: white; border: none; border-radius: 8px; cursor: pointer; font-size: 0.85rem; font-weight: 600; margin-top: 14px; transition: transform 0.15s; }
.try-btn:hover { transform: translateY(-1px); }
.try-result { margin-top: 14px; max-height: 400px; overflow-y: auto; }

/* Note */
.note { background: linear-gradient(135deg, #fffbeb, #fef3c7); border: 1px solid #fde68a; border-radius: 10px; padding: 14px 18px; margin: 16px 0; font-size: 0.88rem; color: #92400e; display: flex; gap: 10px; align-items: flex-start; }
.note-icon { font-size: 1.1rem; flex-shrink: 0; margin-top: 1px; }

/* Footer */
.footer { text-align: center; padding: 40px 20px; color: #94a3b8; font-size: 0.82rem; }
.footer .sources { display: flex; flex-wrap: wrap; justify-content: center; gap: 8px; margin-top: 10px; }
.footer .source-tag { background: #f1f5f9; padding: 4px 10px; border-radius: 999px; font-size: 0.72rem; color: #64748b; }

/* Responsive */
@media (max-width: 640px) {
  .hero { padding: 40px 16px 36px; }
  .hero h1 { font-size: 1.6rem; }
  .hero .subtitle { font-size: 0.95rem; }
  .hero-stats { gap: 20px; }
  .hero-stat .num { font-size: 1.2rem; }
  .section { padding: 20px 16px; margin: 16px 0; border-radius: 12px; }
  h2 { font-size: 1.2rem; }
  pre { font-size: 0.72rem; padding: 14px; }
  .endpoint-header { flex-direction: column; align-items: flex-start; gap: 8px; padding: 12px 14px; }
  .endpoint-body { padding: 14px; }
  .params-table td, .params-table th { padding: 8px 10px; }
  .enrichment-grid { grid-template-columns: 1fr; }
  .nav a { padding: 12px 12px; font-size: 0.8rem; }
  .code-tab { padding: 8px 12px; font-size: 0.78rem; }
  .container { padding: 16px 12px; }
}
@media (min-width: 641px) {
  .endpoint-desc { display: block; }
}
</style>
</head>
<body>

<!-- Hero -->
<div class="hero">
  <div class="hero-content">
    <h1>UK Postcode & Address Lookup API</h1>
    <p class="subtitle">Access comprehensive UK address data with property prices, company registrations, food hygiene ratings, and commercial valuations</p>
    <div class="hero-stats">
      <div class="hero-stat"><div class="num">31M+</div><div class="label">Addresses</div></div>
      <div class="hero-stat"><div class="num">2.7M</div><div class="label">Postcodes</div></div>
      <div class="hero-stat"><div class="num">30M+</div><div class="label">Price Records</div></div>
      <div class="hero-stat"><div class="num">5.5M+</div><div class="label">Companies</div></div>
      <div class="hero-stat"><div class="num">9</div><div class="label">Data Sources</div></div>
    </div>
  </div>
</div>

<!-- Nav -->
<div class="nav">
  <div class="nav-inner">
    <a href="#auth">Authentication</a>
    <a href="#endpoints">Endpoints</a>
    <a href="#responses">Responses</a>
    <a href="#data">Data</a>
    <a href="#examples">Examples</a>
    <a href="#try">Try It</a>
  </div>
</div>

<div class="container">

<!-- Authentication -->
<div class="section" id="auth">
  <h2><span class="icon" style="background:#eff6ff;color:#2563eb">&#128274;</span> Authentication</h2>
  <p>All API requests require an API key for authentication. You can include it in two ways:</p>

  <h3>Option 1: Query Parameter (recommended)</h3>
  <pre><code>GET /api/postcodes/SW1A1AA<span class="code-string">?apiKey=YOUR_API_KEY</span></code></pre>

  <h3>Option 2: HTTP Header</h3>
  <pre><code><span class="code-key">X-API-Key:</span> <span class="code-string">YOUR_API_KEY</span></code></pre>

  <div class="note">
    <span class="note-icon">&#9432;</span>
    <span>Contact your administrator to obtain an API key. Each key has a configurable daily rate limit (default: 10,000 requests/day).</span>
  </div>
</div>

<!-- Endpoints -->
<div class="section" id="endpoints">
  <h2><span class="icon" style="background:#f0fdf4;color:#16a34a">&#9889;</span> Endpoints</h2>

  <!-- 1. Postcode Lookup -->
  <div class="endpoint">
    <div class="endpoint-header">
      <span class="method">GET</span>
      <span class="endpoint-url">/api/postcodes/{postcode}</span>
      <span class="endpoint-desc">Look up addresses by postcode</span>
    </div>
    <div class="endpoint-body">
      <p>Returns all addresses linked to a UK postcode with enrichment data including property prices, registered companies, food hygiene ratings, and commercial valuations.</p>
      <table class="params-table">
        <thead><tr><th>Parameter</th><th>Location</th><th>Description</th></tr></thead>
        <tbody>
          <tr><td>postcode</td><td>path <span class="badge required">required</span></td><td>UK postcode in any format (e.g. SW1A1AA, SW1A 1AA, sw1a1aa)</td></tr>
          <tr><td>apiKey</td><td>query <span class="badge required">required</span></td><td>Your API key</td></tr>
          <tr><td>page</td><td>query <span class="badge optional">optional</span></td><td>Page number, starting from 1 (default: 1)</td></tr>
          <tr><td>page_size</td><td>query <span class="badge optional">optional</span></td><td>Number of addresses per page (default: 20, max: 100)</td></tr>
        </tbody>
      </table>
      <h3>Example</h3>
      <pre><code><span class="code-comment"># Look up postcode SO23 0QD</span>
GET /api/postcodes/SO230QD?apiKey=YOUR_API_KEY

<span class="code-comment"># With pagination</span>
GET /api/postcodes/SO230QD?apiKey=YOUR_API_KEY&page=1&page_size=50</code></pre>
      <h3>Response</h3>
      <pre><code>{
  <span class="code-key">"postcode"</span>: {
    <span class="code-key">"postcode"</span>: <span class="code-string">"SO23 0QD"</span>,
    <span class="code-key">"latitude"</span>: 51.0578,
    <span class="code-key">"longitude"</span>: -1.3008
  },
  <span class="code-key">"total"</span>: 36,
  <span class="code-key">"page"</span>: 1,
  <span class="code-key">"page_size"</span>: 20,
  <span class="code-key">"addresses"</span>: [
    {
      <span class="code-key">"house_number"</span>: <span class="code-string">"1"</span>,
      <span class="code-key">"street"</span>: <span class="code-string">"St Leonards Road"</span>,
      <span class="code-key">"city"</span>: <span class="code-string">"Winchester"</span>,
      <span class="code-key">"source"</span>: <span class="code-string">"land_registry"</span>,
      <span class="code-key">"price_paid"</span>: [ ... ],
      <span class="code-key">"companies"</span>: [],
      <span class="code-key">"food_ratings"</span>: [],
      <span class="code-key">"voa_ratings"</span>: []
    }
  ]
}</code></pre>
    </div>
  </div>

  <!-- 2. Autocomplete -->
  <div class="endpoint">
    <div class="endpoint-header">
      <span class="method">GET</span>
      <span class="endpoint-url">/api/postcodes/autocomplete</span>
      <span class="endpoint-desc">Type-ahead postcode search</span>
    </div>
    <div class="endpoint-body">
      <p>Returns postcodes matching a given prefix. Ideal for building search-as-you-type interfaces.</p>
      <table class="params-table">
        <thead><tr><th>Parameter</th><th>Location</th><th>Description</th></tr></thead>
        <tbody>
          <tr><td>q</td><td>query <span class="badge required">required</span></td><td>Postcode prefix, minimum 2 characters (e.g. SW1A, EC1, M1)</td></tr>
          <tr><td>apiKey</td><td>query <span class="badge required">required</span></td><td>Your API key</td></tr>
          <tr><td>limit</td><td>query <span class="badge optional">optional</span></td><td>Maximum suggestions to return (default: 10, max: 50)</td></tr>
        </tbody>
      </table>
      <h3>Example</h3>
      <pre><code>GET /api/postcodes/autocomplete?q=SW1A&apiKey=YOUR_API_KEY</code></pre>
      <h3>Response</h3>
      <pre><code>{
  <span class="code-key">"query"</span>: <span class="code-string">"SW1A"</span>,
  <span class="code-key">"count"</span>: 3,
  <span class="code-key">"results"</span>: [
    { <span class="code-key">"postcode"</span>: <span class="code-string">"SW1A 0AA"</span>, <span class="code-key">"postcode_no_space"</span>: <span class="code-string">"SW1A0AA"</span> },
    { <span class="code-key">"postcode"</span>: <span class="code-string">"SW1A 0AB"</span>, <span class="code-key">"postcode_no_space"</span>: <span class="code-string">"SW1A0AB"</span> },
    { <span class="code-key">"postcode"</span>: <span class="code-string">"SW1A 1AA"</span>, <span class="code-key">"postcode_no_space"</span>: <span class="code-string">"SW1A1AA"</span> }
  ]
}</code></pre>
    </div>
  </div>

  <!-- 3. Address Search -->
  <div class="endpoint">
    <div class="endpoint-header">
      <span class="method">GET</span>
      <span class="endpoint-url">/api/addresses/search</span>
      <span class="endpoint-desc">Search addresses by text</span>
    </div>
    <div class="endpoint-body">
      <p>Search for addresses using free text, street name, city, or postcode. At least one search parameter is required.</p>
      <table class="params-table">
        <thead><tr><th>Parameter</th><th>Location</th><th>Description</th></tr></thead>
        <tbody>
          <tr><td>q</td><td>query <span class="badge optional">optional</span></td><td>Free text search across all fields (min 2 chars)</td></tr>
          <tr><td>street</td><td>query <span class="badge optional">optional</span></td><td>Filter by street name</td></tr>
          <tr><td>city</td><td>query <span class="badge optional">optional</span></td><td>Filter by city name</td></tr>
          <tr><td>postcode</td><td>query <span class="badge optional">optional</span></td><td>Filter by postcode</td></tr>
          <tr><td>apiKey</td><td>query <span class="badge required">required</span></td><td>Your API key</td></tr>
          <tr><td>page</td><td>query <span class="badge optional">optional</span></td><td>Page number (default: 1)</td></tr>
          <tr><td>page_size</td><td>query <span class="badge optional">optional</span></td><td>Results per page (default: 20)</td></tr>
        </tbody>
      </table>
      <h3>Example</h3>
      <pre><code>GET /api/addresses/search?q=Downing Street&city=London&apiKey=YOUR_API_KEY</code></pre>
    </div>
  </div>

  <!-- 4. Single Address -->
  <div class="endpoint">
    <div class="endpoint-header">
      <span class="method">GET</span>
      <span class="endpoint-url">/api/addresses/{id}</span>
      <span class="endpoint-desc">Get single address details</span>
    </div>
    <div class="endpoint-body">
      <p>Retrieve the full details for a specific address by its ID, including all enrichment data.</p>
      <table class="params-table">
        <thead><tr><th>Parameter</th><th>Location</th><th>Description</th></tr></thead>
        <tbody>
          <tr><td>id</td><td>path <span class="badge required">required</span></td><td>Address ID (from search or postcode lookup results)</td></tr>
          <tr><td>apiKey</td><td>query <span class="badge required">required</span></td><td>Your API key</td></tr>
        </tbody>
      </table>
      <h3>Example</h3>
      <pre><code>GET /api/addresses/12345?apiKey=YOUR_API_KEY</code></pre>
    </div>
  </div>

  <!-- 5. Health -->
  <div class="endpoint">
    <div class="endpoint-header">
      <span class="method">GET</span>
      <span class="endpoint-url">/api/health</span>
      <span class="endpoint-desc">Service health check</span>
    </div>
    <div class="endpoint-body">
      <p>Returns database connection status and approximate record counts. <strong>No API key required.</strong></p>
      <h3>Response</h3>
      <pre><code>{
  <span class="code-key">"status"</span>: <span class="code-string">"healthy"</span>,
  <span class="code-key">"database"</span>: <span class="code-string">"connected"</span>,
  <span class="code-key">"postcode_count"</span>: 2720643,
  <span class="code-key">"address_count"</span>: 31132720,
  <span class="code-key">"price_paid_count"</span>: 30942156,
  <span class="code-key">"company_count"</span>: 5595871,
  <span class="code-key">"food_rating_count"</span>: 505234
}</code></pre>
    </div>
  </div>
</div>

<!-- Response Codes -->
<div class="section" id="responses">
  <h2><span class="icon" style="background:#fef2f2;color:#dc2626">&#128196;</span> Response Codes</h2>
  <table class="response-codes">
    <thead><tr><th>Code</th><th>Status</th><th>Description</th></tr></thead>
    <tbody>
      <tr><td class="code-200">200</td><td>OK</td><td>Request successful. Response body contains the requested data.</td></tr>
      <tr><td class="code-400">401</td><td>Unauthorized</td><td>API key is missing. Include <code>?apiKey=YOUR_KEY</code> in your request.</td></tr>
      <tr><td class="code-400">403</td><td>Forbidden</td><td>API key is invalid or has been deactivated by the administrator.</td></tr>
      <tr><td class="code-400">404</td><td>Not Found</td><td>The requested postcode or address does not exist in the database.</td></tr>
      <tr><td class="code-400">422</td><td>Unprocessable</td><td>Invalid input format (e.g. malformed postcode or missing required parameters).</td></tr>
      <tr><td class="code-500">500</td><td>Server Error</td><td>An unexpected error occurred. Please contact the administrator.</td></tr>
    </tbody>
  </table>
</div>

<!-- Enrichment Data -->
<div class="section" id="data">
  <h2><span class="icon" style="background:#fefce8;color:#ca8a04">&#128202;</span> Enrichment Data</h2>
  <p>Every address is enriched with data from multiple UK government sources. The following data types may be included with each address:</p>

  <div class="enrichment-grid">
    <div class="enrichment-card">
      <h4><span class="enrichment-icon">&#127968;</span> Price Paid</h4>
      <p>Property sale transactions from HM Land Registry. Includes price, date, property type, and tenure.</p>
    </div>
    <div class="enrichment-card">
      <h4><span class="enrichment-icon">&#127970;</span> Companies</h4>
      <p>Registered companies from Companies House. Includes name, number, status, type, and incorporation date.</p>
    </div>
    <div class="enrichment-card">
      <h4><span class="enrichment-icon">&#127860;</span> Food Ratings</h4>
      <p>Food hygiene ratings from the FSA. Includes rating (0-5), inspection date, and sub-scores.</p>
    </div>
    <div class="enrichment-card">
      <h4><span class="enrichment-icon">&#128176;</span> VOA Ratings</h4>
      <p>Commercial property valuations from the Valuation Office Agency. Includes rateable value and description.</p>
    </div>
  </div>

  <h3 style="margin-top:24px">Property Type Codes</h3>
  <table class="params-table">
    <thead><tr><th>Code</th><th>Type</th></tr></thead>
    <tbody>
      <tr><td>D</td><td>Detached</td></tr>
      <tr><td>S</td><td>Semi-detached</td></tr>
      <tr><td>T</td><td>Terraced</td></tr>
      <tr><td>F</td><td>Flat / Maisonette</td></tr>
      <tr><td>O</td><td>Other</td></tr>
    </tbody>
  </table>

  <h3>Tenure Codes</h3>
  <table class="params-table">
    <thead><tr><th>Code</th><th>Type</th></tr></thead>
    <tbody>
      <tr><td>F</td><td>Freehold</td></tr>
      <tr><td>L</td><td>Leasehold</td></tr>
    </tbody>
  </table>
</div>

<!-- Code Examples -->
<div class="section" id="examples">
  <h2><span class="icon" style="background:#f0fdf4;color:#16a34a">&#128187;</span> Code Examples</h2>

  <div class="code-tabs">
    <div class="code-tab active" onclick="switchTab(event,'js')">JavaScript</div>
    <div class="code-tab" onclick="switchTab(event,'py')">Python</div>
    <div class="code-tab" onclick="switchTab(event,'curl')">cURL</div>
    <div class="code-tab" onclick="switchTab(event,'php')">PHP</div>
    <div class="code-tab" onclick="switchTab(event,'csharp')">C#</div>
  </div>

  <div class="code-panel active" id="panel-js">
    <pre><code><span class="code-comment">// JavaScript / Node.js</span>
const API_KEY = <span class="code-string">'YOUR_API_KEY'</span>;
const BASE = <span class="code-string">'https://getaddress.etakeawaymax.co.uk/api'</span>;

<span class="code-comment">// Postcode lookup</span>
const res = await fetch(
  `${BASE}/postcodes/SW1A1AA?apiKey=${API_KEY}`
);
const data = await res.json();
console.log(`Found ${data.total} addresses`);

<span class="code-comment">// Autocomplete for search-as-you-type</span>
const suggestions = await fetch(
  `${BASE}/postcodes/autocomplete?q=SW1A&apiKey=${API_KEY}`
).then(r => r.json());

<span class="code-comment">// Address search</span>
const results = await fetch(
  `${BASE}/addresses/search?city=London&street=Downing&apiKey=${API_KEY}`
).then(r => r.json());</code></pre>
  </div>

  <div class="code-panel" id="panel-py">
    <pre><code><span class="code-comment"># Python</span>
import requests

API_KEY = <span class="code-string">'YOUR_API_KEY'</span>
BASE = <span class="code-string">'https://getaddress.etakeawaymax.co.uk/api'</span>

<span class="code-comment"># Postcode lookup</span>
r = requests.get(f'{BASE}/postcodes/SW1A1AA', params={'apiKey': API_KEY})
data = r.json()
print(f"Found {data['total']} addresses")

<span class="code-comment"># Address search</span>
r = requests.get(f'{BASE}/addresses/search', params={
    'q': 'Downing Street',
    'city': 'London',
    'apiKey': API_KEY
})</code></pre>
  </div>

  <div class="code-panel" id="panel-curl">
    <pre><code><span class="code-comment"># cURL - Postcode lookup</span>
curl <span class="code-string">"https://getaddress.etakeawaymax.co.uk/api/postcodes/SW1A1AA?apiKey=YOUR_API_KEY"</span>

<span class="code-comment"># Using header authentication</span>
curl -H <span class="code-string">"X-API-Key: YOUR_API_KEY"</span> \\
  <span class="code-string">"https://getaddress.etakeawaymax.co.uk/api/postcodes/SW1A1AA"</span>

<span class="code-comment"># Autocomplete</span>
curl <span class="code-string">"https://getaddress.etakeawaymax.co.uk/api/postcodes/autocomplete?q=SW1A&apiKey=YOUR_API_KEY"</span>

<span class="code-comment"># Address search</span>
curl <span class="code-string">"https://getaddress.etakeawaymax.co.uk/api/addresses/search?city=London&apiKey=YOUR_API_KEY"</span></code></pre>
  </div>

  <div class="code-panel" id="panel-php">
    <pre><code><span class="code-comment">// PHP</span>
$apiKey = <span class="code-string">'YOUR_API_KEY'</span>;
$base = <span class="code-string">'https://getaddress.etakeawaymax.co.uk/api'</span>;

<span class="code-comment">// Postcode lookup</span>
$url = "{$base}/postcodes/SW1A1AA?apiKey={$apiKey}";
$response = file_get_contents($url);
$data = json_decode($response, true);
echo "Found " . $data['total'] . " addresses";

<span class="code-comment">// Using cURL</span>
$ch = curl_init("{$base}/postcodes/SW1A1AA?apiKey={$apiKey}");
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
$response = curl_exec($ch);
$data = json_decode($response, true);
curl_close($ch);</code></pre>
  </div>

  <div class="code-panel" id="panel-csharp">
    <pre><code><span class="code-comment">// C# / .NET</span>
using var client = new HttpClient();
var apiKey = <span class="code-string">"YOUR_API_KEY"</span>;
var baseUrl = <span class="code-string">"https://getaddress.etakeawaymax.co.uk/api"</span>;

<span class="code-comment">// Postcode lookup</span>
var response = await client.GetStringAsync(
    $"{baseUrl}/postcodes/SW1A1AA?apiKey={apiKey}"
);
var data = JsonSerializer.Deserialize&lt;JsonElement&gt;(response);
Console.WriteLine($"Found {data.GetProperty("total")} addresses");

<span class="code-comment">// With header authentication</span>
client.DefaultRequestHeaders.Add(<span class="code-string">"X-API-Key"</span>, apiKey);
var response2 = await client.GetStringAsync(
    $"{baseUrl}/postcodes/SW1A1AA"
);</code></pre>
  </div>
</div>

<!-- Try It -->
<div class="section" id="try">
  <h2><span class="icon" style="background:#ecfdf5;color:#059669">&#9654;</span> Try It Live</h2>
  <p>Test the API directly from your browser. Enter your API key and a postcode to see the response.</p>
  <div class="try-section">
    <label>API Key</label>
    <input type="text" id="tryApiKey" placeholder="Paste your API key here">
    <label>Postcode</label>
    <input type="text" id="tryPostcode" placeholder="e.g. SW1A 1AA, SO23 0QD" value="SW1A1AA">
    <button class="try-btn" onclick="tryApi()">Send Request</button>
    <div class="try-result">
      <pre id="tryResult" style="display:none; max-height:400px; overflow-y:auto"><code id="tryResultCode"></code></pre>
    </div>
  </div>
</div>

<!-- Rate Limits -->
<div class="section">
  <h2><span class="icon" style="background:#fef2f2;color:#dc2626">&#9888;</span> Rate Limits</h2>
  <p>Each API key has a configurable daily request limit (default: 10,000 requests per day). When the limit is exceeded, subsequent requests will be rejected until the next day.</p>
  <p>If you require a higher rate limit, contact your administrator to adjust your key settings.</p>
</div>

</div>

<!-- Footer -->
<div class="footer">
  <p>UK Postcode & Address Lookup API</p>
  <div class="sources">
    <span class="source-tag">Land Registry</span>
    <span class="source-tag">EPC</span>
    <span class="source-tag">Companies House</span>
    <span class="source-tag">FSA</span>
    <span class="source-tag">VOA</span>
    <span class="source-tag">CQC</span>
    <span class="source-tag">Charity Commission</span>
    <span class="source-tag">GIAS Schools</span>
    <span class="source-tag">NHS ODS</span>
  </div>
</div>

<script>
function switchTab(e, lang) {
  document.querySelectorAll('.code-tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.code-panel').forEach(p => p.classList.remove('active'));
  e.target.classList.add('active');
  document.getElementById('panel-' + lang).classList.add('active');
}

async function tryApi() {
  const apiKey = document.getElementById('tryApiKey').value.trim();
  const postcode = document.getElementById('tryPostcode').value.trim().replace(/\\s/g, '');
  const resultEl = document.getElementById('tryResult');
  const codeEl = document.getElementById('tryResultCode');

  if (!apiKey) { alert('Please enter your API key'); return; }
  if (!postcode) { alert('Please enter a postcode'); return; }

  resultEl.style.display = 'block';
  codeEl.textContent = 'Loading...';

  try {
    const res = await fetch(`/api/postcodes/${postcode}?apiKey=${encodeURIComponent(apiKey)}`);
    const data = await res.json();
    codeEl.textContent = JSON.stringify(data, null, 2);
  } catch (err) {
    codeEl.textContent = 'Error: ' + err.message;
  }
}

// Sticky nav active state
const sections = document.querySelectorAll('.section[id]');
const navLinks = document.querySelectorAll('.nav a');
window.addEventListener('scroll', () => {
  let current = '';
  sections.forEach(s => {
    if (window.scrollY >= s.offsetTop - 80) current = s.id;
  });
  navLinks.forEach(a => {
    a.classList.remove('active');
    if (a.getAttribute('href') === '#' + current) a.classList.add('active');
  });
});
</script>

</body>
</html>
""")
