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
<title>UK Postcode & Address Lookup — API Documentation</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; color: #1e293b; background: #f8fafc; line-height: 1.6; }
.header { background: linear-gradient(135deg, #1e40af, #3b82f6); color: white; padding: 40px 20px; text-align: center; }
.header h1 { font-size: 2rem; margin-bottom: 8px; }
.header p { font-size: 1.1rem; opacity: 0.9; }
.container { max-width: 900px; margin: 0 auto; padding: 20px; }
.section { background: white; border-radius: 12px; padding: 24px; margin: 20px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
h2 { font-size: 1.4rem; color: #1e40af; margin-bottom: 16px; padding-bottom: 8px; border-bottom: 2px solid #e2e8f0; }
h3 { font-size: 1.1rem; color: #334155; margin: 20px 0 8px; }
p { margin: 8px 0; color: #475569; }
code { background: #f1f5f9; padding: 2px 6px; border-radius: 4px; font-size: 0.9em; color: #e11d48; font-family: 'SF Mono', 'Fira Code', monospace; }
pre { background: #1e293b; color: #e2e8f0; padding: 16px; border-radius: 8px; overflow-x: auto; margin: 12px 0; font-size: 0.85rem; line-height: 1.5; }
pre code { background: none; color: inherit; padding: 0; }
.endpoint { border: 1px solid #e2e8f0; border-radius: 8px; margin: 16px 0; overflow: hidden; }
.endpoint-header { display: flex; align-items: center; gap: 12px; padding: 12px 16px; background: #f8fafc; border-bottom: 1px solid #e2e8f0; }
.method { background: #22c55e; color: white; padding: 4px 10px; border-radius: 4px; font-size: 0.8rem; font-weight: 700; }
.endpoint-url { font-family: monospace; font-size: 0.9rem; color: #1e293b; word-break: break-all; }
.endpoint-body { padding: 16px; }
.endpoint-body p { font-size: 0.9rem; }
.params-table { width: 100%; border-collapse: collapse; margin: 12px 0; font-size: 0.85rem; }
.params-table th { text-align: left; padding: 8px 12px; background: #f8fafc; color: #64748b; font-weight: 600; font-size: 0.75rem; text-transform: uppercase; border-bottom: 2px solid #e2e8f0; }
.params-table td { padding: 8px 12px; border-bottom: 1px solid #f1f5f9; }
.params-table td:first-child { font-family: monospace; color: #e11d48; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 0.7rem; font-weight: 600; }
.required { background: #fee2e2; color: #991b1b; }
.optional { background: #e0f2fe; color: #075985; }
.response-codes { width: 100%; border-collapse: collapse; margin: 12px 0; }
.response-codes td, .response-codes th { padding: 8px 12px; text-align: left; border-bottom: 1px solid #f1f5f9; font-size: 0.9rem; }
.response-codes th { background: #f8fafc; color: #64748b; font-weight: 600; font-size: 0.75rem; text-transform: uppercase; }
.code-200 { color: #16a34a; font-weight: 700; }
.code-400 { color: #ea580c; font-weight: 700; }
.code-500 { color: #dc2626; font-weight: 700; }
.tab-container { display: flex; gap: 0; border-bottom: 2px solid #e2e8f0; margin-bottom: 0; }
.tab { padding: 8px 16px; cursor: pointer; font-size: 0.85rem; font-weight: 500; color: #64748b; border-bottom: 2px solid transparent; margin-bottom: -2px; }
.tab.active { color: #1e40af; border-bottom-color: #1e40af; }
.tab-content { display: none; }
.tab-content.active { display: block; }
.try-it { margin-top: 12px; padding: 12px; background: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 8px; }
.try-it input { width: 100%; padding: 8px 12px; border: 1px solid #d1d5db; border-radius: 6px; font-family: monospace; font-size: 0.85rem; margin: 4px 0; }
.try-it button { padding: 8px 16px; background: #3b82f6; color: white; border: none; border-radius: 6px; cursor: pointer; font-size: 0.85rem; margin-top: 4px; }
.try-it button:hover { background: #2563eb; }
#tryResult { margin-top: 8px; }
.note { background: #fffbeb; border: 1px solid #fde68a; border-radius: 8px; padding: 12px 16px; margin: 12px 0; font-size: 0.9rem; color: #92400e; }
.footer { text-align: center; padding: 30px 20px; color: #94a3b8; font-size: 0.85rem; }
@media (max-width: 640px) {
  .header h1 { font-size: 1.4rem; }
  .header p { font-size: 0.9rem; }
  .section { padding: 16px; }
  pre { font-size: 0.75rem; padding: 12px; }
  .endpoint-header { flex-direction: column; align-items: flex-start; gap: 6px; }
}
</style>
</head>
<body>

<div class="header">
  <h1>UK Postcode & Address Lookup API</h1>
  <p>Access 31M+ UK addresses with property prices, company data, and food ratings</p>
</div>

<div class="container">

<!-- Authentication -->
<div class="section">
  <h2>Authentication</h2>
  <p>All API requests require an API key. You can pass it in two ways:</p>

  <h3>Option 1: Query Parameter</h3>
  <pre><code>GET /api/postcodes/SW1A1AA?apiKey=YOUR_API_KEY</code></pre>

  <h3>Option 2: HTTP Header</h3>
  <pre><code>X-API-Key: YOUR_API_KEY</code></pre>

  <div class="note">
    Contact your administrator to obtain an API key. Each key has a daily rate limit (default: 10,000 requests/day).
  </div>
</div>

<!-- Endpoints -->
<div class="section">
  <h2>Endpoints</h2>

  <!-- 1. Postcode Lookup -->
  <div class="endpoint">
    <div class="endpoint-header">
      <span class="method">GET</span>
      <span class="endpoint-url">/api/postcodes/{postcode}</span>
    </div>
    <div class="endpoint-body">
      <p>Look up a postcode and get all addresses linked to it, including property prices, company registrations, and food hygiene ratings.</p>
      <table class="params-table">
        <thead><tr><th>Parameter</th><th>Type</th><th>Description</th></tr></thead>
        <tbody>
          <tr><td>postcode</td><td>path <span class="badge required">required</span></td><td>UK postcode (e.g. SW1A1AA, SW1A 1AA, sw1a1aa)</td></tr>
          <tr><td>apiKey</td><td>query <span class="badge required">required</span></td><td>Your API key</td></tr>
          <tr><td>page</td><td>query <span class="badge optional">optional</span></td><td>Page number (default: 1)</td></tr>
          <tr><td>page_size</td><td>query <span class="badge optional">optional</span></td><td>Results per page (default: 20, max: 100)</td></tr>
        </tbody>
      </table>
      <h3>Example Request</h3>
      <pre><code>GET /api/postcodes/SW1A1AA?apiKey=YOUR_API_KEY&page=1&page_size=20</code></pre>
      <h3>Example Response</h3>
      <pre><code>{
  "postcode": {
    "postcode": "SW1A 1AA",
    "postcode_no_space": "SW1A1AA",
    "latitude": 51.501009,
    "longitude": -0.141588
  },
  "total": 12,
  "page": 1,
  "page_size": 20,
  "address_count": 12,
  "addresses": [
    {
      "id": 12345,
      "house_number": "10",
      "street": "Downing Street",
      "city": "London",
      "county": "GREATER LONDON",
      "postcode_raw": "SW1A 1AA",
      "source": "land_registry",
      "confidence": 0.8,
      "price_paid": [
        {
          "price": 500000,
          "date_of_transfer": "2020-01-15",
          "property_type": "T",
          "duration": "F"
        }
      ],
      "companies": [],
      "food_ratings": [],
      "voa_ratings": []
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
    </div>
    <div class="endpoint-body">
      <p>Type-ahead postcode search. Returns matching postcodes for a given prefix.</p>
      <table class="params-table">
        <thead><tr><th>Parameter</th><th>Type</th><th>Description</th></tr></thead>
        <tbody>
          <tr><td>q</td><td>query <span class="badge required">required</span></td><td>Postcode prefix, min 2 chars (e.g. SW1A, EC1)</td></tr>
          <tr><td>apiKey</td><td>query <span class="badge required">required</span></td><td>Your API key</td></tr>
          <tr><td>limit</td><td>query <span class="badge optional">optional</span></td><td>Max results (default: 10, max: 50)</td></tr>
        </tbody>
      </table>
      <h3>Example Request</h3>
      <pre><code>GET /api/postcodes/autocomplete?q=SW1A&apiKey=YOUR_API_KEY</code></pre>
      <h3>Example Response</h3>
      <pre><code>{
  "query": "SW1A",
  "count": 3,
  "results": [
    { "postcode": "SW1A 0AA", "postcode_no_space": "SW1A0AA" },
    { "postcode": "SW1A 0AB", "postcode_no_space": "SW1A0AB" },
    { "postcode": "SW1A 1AA", "postcode_no_space": "SW1A1AA" }
  ]
}</code></pre>
    </div>
  </div>

  <!-- 3. Address Search -->
  <div class="endpoint">
    <div class="endpoint-header">
      <span class="method">GET</span>
      <span class="endpoint-url">/api/addresses/search</span>
    </div>
    <div class="endpoint-body">
      <p>Search addresses by free text, street, city, or postcode. At least one search parameter is required.</p>
      <table class="params-table">
        <thead><tr><th>Parameter</th><th>Type</th><th>Description</th></tr></thead>
        <tbody>
          <tr><td>q</td><td>query <span class="badge optional">optional</span></td><td>Free text search (min 2 chars)</td></tr>
          <tr><td>street</td><td>query <span class="badge optional">optional</span></td><td>Street name filter</td></tr>
          <tr><td>city</td><td>query <span class="badge optional">optional</span></td><td>City name filter</td></tr>
          <tr><td>postcode</td><td>query <span class="badge optional">optional</span></td><td>Postcode filter</td></tr>
          <tr><td>apiKey</td><td>query <span class="badge required">required</span></td><td>Your API key</td></tr>
          <tr><td>page</td><td>query <span class="badge optional">optional</span></td><td>Page number (default: 1)</td></tr>
          <tr><td>page_size</td><td>query <span class="badge optional">optional</span></td><td>Results per page (default: 20)</td></tr>
        </tbody>
      </table>
      <h3>Example Request</h3>
      <pre><code>GET /api/addresses/search?q=Downing Street&city=London&apiKey=YOUR_API_KEY</code></pre>
    </div>
  </div>

  <!-- 4. Single Address -->
  <div class="endpoint">
    <div class="endpoint-header">
      <span class="method">GET</span>
      <span class="endpoint-url">/api/addresses/{id}</span>
    </div>
    <div class="endpoint-body">
      <p>Get full details for a single address including all enrichment data.</p>
      <table class="params-table">
        <thead><tr><th>Parameter</th><th>Type</th><th>Description</th></tr></thead>
        <tbody>
          <tr><td>id</td><td>path <span class="badge required">required</span></td><td>Address ID from search results</td></tr>
          <tr><td>apiKey</td><td>query <span class="badge required">required</span></td><td>Your API key</td></tr>
        </tbody>
      </table>
      <h3>Example Request</h3>
      <pre><code>GET /api/addresses/12345?apiKey=YOUR_API_KEY</code></pre>
    </div>
  </div>

  <!-- 5. Health -->
  <div class="endpoint">
    <div class="endpoint-header">
      <span class="method">GET</span>
      <span class="endpoint-url">/api/health</span>
    </div>
    <div class="endpoint-body">
      <p>Health check endpoint. Returns database status and record counts. <strong>No API key required.</strong></p>
      <h3>Example Response</h3>
      <pre><code>{
  "status": "healthy",
  "database": "connected",
  "postcode_count": 2720643,
  "address_count": 31132720,
  "price_paid_count": 30942156,
  "company_count": 5595871,
  "food_rating_count": 505234
}</code></pre>
    </div>
  </div>
</div>

<!-- Response Codes -->
<div class="section">
  <h2>Response Codes</h2>
  <table class="response-codes">
    <thead><tr><th>Code</th><th>Meaning</th><th>Description</th></tr></thead>
    <tbody>
      <tr><td class="code-200">200</td><td>Success</td><td>Request successful, data returned</td></tr>
      <tr><td class="code-400">401</td><td>Unauthorized</td><td>API key missing from request</td></tr>
      <tr><td class="code-400">403</td><td>Forbidden</td><td>API key is invalid or has been deactivated</td></tr>
      <tr><td class="code-400">404</td><td>Not Found</td><td>Postcode or address not found in database</td></tr>
      <tr><td class="code-400">422</td><td>Unprocessable</td><td>Invalid postcode format or missing parameters</td></tr>
      <tr><td class="code-500">500</td><td>Server Error</td><td>Internal server error — contact admin</td></tr>
    </tbody>
  </table>
</div>

<!-- Enrichment Data -->
<div class="section">
  <h2>Enrichment Data</h2>
  <p>Each address may include linked data from multiple government sources:</p>

  <h3>Price Paid (Land Registry)</h3>
  <p>Property sale transactions including price, date, property type (D=Detached, S=Semi-detached, T=Terraced, F=Flat, O=Other) and tenure (F=Freehold, L=Leasehold).</p>

  <h3>Companies (Companies House)</h3>
  <p>Registered companies at the address including company name, number, status, type, and incorporation date.</p>

  <h3>Food Ratings (FSA)</h3>
  <p>Food hygiene ratings including business name, rating (0-5), inspection date, and hygiene/structural/management scores.</p>

  <h3>VOA Ratings</h3>
  <p>Non-domestic property valuations including rateable value, description, and effective date.</p>
</div>

<!-- Code Examples -->
<div class="section">
  <h2>Code Examples</h2>

  <h3>JavaScript / Node.js</h3>
  <pre><code>const API_KEY = 'YOUR_API_KEY';
const BASE_URL = 'https://getaddress.etakeawaymax.co.uk/api';

// Postcode lookup
const response = await fetch(
  `${BASE_URL}/postcodes/SW1A1AA?apiKey=${API_KEY}`
);
const data = await response.json();
console.log(`Found ${data.total} addresses`);

// Autocomplete
const suggestions = await fetch(
  `${BASE_URL}/postcodes/autocomplete?q=SW1A&apiKey=${API_KEY}`
).then(r => r.json());
</code></pre>

  <h3>Python</h3>
  <pre><code>import requests

API_KEY = 'YOUR_API_KEY'
BASE_URL = 'https://getaddress.etakeawaymax.co.uk/api'

# Postcode lookup
response = requests.get(
    f'{BASE_URL}/postcodes/SW1A1AA',
    params={'apiKey': API_KEY}
)
data = response.json()
print(f"Found {data['total']} addresses")

# Address search
response = requests.get(
    f'{BASE_URL}/addresses/search',
    params={'q': 'Downing Street', 'city': 'London', 'apiKey': API_KEY}
)
</code></pre>

  <h3>cURL</h3>
  <pre><code># Postcode lookup
curl "https://getaddress.etakeawaymax.co.uk/api/postcodes/SW1A1AA?apiKey=YOUR_API_KEY"

# With header authentication
curl -H "X-API-Key: YOUR_API_KEY" \\
  "https://getaddress.etakeawaymax.co.uk/api/postcodes/SW1A1AA"

# Autocomplete
curl "https://getaddress.etakeawaymax.co.uk/api/postcodes/autocomplete?q=SW1A&apiKey=YOUR_API_KEY"
</code></pre>

  <h3>PHP</h3>
  <pre><code>$apiKey = 'YOUR_API_KEY';
$postcode = 'SW1A1AA';
$url = "https://getaddress.etakeawaymax.co.uk/api/postcodes/{$postcode}?apiKey={$apiKey}";

$response = file_get_contents($url);
$data = json_decode($response, true);
echo "Found " . $data['total'] . " addresses";
</code></pre>
</div>

<!-- Try It -->
<div class="section">
  <h2>Try It</h2>
  <div class="try-it">
    <label><strong>API Key:</strong></label>
    <input type="text" id="tryApiKey" placeholder="Paste your API key here">
    <label><strong>Postcode:</strong></label>
    <input type="text" id="tryPostcode" placeholder="e.g. SW1A 1AA" value="SW1A1AA">
    <button onclick="tryApi()">Send Request</button>
    <pre id="tryResult" style="display:none"><code id="tryResultCode"></code></pre>
  </div>
</div>

<!-- Rate Limits -->
<div class="section">
  <h2>Rate Limits</h2>
  <p>Each API key has a daily request limit (default: 10,000 requests per day). If you need a higher limit, contact your administrator.</p>
  <p>Rate limit headers are not currently included in responses. Monitor your usage through the admin dashboard.</p>
</div>

</div>

<div class="footer">
  <p>UK Postcode & Address Lookup API &mdash; Powered by 9 government data sources</p>
  <p>Data from Land Registry, EPC, Companies House, FSA, VOA, CQC, Charity Commission, Schools, NHS</p>
</div>

<script>
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
</script>

</body>
</html>
""")
