from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    database_url: str = (
        "postgresql+psycopg://postgres:postgres@localhost:5432/postcode_lookup"
    )

    # Paths
    data_dir: Path = Path("data")

    # ── Original data source URLs ──────────────────────────────────
    osm_download_url: str = (
        "https://download.geofabrik.de/europe/great-britain-latest.osm.pbf"
    )
    codepoint_download_url: str = (
        "https://api.os.uk/downloads/v1/products/CodePointOpen/downloads"
        "?area=GB&format=CSV&redirect"
    )
    nspl_download_url: str = (
        "https://www.arcgis.com/sharing/rest/content/items/"
        "8a1d5b58df824b2e86fe07ddfdd87165/data"
    )

    # ── New data source URLs ───────────────────────────────────────

    # HM Land Registry Price Paid Data (complete CSV, ~4.3GB)
    land_registry_download_url: str = (
        "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
        "/pp-complete.csv"
    )

    # Companies House basic company data (ZIP of CSV, ~400MB)
    # URL changes monthly — set via .env or use the latest redirect
    companies_house_download_url: str = (
        "http://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2026-02-01.zip"
    )

    # FSA Food Hygiene Ratings — uses paginated API, not bulk download
    fsa_api_base_url: str = "https://api.ratings.food.gov.uk"

    # OS Open UPRN (CSV, ~1.4GB)
    open_uprn_download_url: str = (
        "https://api.os.uk/downloads/v1/products/OpenUPRN/downloads"
        "?area=GB&format=CSV&redirect"
    )

    # EPC Open Data — requires free registration for API key
    epc_api_base_url: str = "https://epc.opendatacommunities.org/api/v1"
    epc_api_key: str = ""  # Set via .env after registration at epc.opendatacommunities.org

    # VOA Non-Domestic Rating List (2023 compiled list, ZIP of CSV, ~350MB)
    voa_download_url: str = (
        "https://voaratinglists.blob.core.windows.net/downloads/"
        "uk-englandwales-ndr-2023-listentries-compiled-epoch-0019-baseline-csv.zip"
    )

    # Ingestion tuning
    batch_size: int = 10_000
    osm_index_type: str = "sparse_file_array"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Logging
    log_level: str = "INFO"
    log_format: str = "console"  # 'console' (dev) or 'json' (production)

    # ── File path properties ───────────────────────────────────────

    @property
    def osm_file(self) -> Path:
        return self.data_dir / "great-britain-latest.osm.pbf"

    @property
    def codepoint_file(self) -> Path:
        return self.data_dir / "codepoint-open.zip"

    @property
    def nspl_file(self) -> Path:
        return self.data_dir / "nspl.zip"

    @property
    def land_registry_file(self) -> Path:
        return self.data_dir / "pp-complete.csv"

    @property
    def companies_house_file(self) -> Path:
        return self.data_dir / "companies-house.zip"

    @property
    def open_uprn_file(self) -> Path:
        return self.data_dir / "open-uprn.zip"

    @property
    def voa_file(self) -> Path:
        return self.data_dir / "voa-rating-list.zip"

    @property
    def epc_dir(self) -> Path:
        return self.data_dir / "epc"
