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

    # Data source URLs
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

    # Ingestion tuning
    batch_size: int = 2_000
    osm_index_type: str = "sparse_file_array"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Logging
    log_level: str = "INFO"
    log_format: str = "console"  # 'console' (dev) or 'json' (production)

    @property
    def osm_file(self) -> Path:
        return self.data_dir / "great-britain-latest.osm.pbf"

    @property
    def codepoint_file(self) -> Path:
        return self.data_dir / "codepoint-open.zip"

    @property
    def nspl_file(self) -> Path:
        return self.data_dir / "nspl.zip"
