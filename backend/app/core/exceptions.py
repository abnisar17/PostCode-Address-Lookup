class PostcodeLookupError(Exception):
    """Base exception for the entire project."""


class ConfigError(PostcodeLookupError):
    """Missing or invalid configuration / environment variables."""


class DownloadError(PostcodeLookupError):
    """Network failure, bad HTTP status, or hash mismatch."""

    def __init__(
        self,
        message: str,
        *,
        source: str,
        url: str | None = None,
        status_code: int | None = None,
    ):
        self.source = source
        self.url = url
        self.status_code = status_code
        super().__init__(message)


class ParseError(PostcodeLookupError):
    """Malformed source data that failed validation."""

    def __init__(
        self,
        message: str,
        *,
        source: str,
        line: int | None = None,
        detail: str | None = None,
    ):
        self.source = source
        self.line = line
        self.detail = detail
        super().__init__(message)


class DatabaseError(PostcodeLookupError):
    """Connection refused, migration failed, or query error."""


class PipelineError(PostcodeLookupError):
    """Orchestration error — e.g., missing prerequisite step."""
