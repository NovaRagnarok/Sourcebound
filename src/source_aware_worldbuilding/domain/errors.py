class CanonUnavailableError(RuntimeError):
    """Raised when canonical Wikibase-backed claim storage is unavailable."""


class WikibaseSyncError(RuntimeError):
    """Raised when canonical Wikibase sync cannot complete."""


class ZoteroError(RuntimeError):
    """Base class for Zotero read/write/configuration failures."""

    kind = "zotero_error"


class ZoteroConfigError(ZoteroError):
    """Raised when Zotero configuration is incomplete or invalid."""

    kind = "config"


class ZoteroAuthError(ZoteroError):
    """Raised when Zotero credentials are rejected."""

    kind = "auth"


class ZoteroNotFoundError(ZoteroError):
    """Raised when the configured Zotero library or item cannot be found."""

    kind = "not_found"


class ZoteroRateLimitError(ZoteroError):
    """Raised when the Zotero API asks the client to slow down."""

    kind = "rate_limit"


class ZoteroFetchError(ZoteroError):
    """Raised when Zotero data or attachments cannot be fetched."""

    kind = "fetch"


class ZoteroExtractionError(ZoteroError):
    """Raised when fetched Zotero attachment text cannot be extracted."""

    kind = "extraction"


class ZoteroWriteError(ZoteroError):
    """Raised when Zotero write operations cannot complete."""

    kind = "write"


class WorkerUnavailableError(RuntimeError):
    """Raised when persisted background work is requested without an active worker."""
