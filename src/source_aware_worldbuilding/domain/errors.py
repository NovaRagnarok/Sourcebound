class CanonUnavailableError(RuntimeError):
    """Raised when canonical Wikibase-backed claim storage is unavailable."""


class WikibaseSyncError(RuntimeError):
    """Raised when canonical Wikibase sync cannot complete."""


class ZoteroWriteError(RuntimeError):
    """Raised when Zotero write operations cannot complete."""


class WorkerUnavailableError(RuntimeError):
    """Raised when persisted background work is requested without an active worker."""
