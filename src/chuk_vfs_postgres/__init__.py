"""chuk_vfs_postgres - PostgreSQL storage provider for chuk-virtual-fs.

Exposes :class:`PostgresStorageProvider`, a chunked, transaction-capable
storage backend, and registers it with chuk so that
``AsyncVirtualFileSystem("postgres", dsn=...)`` just works.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError

try:
    from importlib.metadata import version as _version

    __version__ = _version("vfs-pg")
except PackageNotFoundError:  # pragma: no cover - editable/development installs
    __version__ = "0.2.0"

from chuk_virtual_fs.providers import register_provider

from chuk_vfs_postgres.provider import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_DSN,
    SCHEMA_SQL,
    PostgresStorageProvider,
)

__all__ = [
    "DEFAULT_CHUNK_SIZE",
    "DEFAULT_DSN",
    "SCHEMA_SQL",
    "PostgresStorageProvider",
    "__version__",
]

# register so `AsyncVirtualFileSystem("postgres", dsn=...)` just works
register_provider("postgres", PostgresStorageProvider)
