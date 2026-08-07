"""chuk_vfs_postgres - PostgreSQL storage provider for chuk-virtual-fs."""

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
]

# register so `AsyncVirtualFileSystem("postgres", dsn=...)` just works
register_provider("postgres", PostgresStorageProvider)
