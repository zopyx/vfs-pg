"""chuk_fsspec - thin fsspec adapter over chuk-virtual-fs.

Works with *any* chuk provider (memory, sqlite, postgres, s3, ...) —
PostgreSQL is only one possible backend.

    from chuk_fsspec import ChukFileSystem
    import fsspec

    fsspec.register_implementation("chuk", ChukFileSystem)
    fs = fsspec.filesystem("chuk", vfs=vfs)
"""

from __future__ import annotations

from chuk_fsspec.fs import ChukBufferedFile, ChukFileSystem

__all__ = ["ChukBufferedFile", "ChukFileSystem"]
