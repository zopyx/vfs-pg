vfs-pg
======

``vfs-pg`` provides a **PostgreSQL storage provider for chuk-virtual-fs**
plus a thin **fsspec adapter** that exposes any chuk virtual filesystem as
the ``chuk://`` protocol.

.. code-block:: python

    import fsspec
    from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem
    import chuk_vfs_postgres  # registers the "postgres" provider

    vfs = AsyncVirtualFileSystem("postgres", dsn="postgresql://vfs:vfs@localhost:5432/vfs")
    await vfs.initialize()

    fs = fsspec.filesystem("chuk", vfs=vfs)     # sync fsspec API
    fs.pipe_file("/datasets/data.csv", b"a,b\n1,2\n")

Features
--------

- **Chunked storage** — files are stored in fixed-size chunks (1 MiB
  default, persisted per file), so range reads touch only the chunks that
  overlap the requested window.
- **Atomic transactions** — the provider can join an existing connection,
  committing filesystem metadata, file content and business-table rows
  together (or rolling them all back).
- **Tenant namespaces** — paths, content, staging uploads and statistics are
  scoped by ``filesystem_id`` so isolated filesystems can share one database.
- **Bounded streaming writes** — fsspec buffers are staged in PostgreSQL and
  the completed version is published atomically; appends serialize on the
  target row so concurrent suffixes are not lost.
- **Database-enforced integrity** — a unique index on
  ``(filesystem_id, parent_id, name)`` makes duplicate sibling creation
  impossible within a namespace, even under concurrency;
  exclusive creation (fsspec ``xb``) is enforced by the database, not by a
  preflight check.
- **fsspec integration** — ``chuk://`` protocol, buffered file objects with
  seek/range reads, ``pipe``/``cat``, ``open`` (wb/rb/ab/xb), ``mv``/``cp``/
  ``rm``, ``mkdir`` — registered via a ``fsspec.specs`` entry point.
- **Self-describing layout** — every file records its chunk size, so any
  provider instance can range-read any file.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   quickstart
   architecture
   api
   testing
   changelog
