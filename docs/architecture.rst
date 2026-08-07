Architecture
============

Layering
--------

.. code-block:: text

    fsspec.open() / pd.read_csv("chuk://...")
            │
            ▼
    ChukFileSystem (chuk_fsspec, AsyncFileSystem)
            │
            ▼
    chuk AsyncVirtualFileSystem
            │
            ▼
    PostgresStorageProvider (chuk_vfs_postgres, psycopg async)
            │
            ▼
    PostgreSQL 13+ (testcontainers / docker compose)

``chuk_fsspec`` is provider-agnostic: it works with *any* chuk provider
(memory, sqlite, postgres, s3, ...). ``chuk_vfs_postgres`` is one backend.

Schema
------

.. code-block:: sql

    vfs_nodes                          vfs_chunks
    ─────────────────────────          ─────────────────────────
    node_id     uuid PK                node_id  uuid FK -> vfs_nodes
    parent_id   uuid FK -> node        chunk_no int
    name        text                   data     bytea
    is_dir      bool                   PK (node_id, chunk_no)
    size        bigint
    chunk_size  integer   -- per-file chunk size
    sha256      text
    mime_type   text
    created_at / modified_at  timestamptz
    metadata    jsonb

- **``parent_id + name`` instead of a full path column**: rename/move is a
  single ``UPDATE`` — no content copies, no path rewrites.
- **Unique index on ``(parent_id, name)``** (non-root rows): the filesystem
  invariant "no duplicate siblings" is enforced by the database. A
  migration guard refuses to install the index while duplicates exist.
- **Partial unique index** guarantees exactly one root per database.
- **Chunk size is persisted per file**: ``read_range()`` uses the writer's
  chunk size, so any provider instance can read any file correctly.

Concurrency and atomicity
-------------------------

- Every mutating provider method runs in a single transaction:
  content replacement (size, sha256, chunks) is all-or-nothing — readers
  never observe a partial file version.
- ``write_file_atomic()`` creates a missing node *and* writes its content in
  one transaction (no touch-then-write round trip) and enforces
  ``exclusive`` through the unique constraint.
- Schema initialization is serialized with a transaction-scoped advisory
  lock (``pg_advisory_xact_lock``); concurrent ``initialize()`` calls cannot
  deadlock.
- ``initialize()`` is idempotent and failure-safe: a failed initialization
  closes its pool and leaves the provider re-initializable.
- Metadata updates merge atomically in SQL (``metadata || %s::jsonb``), so
  concurrent updates of different keys never lose data.

fsspec adapter details
----------------------

- ``AsyncFileSystem`` with ``async_impl = True``: async ``_``-methods plus
  auto-generated sync wrappers; plain sync user code works.
- ``AbstractBufferedFile`` subclass: writes are buffered and committed once
  on ``close()`` (``_upload_chunk(final=True)``); reads map
  ``_fetch_range`` onto the provider's chunk-aware ``read_range``.
- ``xb`` exclusivity is enforced twice: at open (fast preflight) and at
  commit (database unique constraint).
- ``ls(..., detail=False)`` returns full paths (fsspec convention);
  ``mkdir`` honours ``create_parents``; ``mv``/``cp``/``rm`` follow fsspec
  semantics (directory moves need ``recursive=True``, matching fsspec's
  copy+rm implementation).

Range reads
-----------

``read_range(path, start, end)`` fetches only the chunks overlapping
``[start, end)``. ``start`` is clamped to 0, ``end`` to EOF, and
``end <= start`` yields ``b""``. Cross-chunk, exact-boundary, empty-file and
EOF-clamping behaviour is covered by the test suite.
