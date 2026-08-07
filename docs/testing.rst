Testing
=======

The test suite runs against a **throwaway PostgreSQL server started by
testcontainers** (``postgres:16-alpine``) — no docker-compose, no manual
setup, and no risk to existing databases.

.. code-block:: bash

    uv sync --extra test
    uv run pytest -v

    # optional: run against an existing server instead of a container
    VFS_PG_DSN=postgresql://user:pass@host:5432/testdb uv run pytest -v

Safety
------

- The ``clean_db`` fixture truncates the VFS tables before every test.
  When an external database is supplied via ``VFS_PG_DSN``, truncation is
  **refused** unless the database name contains ``test`` or
  ``VFS_PG_ALLOW_TRUNCATE=1`` is set explicitly.
- The container image can be changed with ``VFS_PG_IMAGE``.

Suite layout
------------

- ``tests/test_postgres_provider.py`` — the full provider API: lifecycle,
  schema, nodes, content write/read, chunk-aware ranges (including
  cross-instance chunk-size reads), move/delete semantics, metadata,
  transaction joining and chuk VFS integration.
- ``tests/test_fsspec.py`` — the sync fsspec contract: pipe/cat, buffered
  open (wb/rb/ab/xb), seek + range reads across chunk and block boundaries,
  mkdir/mv/cp/rm, URL access, exclusive-create races, entry-point
  registration.
- ``tests/test_concurrency.py`` — concurrent initialize (deadlock
  regression), duplicate-create races, exclusive-write races, metadata
  merge races, atomicity under concurrent reads.

Coverage of the acceptance criteria in ``tasks.md`` is tracked there;
regression tests exist for every P0/P1 item.
