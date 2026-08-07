Testing
=======

The test suite runs against a **throwaway PostgreSQL server started by
testcontainers** (``postgres:16-alpine``) — no docker-compose or manual setup.
A random ``VFS_PG_FILESYSTEM_ID`` isolates every test session, including
sessions pointed at an existing shared database. The current suite contains
152 tests.

.. code-block:: bash

    uv sync --extra test
    uv run pytest -v

    # optional: run against an existing server instead of a container
    VFS_PG_DSN=postgresql://user:pass@host:5432/testdb uv run pytest -v

Safety
------

- The ``clean_db`` fixture deletes only the current session namespace's root
  children and staging uploads. It never uses ``TRUNCATE`` and never examines
  the database name to infer safety.
- Session teardown deletes the random test root, cascading only that
  namespace's nodes, chunks, and uploads. Tests never truncate shared VFS
  tables, including when ``VFS_PG_DSN`` points at an existing server.
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
