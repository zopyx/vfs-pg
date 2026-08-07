Quickstart
==========

Install
-------

.. code-block:: bash

    pip install vfs-pg
    # or with uv:
    uv add vfs-pg

Requirements: Python >= 3.12 and a running PostgreSQL >= 13
(``docker compose up -d`` in this repository provides one).

Provider + chuk VFS
-------------------

.. code-block:: python

    import asyncio
    import chuk_vfs_postgres  # registers the "postgres" provider
    from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem

    async def main():
        vfs = AsyncVirtualFileSystem(
            "postgres", dsn="postgresql://vfs:vfs@localhost:5432/vfs"
        )
        await vfs.initialize()
        await vfs.write_file("/hello.txt", "Hello")
        print(await vfs.read_text("/hello.txt"))
        await vfs.close()

    asyncio.run(main())

fsspec adapter
--------------

.. code-block:: python

    import fsspec
    from chuk_fsspec import ChukFileSystem

    fs = fsspec.filesystem("chuk", vfs=vfs)        # or ChukFileSystem(vfs)

    fs.pipe_file("/datasets/data.csv", b"a,b\n1,2\n")
    fs.cat_file("/datasets/data.csv")              # -> b"a,b\n1,2\n"

    with fs.open("/big.bin", "wb") as f:           # buffered write
        f.write(b"chunk" * 1000)

    with fs.open("/big.bin", "rb") as f:           # buffered, range-aware read
        f.seek(1_000_000)
        f.read(64 * 1024)

    fs.mv("/datasets/data.csv", "/archive/data.csv")
    fs.rm("/archive", recursive=True)

URL-based access (``chuk://``) works through the registered entry point:

.. code-block:: python

    with fsspec.open("chuk:///datasets/data.csv", "rb", vfs=vfs) as f:
        print(f.read())

Atomic transactions with business tables
----------------------------------------

.. code-block:: python

    import psycopg
    from chuk_vfs_postgres import PostgresStorageProvider

    conn = await psycopg.AsyncConnection.connect(DSN)
    async with conn.transaction():
        await conn.execute("UPDATE documents SET status='generated' WHERE id=123")
        provider = PostgresStorageProvider(conn=conn)   # joins the transaction
        await provider.write_file("/documents/123/result.pdf", pdf)
        # everything commits together — or rolls back together

Exclusive creation
------------------

fsspec's ``xb`` mode is enforced by the database's unique constraint: of two
concurrent exclusive creates, exactly one succeeds; the loser raises
``FileExistsError``.

.. code-block:: python

    with fs.open("/locks/worker-1", "xb") as f:
        f.write(b"lease")        # FileExistsError if the path already exists

Configuration
-------------

- ``PostgresStorageProvider(dsn=..., chunk_size=...)`` — chunk size in bytes
  (positive integer; persisted per file).
- ``VFS_PG_DSN`` environment variable overrides the default DSN in the
  examples.
- `Concurrency`_: schema initialization is serialized with a PostgreSQL
  advisory lock; concurrent ``initialize()`` calls cannot deadlock.
