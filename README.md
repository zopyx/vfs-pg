# vfs-pg — PostgreSQL storage provider for chuk-virtual-fs + fsspec adapter

Prototype implementing the architecture from the design discussion:

> **PostgreSQL as a chuk storage provider**, with **fsspec as a thin adapter on
> top of chuk** — not another independent PostgreSQL implementation.

```
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
PostgreSQL 16 (docker compose)
```

## Packages

| package            | role                                                                 |
|--------------------|----------------------------------------------------------------------|
| `chuk_vfs_postgres`| `PostgresStorageProvider` — implements the chuk async provider API   |
| `chuk_fsspec`      | `ChukFileSystem` + `ChukBufferedFile` — `chuk://` protocol for fsspec |

`chuk_fsspec` works with **any** chuk provider (memory, sqlite, s3, …) — the
postgres provider is just one backend.

## Quick start

```bash
docker compose up -d          # postgres:16 on localhost:5432 (vfs/vfs/vfs)
uv sync                       # install deps
uv run pytest -v              # run the test suite
uv run python examples/demo.py
```

DSN override: `VFS_PG_DSN=postgresql://user:pass@host:5432/db`.

## Schema

```
vfs_nodes                          vfs_chunks
─────────────────────────          ─────────────────────────
node_id     uuid PK                node_id  uuid FK -> vfs_nodes
parent_id   uuid FK -> node        chunk_no int
name        text                   data     bytea
is_dir      bool                   PK (node_id, chunk_no)
size        bigint
sha256      text
mime_type   text
created_at / modified_at  timestamptz
metadata    jsonb
```

- **`parent_id + name`** instead of a full path column: rename/move is a single
  `UPDATE` (no content copies, no path rewrites). Unique constraint
  `(parent_id, name)`; a partial unique index guarantees exactly one root.
- **Content is chunked** (1 MiB default) so range reads only touch the chunks
  overlapping the window (`PostgresStorageProvider.read_range`), which maps
  directly onto fsspec's `AbstractBufferedFile._fetch_range`.

## The killer feature: atomic transactions

The provider can join an existing connection, so business tables, filesystem
metadata and file content commit atomically:

```python
import psycopg
from chuk_vfs_postgres import PostgresStorageProvider

conn = await psycopg.AsyncConnection.connect(DSN)
async with conn.transaction():
    await conn.execute("UPDATE documents SET status='generated' WHERE id=123")
    provider = PostgresStorageProvider(conn=conn)          # joins the tx
    await provider.write_file("/documents/123/result.pdf", pdf)
    # everything commits together — or rolls back together
```

## Usage

```python
import fsspec
from chuk_fsspec import ChukFileSystem
from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem

vfs = AsyncVirtualFileSystem("postgres", dsn="postgresql://vfs:vfs@localhost:5432/vfs")
await vfs.initialize()

fs = ChukFileSystem(vfs)                  # or: fsspec.filesystem("chuk", vfs=vfs)
fs.pipe_file("/datasets/data.csv", b"a,b\n1,2\n")
df_like = fs.cat_file("/datasets/data.csv")

with fs.open("/big.bin", "rb") as f:      # buffered, range-aware
    f.seek(1_000_000)
    data = f.read(64 * 1024)
```

The `postgres` provider name is registered on import of `chuk_vfs_postgres`
(`register_provider`), so `AsyncVirtualFileSystem("postgres", ...)` just works.

## Notes / prototype scope

- Writes are buffered in memory and written atomically on `close()`
  (`_upload_chunk(final=True)`); fine for files up to tens of MB.
- `read_range` clamps to EOF; missing files raise `FileNotFoundError` (fsspec
  convention).
- No TTL/expiry support yet (`cleanup()` is a no-op), no locking beyond
  Postgres transactions, `ab` append works via read-modify-write.
