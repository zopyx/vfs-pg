# vfs-pg — PostgreSQL storage provider for chuk-virtual-fs + fsspec adapter

[![CI](https://github.com/zopyx/vfs-pg/actions/workflows/ci.yml/badge.svg)](https://github.com/zopyx/vfs-pg/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.12%20|%203.13%20|%203.14-blue)](https://pypi.org/project/vfs-pg/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

`vfs-pg` provides a **PostgreSQL storage provider for
[chuk-virtual-fs](https://pypi.org/project/chuk-virtual-fs/)** plus a thin
**fsspec adapter** that exposes any chuk virtual filesystem as the `chuk://`
protocol — a chunked, transaction-capable, database-enforced filesystem.

```
fsspec.open() / pd.read_csv("chuk://...")
        │
        ▼
ChukFileSystem (chuk_fsspec, AsyncFileSystem)      ← works with ANY chuk provider
        │
        ▼
chuk AsyncVirtualFileSystem
        │
        ▼
PostgresStorageProvider (chuk_vfs_postgres, psycopg async)
        │
        ▼
PostgreSQL 13+ (testcontainers / docker compose)
```

## Features

| | |
|---|---|
| **Chunked storage** | Files stored in fixed-size chunks (1 MiB default, **persisted per file**) — range reads touch only the overlapping chunks; any provider instance can read any file |
| **Atomic transactions** | Provider joins an existing connection → filesystem + business rows commit or roll back together |
| **DB-enforced integrity** | Unique index `(parent_id, name)` makes duplicate siblings impossible; fsspec `xb` exclusivity is enforced by the database, not a preflight check |
| **fsspec integration** | `chuk://` protocol via `fsspec.specs` entry point: `pipe`/`cat`, buffered `open` (wb/rb/ab/xb) with seek + range reads, `mv`/`cp`/`rm`, `mkdir` |
| **Concurrency-safe init** | Schema creation serialized via `pg_advisory_xact_lock`; `initialize()` idempotent, failure-safe |
| **Atomic metadata** | JSONB merge in a single SQL expression — concurrent updates of different keys never lose data |

## Install

```bash
pip install vfs-pg
# or
uv add vfs-pg
```

Requirements: Python ≥ 3.12, PostgreSQL ≥ 13.
For development: `uv sync --extra test` (testcontainers) / `uv sync --extra docs`.

## Quickstart

```python
import asyncio

import fsspec
import chuk_vfs_postgres  # registers the "postgres" provider
from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem


async def main():
    vfs = AsyncVirtualFileSystem(
        "postgres", dsn="postgresql://vfs:vfs@localhost:5432/vfs"
    )
    await vfs.initialize()

    fs = fsspec.filesystem("chuk", vfs=vfs)     # sync fsspec API
    fs.pipe_file("/datasets/data.csv", b"a,b\n1,2\n")

    with fs.open("/datasets/data.csv", "rb") as f:
        print(f.read())

    with fsspec.open("chuk:///datasets/data.csv", "rb", vfs=vfs) as f:
        print(f.read())

    await vfs.close()


asyncio.run(main())
```

### Atomic transaction with business tables

```python
import psycopg
from chuk_vfs_postgres import PostgresStorageProvider

conn = await psycopg.AsyncConnection.connect(DSN)
async with conn.transaction():
    await conn.execute("UPDATE documents SET status='generated' WHERE id=123")
    provider = PostgresStorageProvider(conn=conn)     # joins the tx
    await provider.write_file("/documents/123/result.pdf", pdf)
    # commits together — or rolls back together
```

### Exclusive creation (fsspec `xb`)

```python
with fs.open("/locks/worker-1", "xb") as f:
    f.write(b"lease")        # FileExistsError if the path already exists
```

Of two concurrent exclusive creates, exactly one succeeds — enforced by the
unique constraint at commit time.

## Documentation

Full documentation (quickstart, architecture, API reference, testing) is
generated with Sphinx:

```bash
uv sync --extra docs
uv run --extra docs sphinx-build -b html docs docs/_build/html
open docs/_build/html/index.html
```

## Examples

```bash
uv run python examples/demo.py     # 256 MiB store/read demo with throughput
uv run python examples/stress.py   # 60 s multi-reader/writer stress test
```

`demo.py` and `stress.py` accept `VFS_PG_DSN`; `stress.py` also
`VFS_STRESS_SECONDS` / `VFS_STRESS_WRITERS` / `VFS_STRESS_READERS` /
`VFS_STRESS_MIN_MB` / `VFS_STRESS_MAX_MB`.

## Testing

The suite runs against a **throwaway PostgreSQL started by testcontainers**
(`postgres:16-alpine`) — no docker-compose needed:

```bash
uv sync --extra test
uv run pytest -v
```

Against an existing server: `VFS_PG_DSN=postgresql://user:pass@host:5432/testdb uv run pytest -v`
(truncation of non-`test` databases is refused unless `VFS_PG_ALLOW_TRUNCATE=1`).

67 tests cover the full provider API, the sync fsspec contract (including
chunk/block boundary ranges and `xb` races), and concurrency guarantees
(deadlock regression, duplicate-create races, atomicity under concurrent
reads). See `docs/testing.rst` and `tasks.md` (acceptance criteria with
regression coverage).

## Repository layout

```
src/chuk_vfs_postgres/   the PostgreSQL storage provider
src/chuk_fsspec/         the fsspec adapter (chuk://, provider-agnostic)
examples/                demo.py (256 MiB demo) + stress.py (60 s stress)
tests/                   pytest suite (testcontainers fixtures)
docs/                    Sphinx documentation
tasks.md                 improvement backlog with acceptance criteria
```

## Performance (reference, local docker-compose PostgreSQL 16)

| Operation | Throughput |
|---|---|
| Store 256 MiB (1 MiB writes) | ~115 MiB/s |
| Read 256 MiB (single-shot) | ~117 MiB/s |
| Read 256 MiB (streamed 1 MiB) | ~107 MiB/s |
| 60 s stress: 3 writers + 4 readers, 10–50 MiB files | ~23 MiB/s write, ~86 MiB/s read, 0 errors |

## License

MIT — see [LICENSE](LICENSE).
