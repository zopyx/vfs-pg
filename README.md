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
| **Tenant namespaces** | Multiple isolated filesystems share one database via `filesystem_id`; identical paths, metadata, usage statistics, moves, deletes, and staging uploads stay independent |
| **Bounded streaming uploads** | PostgreSQL-backed fsspec writes persist each buffer block in staging tables, then publish the completed version atomically |
| **Serialized append** | Concurrent PostgreSQL appenders lock the target at publication, preserving every suffix exactly once |
| **DB-enforced integrity** | Unique index `(filesystem_id, parent_id, name)` makes duplicate siblings impossible within a namespace; fsspec `xb` exclusivity is enforced by the database, not a preflight check |
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
        "postgres",
        dsn="postgresql://vfs:vfs@localhost:5432/vfs",
        filesystem_id="customer-123",
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

`filesystem_id` defaults to `VFS_PG_FILESYSTEM_ID` and then to `"default"`.
Every path lookup and mutation, storage statistic, and staged upload is scoped
to that identifier, so two namespaces can use the same path without seeing or
changing each other's data. Existing database rows migrate into the `default`
namespace. A namespace is an isolation key, not an authorization mechanism;
applications must authenticate callers and control which identifier they may
select.

### Atomic transaction with business tables

```python
import psycopg
from chuk_vfs_postgres import PostgresStorageProvider

conn = await psycopg.AsyncConnection.connect(DSN)
provider = PostgresStorageProvider(conn=conn)
try:
    await provider.initialize()
    async with conn.transaction():
        await conn.execute("UPDATE documents SET status='generated' WHERE id=123")
        if not await provider.create_directory("/documents/123"):
            raise OSError("could not create result directory")
        if not await provider.write_file_atomic("/documents/123/result.pdf", pdf):
            raise OSError("could not write result")
        # commits together — or rolls back together
finally:
    await provider.close()
    await conn.close()
```

### Streaming writes and append ordering

`ChukBufferedFile` uses the PostgreSQL provider's staging-upload extension.
Each fsspec buffer is written to PostgreSQL as it fills; the target path remains
unchanged until `close()` publishes the completed upload in one transaction.
This keeps Python memory bounded by the configured buffers rather than total
file size. Failed context managers abort their upload, and `cleanup()` removes
staging uploads abandoned for more than 24 hours; applications should schedule
that cleanup because no background worker runs automatically.

Append uploads use the same staging path. Publication locks the target row, so
concurrent appenders are serialized and no suffix is lost. The final ordering
is the database row-lock acquisition order, not caller start or completion
order. These guarantees apply to `PostgresStorageProvider`; the adapter's
fallback for providers without staging extensions may buffer complete files.

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
uv run python examples/demo.py          # 256 MiB store/read demo with throughput
uv run python examples/stress.py        # 60 s multi-reader/writer stress test
uv run python examples/heavy_stress.py  # 5 min mixed small/large-file stress test
```

`demo.py` and `stress.py` accept `VFS_PG_DSN`; `stress.py` also
`VFS_STRESS_SECONDS` / `VFS_STRESS_WRITERS` / `VFS_STRESS_READERS` /
`VFS_STRESS_MIN_MB` / `VFS_STRESS_MAX_MB`.

`heavy_stress.py` defaults to four 1–256 KiB writers, two 16–64 MiB writers,
and eight full/range readers for 300 seconds. It reports interval and cumulative
throughput, operation counts, errors, latency percentiles, exception types, and
PostgreSQL storage statistics every ten seconds. Content is deterministic: full
reads verify SHA-256 and range reads verify the exact returned bytes. Writers
reuse a fixed set of slots to bound storage growth, and run data is removed on
exit unless `--keep-data` is supplied. Run `--help` for all CLI and environment
controls. A quick smoke profile is:

```bash
uv run python examples/heavy_stress.py --duration 10 --report-interval 2 \
  --small-writers 1 --large-writers 1 --readers 2 \
  --large-min-mib 1 --large-max-mib 2
```

## Testing

The suite runs against a **throwaway PostgreSQL started by testcontainers**
(`postgres:16-alpine`) — no docker-compose needed:

```bash
uv sync --extra test
uv run pytest -v
```

Against an existing server:
`VFS_PG_DSN=postgresql://user:pass@host:5432/database uv run pytest -v`.
Each session uses a random filesystem namespace and deletes only that namespace,
so the suite never truncates shared tables or requires a specially named database.

The test suite covers the full provider API, the sync fsspec contract (including
chunk/block boundary ranges and `xb` races), and concurrency guarantees
(deadlock regression, duplicate-create races, atomicity under concurrent
reads). The current suite contains **152 tests**. See `docs/testing.rst` and
`tasks.md` (acceptance criteria with
regression coverage).

## Repository layout

```
src/chuk_vfs_postgres/   the PostgreSQL storage provider
src/chuk_fsspec/         the fsspec adapter (chuk://, provider-agnostic)
examples/                demos plus 60 s and five-minute stress harnesses
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

## Operational limits and release status

This project is alpha software. It does not yet publish a production maximum
file size or workload envelope: practical limits depend on PostgreSQL storage,
WAL volume, vacuum pressure, overwrite churn, backup size, and restore time.
The 256 MiB demo and local throughput numbers above are reference runs, not a
large-file qualification or capacity guarantee. Benchmark those factors with
your own retention and recovery requirements before production use, and
consider object storage for content when PostgreSQL operational costs become
unacceptable.

## License

MIT — see [LICENSE](LICENSE).
