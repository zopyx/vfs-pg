"""Concurrency tests for the Postgres provider.

Includes the regression test for the concurrent-initialize deadlock that
was found by the stress test, plus parallel write/read access patterns.
"""

from __future__ import annotations

import asyncio
import hashlib
import random

from chuk_virtual_fs.node_info import EnhancedNodeInfo

from chuk_vfs_postgres import PostgresStorageProvider


async def _mkdir(provider, path: str) -> None:
    parent = "/" if "/" not in path[1:] else path.rsplit("/", 1)[0]
    name = path.rsplit("/", 1)[-1]
    assert await provider.create_node(
        EnhancedNodeInfo(name=name, is_dir=True, parent_path=parent)
    )


async def _mkfile(provider, path: str) -> None:
    parent = "/" if "/" not in path[1:] else path.rsplit("/", 1)[0]
    name = path.rsplit("/", 1)[-1]
    assert await provider.create_node(
        EnhancedNodeInfo(name=name, is_dir=False, parent_path=parent)
    )


async def test_concurrent_initialize_no_deadlock(dsn):
    """Regression: concurrent initialize() calls used to deadlock on SCHEMA_SQL
    (RowExclusiveLock on vfs_nodes between CREATE TABLE IF NOT EXISTS and the
    root INSERT). Fixed via pg_advisory_lock serialization."""
    providers = [PostgresStorageProvider(dsn=dsn) for _ in range(6)]
    try:
        results = await asyncio.gather(*(p.initialize() for p in providers))
        assert all(results)
        # every provider is fully usable afterwards
        for p in providers:
            assert await p.exists("/")
    finally:
        await asyncio.gather(*(p.close() for p in providers))


async def test_concurrent_writes_distinct_files(dsn):
    provider = PostgresStorageProvider(dsn=dsn)
    assert await provider.initialize()
    try:
        await _mkdir(provider, "/conc")
        rng = random.Random(7)

        async def _write(i: int) -> tuple[str, bytes]:
            content = rng.randbytes(512 * 1024 + i * 4096)
            path = f"/conc/f{i}.bin"
            assert await provider.write_file(path, content)
            return path, content

        results = await asyncio.gather(*(_write(i) for i in range(8)))
        for path, content in results:
            assert await provider.read_file(path) == content
            node = await provider.get_node_info(path)
            assert node is not None
            assert node.sha256 == hashlib.sha256(content).hexdigest()
    finally:
        await provider.close()


async def test_concurrent_reads_never_see_partial_content(dsn):
    """Readers observe either the old or the new version of a file, never a
    mixture of chunks (writes are single atomic transactions)."""
    provider = PostgresStorageProvider(dsn=dsn)
    assert await provider.initialize()
    try:
        await _mkdir(provider, "/mix")
        rng = random.Random(99)
        initial = {f"/mix/f{i}.bin": rng.randbytes(1024 * 1024 + i) for i in range(4)}
        final = {
            path: rng.randbytes(1024 * 1024 + i * 2) for i, path in enumerate(initial)
        }

        for path, content in initial.items():
            await _mkfile(provider, path)
            assert await provider.write_file(path, content)

        def _sha(data: bytes) -> str:
            return hashlib.sha256(data).hexdigest()

        ok_hashes = {path: {_sha(initial[path]), _sha(final[path])} for path in initial}

        async def writer(path: str) -> None:
            assert await provider.write_file(path, final[path])

        async def reader(path: str) -> None:
            for _ in range(15):
                data = await provider.read_file(path)
                assert data is not None
                assert _sha(data) in ok_hashes[path], f"partial read of {path}"

        tasks = [writer(p) for p in initial]
        tasks += [reader(p) for p in initial for _ in range(2)]
        await asyncio.gather(*tasks)

        for path, content in final.items():
            assert await provider.read_file(path) == content
    finally:
        await provider.close()
