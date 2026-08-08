"""Concurrency tests for the Postgres provider.

Includes the regression test for the concurrent-initialize deadlock that
was found by the stress test, plus parallel write/read access patterns.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import random
from typing import Any

import pytest
from chuk_virtual_fs.node_info import EnhancedNodeInfo
from psycopg import AsyncConnection

from chuk_vfs_postgres import PostgresStorageProvider


async def _mkdir(provider, path: str) -> None:
    parent = "/" if "/" not in path[1:] else path.rsplit("/", 1)[0]
    name = path.rsplit("/", 1)[-1]
    assert await provider.create_node(EnhancedNodeInfo(name=name, is_dir=True, parent_path=parent))


async def _mkfile(provider, path: str) -> None:
    parent = "/" if "/" not in path[1:] else path.rsplit("/", 1)[0]
    name = path.rsplit("/", 1)[-1]
    assert await provider.create_node(EnhancedNodeInfo(name=name, is_dir=False, parent_path=parent))


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
            await _mkfile(provider, path)  # pre-create the node
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
        final = {path: rng.randbytes(1024 * 1024 + i * 2) for i, path in enumerate(initial)}

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


async def test_range_read_uses_one_snapshot_for_metadata_and_chunks(dsn, monkeypatch):
    """An overwrite between resolution and content fetch cannot mix versions.

    The old and new versions deliberately use different chunk sizes. With
    separate metadata and chunk snapshots, range 4:8 was assembled as
    ``b"YYYY"``: new chunk 1 interpreted using the old four-byte chunk size.
    """
    old_content = b"AAAABBBBCCCC"
    new_content = b"xxxxxxYYYYYY"
    old_writer = PostgresStorageProvider(dsn=dsn, chunk_size=4)
    new_writer = PostgresStorageProvider(dsn=dsn, chunk_size=6)
    assert await old_writer.initialize()
    assert await new_writer.initialize()
    try:
        await _mkfile(old_writer, "/snapshot.bin")
        assert await old_writer.write_file("/snapshot.bin", old_content)

        resolved = asyncio.Event()
        resume = asyncio.Event()
        original_resolve = old_writer._resolve

        async def _pause_after_resolution(conn, path):
            row = await original_resolve(conn, path)
            resolved.set()
            await resume.wait()
            return row

        monkeypatch.setattr(old_writer, "_resolve", _pause_after_resolution)

        read = asyncio.create_task(old_writer.read_range("/snapshot.bin", 4, 8))
        await asyncio.wait_for(resolved.wait(), timeout=2)
        assert await new_writer.write_file("/snapshot.bin", new_content)
        resume.set()

        result = await asyncio.wait_for(read, timeout=2)
        assert result in {old_content[4:8], new_content[4:8]}
        assert result != b"YYYY"
    finally:
        await old_writer.close()
        await new_writer.close()


async def test_concurrent_duplicate_creates_single_node(dsn):
    """The (parent_id, name) unique index guarantees exactly one node even
    when many sessions create the same path at once."""
    provider = PostgresStorageProvider(dsn=dsn)
    assert await provider.initialize()
    try:
        await _mkdir(provider, "/dup")

        async def _create() -> bool:
            return await provider.create_node(
                EnhancedNodeInfo(name="x.txt", is_dir=False, parent_path="/dup")
            )

        results = await asyncio.gather(*(_create() for _ in range(10)))
        assert results.count(True) == 1
        assert await provider.list_directory("/dup") == ["x.txt"]
    finally:
        await provider.close()


async def test_concurrent_exclusive_writes_single_winner(dsn):
    """write_file_atomic(exclusive=True) races: exactly one wins, the file
    holds the winner's content, nobody sees a partial version."""
    provider = PostgresStorageProvider(dsn=dsn)
    assert await provider.initialize()
    try:
        await _mkdir(provider, "/excl")
        rng = random.Random(5)
        contents = [rng.randbytes(256 * 1024 + i) for i in range(8)]

        async def _write(i: int) -> bool:
            return await provider.write_file_atomic("/excl/race.bin", contents[i], exclusive=True)

        results = await asyncio.gather(*(_write(i) for i in range(8)))
        assert results.count(True) == 1

        data = await provider.read_file("/excl/race.bin")
        assert data in contents
        node = await provider.get_node_info("/excl/race.bin")
        assert node is not None
        assert node.sha256 == hashlib.sha256(data).hexdigest()
    finally:
        await provider.close()


async def test_concurrent_staged_appends_all_survive_once(dsn):
    provider = PostgresStorageProvider(dsn=dsn, chunk_size=7, pool_min=4, pool_max=8)
    assert await provider.initialize()
    try:
        assert await provider.write_file_atomic("/append.log", b"start|")
        suffixes = [f"part-{i:02d}|".encode() for i in range(12)]

        async def _append(suffix: bytes) -> bool:
            upload_id = await provider.start_upload("/append.log", append=True)
            # Split every suffix differently to exercise partial staged chunks.
            split = max(1, len(suffix) // 2)
            assert await provider.upload_part(upload_id, suffix[:split])
            assert await provider.upload_part(upload_id, suffix[split:])
            return await provider.finish_upload(upload_id, size=len(suffix))

        assert all(await asyncio.gather(*(_append(suffix) for suffix in suffixes)))
        result = await provider.read_file("/append.log")
        assert result is not None and result.startswith(b"start|")
        for suffix in suffixes:
            assert result.count(suffix) == 1
        assert len(result) == len(b"start|") + sum(map(len, suffixes))
        node = await provider.get_node_info("/append.log")
        assert node is not None
        assert node.sha256 == hashlib.sha256(result).hexdigest()
    finally:
        await provider.close()


async def test_concurrent_staged_exclusive_losers_are_cleaned(dsn):
    provider = PostgresStorageProvider(dsn=dsn, pool_min=4, pool_max=8)
    assert await provider.initialize()
    try:
        contents = [f"exclusive-{i}".encode() for i in range(8)]

        async def _exclusive(content: bytes) -> bool:
            upload_id = await provider.start_upload("/staged-race.bin", exclusive=True)
            assert await provider.upload_part(upload_id, content)
            return await provider.finish_upload(
                upload_id, len(content), hashlib.sha256(content).hexdigest()
            )

        results = await asyncio.gather(*(_exclusive(content) for content in contents))
        assert results.count(True) == 1
        assert await provider.read_file("/staged-race.bin") in contents
        async with provider._acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                SELECT COUNT(*) FROM vfs_uploads u
                JOIN vfs_nodes root ON root.node_id = u.root_id
                WHERE root.filesystem_id = %s
                """,
                (provider.filesystem_id,),
            )
            assert await cur.fetchone() == (0,)
            await cur.execute(
                """
                SELECT COUNT(*) FROM vfs_upload_chunks c
                JOIN vfs_uploads u ON u.upload_id = c.upload_id
                JOIN vfs_nodes root ON root.node_id = u.root_id
                WHERE root.filesystem_id = %s
                """,
                (provider.filesystem_id,),
            )
            assert await cur.fetchone() == (0,)
    finally:
        await provider.close()


async def test_staged_overwrite_readers_see_only_old_or_new(dsn):
    provider = PostgresStorageProvider(dsn=dsn, chunk_size=64 * 1024)
    assert await provider.initialize()
    try:
        old = b"o" * (3 * 64 * 1024 + 13)
        new = b"n" * (4 * 64 * 1024 + 29)
        assert await provider.write_file_atomic("/atomic-stage.bin", old)
        upload_id = await provider.start_upload("/atomic-stage.bin")
        for offset in range(0, len(new), 31 * 1024):
            assert await provider.upload_part(upload_id, new[offset : offset + 31 * 1024])
            assert await provider.read_file("/atomic-stage.bin") == old

        allowed = {hashlib.sha256(old).hexdigest(), hashlib.sha256(new).hexdigest()}

        async def _finish() -> None:
            assert await provider.finish_upload(
                upload_id, len(new), hashlib.sha256(new).hexdigest()
            )

        async def _read() -> None:
            for _ in range(30):
                data = await provider.read_file("/atomic-stage.bin")
                assert data is not None
                assert hashlib.sha256(data).hexdigest() in allowed

        await asyncio.gather(_finish(), *(_read() for _ in range(3)))
        assert await provider.read_file("/atomic-stage.bin") == new
    finally:
        await provider.close()


async def test_concurrent_nonexclusive_first_writes_both_succeed(dsn, monkeypatch):
    """A missing-file insert race remains create-or-replace for both writers."""
    provider = PostgresStorageProvider(dsn=dsn, pool_min=2, pool_max=2)
    assert await provider.initialize()
    try:
        await _mkdir(provider, "/replace")
        target = "/replace/race.bin"
        contents = [b"first" * 100_000, b"second" * 100_000]
        original_resolve = provider._resolve
        both_saw_missing = asyncio.Event()
        missing_count = 0

        async def _synchronize_missing_resolutions(conn, path):
            nonlocal missing_count
            row = await original_resolve(conn, path)
            if path == target and row is None:
                missing_count += 1
                if missing_count == 2:
                    both_saw_missing.set()
                await asyncio.wait_for(both_saw_missing.wait(), timeout=2)
            return row

        monkeypatch.setattr(provider, "_resolve", _synchronize_missing_resolutions)

        results = await asyncio.gather(
            *(provider.write_file_atomic(target, content) for content in contents)
        )
        assert results == [True, True]
        assert await provider.read_file(target) in contents
        assert await provider.list_directory("/replace") == ["race.bin"]
    finally:
        await provider.close()


async def test_concurrent_metadata_merge_disjoint_keys(dsn):
    """Concurrent metadata updates of different keys must not lose data
    (atomic JSONB merge)."""
    provider = PostgresStorageProvider(dsn=dsn)
    assert await provider.initialize()
    try:
        await _mkfile(provider, "/meta.bin")
        assert await provider.write_file("/meta.bin", b"x")

        async def _set(i: int) -> bool:
            return await provider.set_metadata("/meta.bin", {f"key_{i}": i})

        results = await asyncio.gather(*(_set(i) for i in range(10)))
        assert all(results)

        meta = await provider.get_metadata("/meta.bin")
        assert {f"key_{i}": i for i in range(10)}.items() <= meta.items()
    finally:
        await provider.close()


async def test_concurrent_cross_moves_cannot_disconnect_tree(dsn, monkeypatch):
    """Concurrent cross-moves cannot create a detached directory cycle.

    The ancestry-check barrier makes the former race deterministic: without
    move serialization both operations validate the old topology before
    either UPDATE. With serialization, the first operation times out of the
    barrier, commits, and the second re-resolves against the new topology.
    """
    provider = PostgresStorageProvider(dsn=dsn, pool_min=2, pool_max=2)
    assert await provider.initialize()
    try:
        await _mkdir(provider, "/a")
        await _mkdir(provider, "/a/d")
        await _mkdir(provider, "/b")
        await _mkdir(provider, "/b/c")

        original_is_within = provider._is_within
        both_validated = asyncio.Event()
        validation_count = 0

        async def _synchronized_is_within(
            conn: AsyncConnection, node_id: Any, ancestor_id: Any
        ) -> bool:
            nonlocal validation_count
            result = await original_is_within(conn, node_id, ancestor_id)
            validation_count += 1
            if validation_count == 2:
                both_validated.set()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(both_validated.wait(), timeout=0.2)
            return result

        monkeypatch.setattr(provider, "_is_within", _synchronized_is_within)

        start = asyncio.Event()

        async def _move(source: str, destination: str) -> bool:
            await start.wait()
            return await provider.move_node(source, destination)

        moves = [
            asyncio.create_task(_move("/a", "/b/c/a")),
            asyncio.create_task(_move("/b", "/a/d/b")),
        ]
        start.set()
        results = await asyncio.gather(*moves)

        assert sorted(results) == [False, True]
        root_entries = await provider.list_directory("/")
        assert root_entries in (["a"], ["b"])
        surviving_root = root_entries[0]
        reachable_leaf = "/a/d/b/c" if surviving_root == "a" else "/b/c/a/d"
        assert await provider.exists(reachable_leaf)

        async with provider._acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                WITH RECURSIVE reachable AS (
                    SELECT node_id FROM vfs_nodes
                     WHERE filesystem_id = %s AND parent_id IS NULL
                    UNION ALL
                    SELECT child.node_id
                      FROM vfs_nodes child
                      JOIN reachable parent ON child.parent_id = parent.node_id
                     WHERE child.filesystem_id = %s
                )
                SELECT
                    (SELECT COUNT(*) FROM reachable),
                    (SELECT COUNT(*) FROM vfs_nodes WHERE filesystem_id = %s)
                """,
                (
                    provider.filesystem_id,
                    provider.filesystem_id,
                    provider.filesystem_id,
                ),
            )
            counts = await cur.fetchone()
            assert counts is not None
            reachable, total = counts
        assert reachable == total == 5
    finally:
        await provider.close()


async def test_initialize_idempotent_same_instance(dsn):
    provider = PostgresStorageProvider(dsn=dsn)
    assert await provider.initialize()
    assert await provider.initialize()  # no-op
    try:
        assert await provider.exists("/")
    finally:
        await provider.close()


async def test_close_waits_for_initialize_and_wins_when_second(dsn, monkeypatch):
    """Lifecycle state follows lock order when initialize and close race.

    Initialization enters the lifecycle lock first and pauses during schema
    setup. close() must wait, then leave the provider consistently closed.
    """
    provider = PostgresStorageProvider(dsn=dsn)
    schema_started = asyncio.Event()
    resume_schema = asyncio.Event()
    original_ensure_schema = provider._ensure_schema

    async def _paused_ensure_schema(conn):
        schema_started.set()
        await resume_schema.wait()
        await original_ensure_schema(conn)

    monkeypatch.setattr(provider, "_ensure_schema", _paused_ensure_schema)

    initialize = asyncio.create_task(provider.initialize())
    await asyncio.wait_for(schema_started.wait(), timeout=2)
    close = asyncio.create_task(provider.close())
    await asyncio.sleep(0)
    assert not close.done()

    resume_schema.set()
    assert await asyncio.wait_for(initialize, timeout=10)
    await asyncio.wait_for(close, timeout=10)

    assert provider._initialized is False
    assert provider._pool is None
    with pytest.raises(RuntimeError, match="not initialized"):
        await provider.exists("/")
