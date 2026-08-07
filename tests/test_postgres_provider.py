"""Tests for the PostgreSQL storage provider (chuk_vfs_postgres).

Covers the full chuk provider API: lifecycle, schema, nodes, content
write/read, chunk-aware range reads, move/delete semantics, metadata,
transaction joining and the chuk AsyncVirtualFileSystem integration.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
from uuid import uuid4

import psycopg
import pytest
from chuk_virtual_fs.node_info import EnhancedNodeInfo
from psycopg import sql

from chuk_vfs_postgres import PostgresStorageProvider

CHUNK = 1024 * 1024


async def _mkfile(provider, path: str) -> None:
    parent = "/" if "/" not in path[1:] else path.rsplit("/", 1)[0]
    name = path.rsplit("/", 1)[-1]
    assert await provider.create_node(EnhancedNodeInfo(name=name, is_dir=False, parent_path=parent))


async def _mkdir(provider, path: str) -> None:
    parent = "/" if "/" not in path[1:] else path.rsplit("/", 1)[0]
    name = path.rsplit("/", 1)[-1]
    assert await provider.create_node(EnhancedNodeInfo(name=name, is_dir=True, parent_path=parent))


# ----------------------------------------------------------------------
# lifecycle / schema
# ----------------------------------------------------------------------


async def test_initialize_creates_root(provider):
    assert await provider.exists("/")
    node = await provider.get_node_info("/")
    assert node is not None and node.is_dir


async def test_get_storage_stats(provider):
    stats = await provider.get_storage_stats()
    assert stats["directory_count"] >= 1
    assert stats["file_count"] == 0
    assert stats["total_size_bytes"] == 0
    assert stats["chunk_count"] == 0


def test_filesystem_id_defaults_and_validation(monkeypatch):
    monkeypatch.delenv("VFS_PG_FILESYSTEM_ID", raising=False)
    assert PostgresStorageProvider().filesystem_id == "default"

    monkeypatch.setenv("VFS_PG_FILESYSTEM_ID", "from-environment")
    assert PostgresStorageProvider().filesystem_id == "from-environment"
    assert PostgresStorageProvider(filesystem_id="explicit").filesystem_id == "explicit"

    for invalid in ("", "   ", 42):
        with pytest.raises(ValueError, match="non-empty string"):
            PostgresStorageProvider(filesystem_id=invalid)  # type: ignore[arg-type]


async def test_schema_namespace_indexes_and_default_migration(provider, dsn):
    async def definitions_and_oids():
        async with provider._acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                SELECT c.relname, c.oid, pg_get_indexdef(c.oid)
                  FROM pg_class c
                 WHERE c.oid IN (
                    to_regclass('uq_vfs_nodes_root'),
                    to_regclass('uq_vfs_nodes_sibling')
                 )
                 ORDER BY c.relname
                """
            )
            indexes = {name: (oid, definition) for name, oid, definition in await cur.fetchall()}
            await cur.execute(
                """
                SELECT column_default, is_nullable
                  FROM information_schema.columns
                 WHERE table_schema = current_schema()
                   AND table_name = 'vfs_nodes'
                   AND column_name = 'filesystem_id'
                """
            )
            column = await cur.fetchone()
        return indexes, column

    before, column = await definitions_and_oids()
    assert column == ("'default'::text", "NO")
    assert "(filesystem_id) WHERE (parent_id IS NULL)" in before["uq_vfs_nodes_root"][1]
    assert (
        "(filesystem_id, parent_id, name) WHERE (parent_id IS NOT NULL)"
        in before["uq_vfs_nodes_sibling"][1]
    )

    # A fresh provider performs schema initialization again. Correct indexes
    # keep their OIDs instead of being dropped and recreated on every call.
    again = PostgresStorageProvider(dsn=dsn)
    assert await again.initialize()
    try:
        after, _ = await definitions_and_oids()
    finally:
        await again.close()
    assert {name: value[0] for name, value in after.items()} == {
        name: value[0] for name, value in before.items()
    }


async def test_legacy_schema_rows_migrate_into_default_namespace(dsn):
    """A pre-namespace tree remains visible through the default provider."""
    schema = f"vfs_migration_{uuid4().hex}"
    conn = await psycopg.AsyncConnection.connect(dsn)
    try:
        async with conn.transaction():
            await conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            await conn.execute(
                sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema))
            )
            await conn.execute(
                """
                CREATE TABLE vfs_nodes (
                    node_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                    parent_id uuid REFERENCES vfs_nodes(node_id) ON DELETE CASCADE,
                    name text NOT NULL,
                    is_dir boolean NOT NULL DEFAULT false,
                    size bigint NOT NULL DEFAULT 0,
                    chunk_size integer NOT NULL DEFAULT 1048576,
                    mime_type text NOT NULL DEFAULT 'application/octet-stream',
                    sha256 text,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    modified_at timestamptz NOT NULL DEFAULT now(),
                    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
                );
                CREATE UNIQUE INDEX uq_vfs_nodes_root
                    ON vfs_nodes ((parent_id IS NULL)) WHERE parent_id IS NULL;
                CREATE UNIQUE INDEX uq_vfs_nodes_sibling
                    ON vfs_nodes (parent_id, name) WHERE parent_id IS NOT NULL;
                WITH root AS (
                    INSERT INTO vfs_nodes (parent_id, name, is_dir, mime_type)
                    VALUES (NULL, '', true, 'inode/directory')
                    RETURNING node_id
                )
                INSERT INTO vfs_nodes (parent_id, name, is_dir, mime_type)
                SELECT node_id, 'legacy', true, 'inode/directory' FROM root;
                """
            )

            default = PostgresStorageProvider(conn=conn, filesystem_id="default")
            tenant = PostgresStorageProvider(conn=conn, filesystem_id="new-tenant")
            assert await default.initialize()
            assert await tenant.initialize()
            try:
                assert await default.exists("/legacy")
                assert not await tenant.exists("/legacy")
                assert await tenant.exists("/")
                rows = await (
                    await conn.execute(
                        """
                        SELECT filesystem_id, COUNT(*)
                          FROM vfs_nodes
                         GROUP BY filesystem_id
                         ORDER BY filesystem_id
                        """
                    )
                ).fetchall()
                assert rows == [("default", 2), ("new-tenant", 1)]
            finally:
                await default.close()
                await tenant.close()

            # The entire migration fixture lives in its own schema. Remove
            # only that schema before committing the test transaction.
            await conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
    finally:
        await conn.close()


async def test_filesystem_namespaces_isolate_paths_stats_and_uploads(dsn):
    marker = f"namespace-{uuid4()}"
    first_id = f"{marker}-first"
    second_id = f"{marker}-second"
    first = PostgresStorageProvider(dsn=dsn, filesystem_id=first_id, chunk_size=4)
    second = PostgresStorageProvider(dsn=dsn, filesystem_id=second_id, chunk_size=4)
    assert await first.initialize()
    assert await second.initialize()
    try:
        assert await first.create_directory("/same")
        assert await second.create_directory("/same")
        assert await first.write_file_atomic("/same/data.bin", b"one")
        assert await second.write_file_atomic("/same/data.bin", b"second!")
        assert await first.set_metadata("/same/data.bin", {"tenant": "first"})
        assert await second.set_metadata("/same/data.bin", {"tenant": "second"})

        assert await first.read_file("/same/data.bin") == b"one"
        assert await second.read_file("/same/data.bin") == b"second!"
        assert await first.get_metadata("/same/data.bin") == {"tenant": "first"}
        assert await second.get_metadata("/same/data.bin") == {"tenant": "second"}
        assert (await first.get_storage_stats())["total_size_bytes"] == 3
        assert (await second.get_storage_stats())["total_size_bytes"] == 7
        assert (await first.get_storage_stats())["chunk_count"] == 1
        assert (await second.get_storage_stats())["chunk_count"] == 2

        assert await first.move_node("/same/data.bin", "/same/moved.bin")
        assert not await first.exists("/same/data.bin")
        assert await second.read_file("/same/data.bin") == b"second!"
        assert await second.delete_node("/same/data.bin")
        assert await first.read_file("/same/moved.bin") == b"one"

        first_upload = await first.start_upload("/staged.bin")
        second_upload = await second.start_upload("/staged.bin")
        assert not await second.upload_part(first_upload, b"cross-namespace")
        assert not await second.finish_upload(first_upload)
        assert not await second.abort_upload(first_upload)
        assert await first.upload_part(first_upload, b"a")
        assert await second.upload_part(second_upload, b"bb")

        async with first._acquire() as conn, first._tx(conn), conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE vfs_uploads
                   SET created_at = now() - interval '25 hours'
                 WHERE upload_id IN (%s, %s)
                """,
                (first_upload, second_upload),
            )

        assert await first.cleanup() == {
            "files_removed": 0,
            "bytes_freed": 1,
            "expired_removed": 1,
        }
        assert not await first.abort_upload(first_upload)
        assert await second.upload_part(second_upload, b"!")
        assert await second.finish_upload(second_upload, size=3)
        assert await second.read_file("/staged.bin") == b"bb!"
        assert await first.delete_node("/same/moved.bin")
        assert await second.read_file("/staged.bin") == b"bb!"
    finally:
        async with first._acquire() as conn, first._tx(conn), conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM vfs_nodes
                 WHERE filesystem_id IN (%s, %s) AND parent_id IS NULL
                """,
                (first_id, second_id),
            )
        await first.close()
        await second.close()


async def test_double_initialize_is_safe(provider):
    assert await provider.initialize() is True


async def test_double_close_is_safe(provider):
    await provider.close()
    await provider.close()  # must not raise
    with pytest.raises(RuntimeError, match="not initialized"):
        await provider.exists("/")
    # provider is usable again after re-initialize
    assert await provider.initialize() is True
    assert await provider.exists("/")


async def test_cleanup_is_noop(provider):
    assert await provider.cleanup() == {
        "files_removed": 0,
        "bytes_freed": 0,
        "expired_removed": 0,
    }


async def test_uninitialized_provider_raises(dsn):
    p = PostgresStorageProvider(dsn=dsn)
    with pytest.raises(RuntimeError, match="not initialized"):
        await p.read_file("/x")
    await p.close()


async def test_pooled_provider_operations_fail_after_close(dsn):
    p = PostgresStorageProvider(dsn=dsn)
    assert await p.initialize()
    await p.close()

    with pytest.raises(RuntimeError, match="not initialized"):
        await p.exists("/")
    with pytest.raises(RuntimeError, match="not initialized"):
        await p.cleanup()
    with pytest.raises(RuntimeError, match="not initialized"):
        await p.create_directory("/")


async def test_external_provider_close_requires_reinitialize(external_conn):
    joined = PostgresStorageProvider(conn=external_conn)
    assert await joined.initialize()
    assert await joined.exists("/")

    await joined.close()
    assert not external_conn.closed
    with pytest.raises(RuntimeError, match="not initialized"):
        await joined.exists("/")

    # close() never takes ownership of the injected connection, and the same
    # provider can explicitly rejoin it afterwards.
    assert await (await external_conn.execute("SELECT 1")).fetchone() == (1,)
    assert await joined.initialize()
    assert await joined.exists("/")


# ----------------------------------------------------------------------
# nodes
# ----------------------------------------------------------------------


async def test_create_node_requires_existing_parent(provider):
    assert (
        await provider.create_node(EnhancedNodeInfo(name="x", is_dir=True, parent_path="/nope"))
        is False
    )
    # parent must be a directory
    await _mkfile(provider, "/f.txt")
    assert (
        await provider.create_node(EnhancedNodeInfo(name="x", is_dir=True, parent_path="/f.txt"))
        is False
    )


async def test_duplicate_node_rejected(provider):
    await _mkdir(provider, "/a")
    assert (
        await provider.create_node(EnhancedNodeInfo(name="a", is_dir=True, parent_path="/"))
        is False
    )


async def test_root_node_ops_rejected(provider):
    assert (
        await provider.create_node(EnhancedNodeInfo(name="", is_dir=True, parent_path="/")) is False
    )
    assert await provider.delete_node("/") is False
    assert await provider.move_node("/", "/x") is False
    assert await provider.move_node("/x", "/") is False


async def test_path_trailing_slash_normalized(provider):
    await _mkfile(provider, "/f.bin")
    assert await provider.write_file("/f.bin/", b"x")  # "/f.bin/" -> "/f.bin"
    assert await provider.read_file("/f.bin//") == b"x"
    assert await provider.exists("/f.bin/")


async def test_relative_provider_path_is_rooted_without_dropping_first_component(provider):
    await _mkdir(provider, "/foo")
    await _mkfile(provider, "/foo/bar.txt")

    assert await provider.write_file("foo/bar.txt", b"correct node")
    assert await provider.read_file("foo/bar.txt") == b"correct node"
    assert await provider.read_file("/foo/bar.txt") == b"correct node"
    assert not await provider.exists("/bar.txt")


async def test_repeated_separators_and_dot_components_are_canonical(provider):
    await _mkdir(provider, "/canonical")
    await _mkfile(provider, "/canonical/file.txt")

    assert await provider.write_file("//canonical///./file.txt/", b"same file")
    assert await provider.read_file("canonical/./file.txt") == b"same file"
    node = await provider.get_node_info("///canonical//file.txt")
    assert node is not None
    assert node.parent_path == "/canonical"
    assert node.name == "file.txt"


async def test_list_directory_sorted(provider):
    await _mkdir(provider, "/b")
    await _mkdir(provider, "/a")
    await _mkfile(provider, "/f.txt")
    assert await provider.list_directory("/") == ["a", "b", "f.txt"]
    # missing / non-dir
    assert await provider.list_directory("/a") == []
    assert await provider.list_directory("/f.txt") == []
    assert await provider.list_directory("/missing") == []


async def test_get_node_info_fields(provider):
    await _mkfile(provider, "/hello.txt")
    await provider.write_file("/hello.txt", b"abc")
    node = await provider.get_node_info("/hello.txt")
    assert node is not None
    assert node.name == "hello.txt"
    assert node.parent_path == "/"
    assert node.size == 3
    assert node.sha256 == hashlib.sha256(b"abc").hexdigest()
    assert node.provider == "postgres"
    assert node.is_dir is False
    assert node.permissions == "644"
    assert node.mime_type == "application/octet-stream"
    assert await provider.get_node_info("/missing") is None


async def test_directory_mime_type(provider):
    await _mkdir(provider, "/d")
    node = await provider.get_node_info("/d")
    assert node is not None
    assert node.is_dir and node.mime_type == "inode/directory"
    assert node.permissions == "755"


# ----------------------------------------------------------------------
# content: write / read / chunking / ranges
# ----------------------------------------------------------------------


async def test_write_read_roundtrip(provider):
    await _mkfile(provider, "/hello.txt")
    content = b"Hello, PostgreSQL VFS!"
    assert await provider.write_file("/hello.txt", content)
    assert await provider.read_file("/hello.txt") == content


async def test_write_empty_file(provider):
    await _mkfile(provider, "/empty.bin")
    assert await provider.write_file("/empty.bin", b"")
    assert await provider.read_file("/empty.bin") == b""
    assert await provider.read_range("/empty.bin", 0, 10) == b""
    node = await provider.get_node_info("/empty.bin")
    assert node.size == 0
    assert node.sha256 == hashlib.sha256(b"").hexdigest()


async def test_write_str_and_unicode(provider):
    await _mkfile(provider, "/u.txt")
    text = "héllo wörld ☃ — Grüße"
    assert await provider.write_file("/u.txt", text)
    assert await provider.read_file("/u.txt") == text.encode("utf-8")


async def test_overwrite_replaces_content(provider):
    await _mkfile(provider, "/f.bin")
    await provider.write_file("/f.bin", b"short")
    await provider.write_file("/f.bin", b"a" * (2 * CHUNK + 7))
    assert await provider.read_file("/f.bin") == b"a" * (2 * CHUNK + 7)
    node = await provider.get_node_info("/f.bin")
    assert node.size == 2 * CHUNK + 7
    # old chunks are gone, new chunk count is exact
    stats = await provider.get_storage_stats()
    assert stats["chunk_count"] == 3


async def test_multichunk_write_read(provider):
    rng = random.Random(42)
    content = bytes(rng.getrandbits(8) for _ in range(2 * CHUNK + CHUNK // 2))
    await _mkfile(provider, "/big.bin")
    assert await provider.write_file("/big.bin", content)
    assert await provider.read_file("/big.bin") == content
    stats = await provider.get_storage_stats()
    assert stats["file_count"] == 1
    assert stats["chunk_count"] == 3
    assert stats["total_size_bytes"] == len(content)


async def test_write_rejects_directory_and_missing(provider):
    await _mkdir(provider, "/d")
    assert await provider.write_file("/d", b"x") is False
    assert await provider.write_file("/missing", b"x") is False
    assert await provider.read_file("/missing") is None
    assert await provider.read_file("/d") is None


async def test_read_range_across_chunk_boundary(provider):
    cs = provider.chunk_size
    content = bytes(range(256)) * (cs // 256) * 2  # exactly 2 MiB, deterministic
    await _mkfile(provider, "/range.bin")
    await provider.write_file("/range.bin", content)

    assert await provider.read_range("/range.bin", 100, 200) == content[100:200]
    assert await provider.read_range("/range.bin", cs - 10, cs + 10) == content[cs - 10 : cs + 10]
    assert (
        await provider.read_range("/range.bin", cs + 5, cs + 5 + 100) == content[cs + 5 : cs + 105]
    )
    # beyond EOF clamps
    assert (
        await provider.read_range("/range.bin", len(content) - 5, len(content) + 100)
        == content[-5:]
    )
    assert await provider.read_range("/range.bin", len(content) + 10, None) == b""
    # empty / invalid windows
    assert await provider.read_range("/range.bin", 0, 0) == b""
    assert await provider.read_range("/range.bin", 50, 10) == b""


async def test_read_range_exact_chunk_boundary(provider):
    cs = provider.chunk_size
    content = bytes(range(256)) * (cs // 256) * 2
    await _mkfile(provider, "/edge.bin")
    await provider.write_file("/edge.bin", content)
    # window starting exactly on a chunk boundary
    assert await provider.read_range("/edge.bin", cs, cs + 5) == content[cs : cs + 5]
    # window ending exactly on a boundary
    assert await provider.read_range("/edge.bin", cs - 5, cs) == content[cs - 5 : cs]


async def test_read_range_negative_start_clamps(provider):
    await _mkfile(provider, "/neg.bin")
    await provider.write_file("/neg.bin", b"0123456789")
    assert await provider.read_range("/neg.bin", -100, 5) == b"01234"
    assert await provider.read_range("/neg.bin", -100, None) == b"0123456789"


async def test_read_range_missing_and_dir(provider):
    await _mkdir(provider, "/d")
    assert await provider.read_range("/missing", 0, 10) is None
    assert await provider.read_range("/d", 0, 10) is None


async def test_custom_chunk_size_provider(dsn):
    p = PostgresStorageProvider(dsn=dsn, chunk_size=64 * 1024)
    assert await p.initialize()
    try:
        await _mkfile(p, "/c.bin")
        content = bytes(range(256)) * 600  # 153600 B = 2.34 x 64 KiB
        assert await p.write_file("/c.bin", content)
        assert await p.read_file("/c.bin") == content
        stats = await p.get_storage_stats()
        assert stats["chunk_count"] == 3
        cs = 64 * 1024
        assert await p.read_range("/c.bin", cs - 5, cs + 5) == content[cs - 5 : cs + 5]
    finally:
        await p.close()


async def test_invalid_chunk_size_rejected(dsn):
    with pytest.raises(ValueError, match="positive integer"):
        PostgresStorageProvider(dsn=dsn, chunk_size=0)
    with pytest.raises(ValueError, match="positive integer"):
        PostgresStorageProvider(dsn=dsn, chunk_size=-5)
    with pytest.raises(ValueError, match="positive integer"):
        PostgresStorageProvider(dsn=dsn, chunk_size="big")  # type: ignore[arg-type]


async def test_chunk_size_is_per_file_self_describing(dsn):
    """A file written with 64 KiB chunks is range-read correctly by a
    provider configured with the default 1 MiB chunk size."""
    writer = PostgresStorageProvider(dsn=dsn, chunk_size=64 * 1024)
    reader = PostgresStorageProvider(dsn=dsn)
    assert await writer.initialize()
    assert await reader.initialize()
    try:
        await _mkfile(writer, "/mixed.bin")
        content = bytes(range(256)) * 800  # 204800 B = 3.125 x 64 KiB
        assert await writer.write_file("/mixed.bin", content)

        assert await reader.read_file("/mixed.bin") == content
        cs64 = 64 * 1024
        for start, end in [
            (cs64 - 5, cs64 + 5),  # writer chunk boundary
            (2 * cs64, 2 * cs64 + 100),  # exact boundary start
            (100_000, 150_000),  # inside writer chunk 1
            (len(content) - 10, len(content) + 50),
        ]:
            assert await reader.read_range("/mixed.bin", start, end) == content[start:end]
    finally:
        await writer.close()
        await reader.close()


async def test_randomized_ranges_match_read_file(provider):
    """Property-style check: read_range windows agree with read_file slices."""
    rng = random.Random(20260807)
    for i in range(15):
        size = rng.randrange(0, 5 * CHUNK)  # 0..5 MiB, crosses chunk boundaries
        content = rng.randbytes(size)
        path = f"/rand_{i}.bin"
        await _mkfile(provider, path)
        assert await provider.write_file(path, content)
        assert await provider.read_file(path) == content
        for _ in range(5):
            start = rng.randrange(0, max(1, size))
            length = rng.randrange(1, min(200_000, size - start + 1))
            assert (
                await provider.read_range(path, start, start + length)
                == content[start : start + length]
            )


# ----------------------------------------------------------------------
# move / delete
# ----------------------------------------------------------------------


async def test_move_file_and_rename_dir(provider):
    await _mkdir(provider, "/a")
    await _mkdir(provider, "/b")
    await _mkfile(provider, "/a/f.txt")
    await provider.write_file("/a/f.txt", b"data")

    assert await provider.move_node("/a/f.txt", "/b/f.txt")
    assert not await provider.exists("/a/f.txt")
    assert await provider.read_file("/b/f.txt") == b"data"

    # renaming a directory keeps children (single UPDATE, no copy)
    assert await provider.move_node("/b", "/b2")
    assert await provider.read_file("/b2/f.txt") == b"data"
    assert (await provider.get_node_info("/b2")).is_dir


async def test_move_preserves_content_and_metadata(provider):
    await _mkdir(provider, "/a")
    await _mkfile(provider, "/a/s.txt")
    await provider.write_file("/a/s.txt", b"payload")
    await provider.set_metadata("/a/s.txt", {"owner": "ajung"})
    assert await provider.move_node("/a/s.txt", "/s.txt")
    node = await provider.get_node_info("/s.txt")
    assert node.sha256 == hashlib.sha256(b"payload").hexdigest()
    assert await provider.get_metadata("/s.txt") == {"owner": "ajung"}


async def test_move_protects_destination(provider):
    await _mkdir(provider, "/a")
    await _mkfile(provider, "/a/s.txt")
    await _mkfile(provider, "/a/d.txt")
    await provider.write_file("/a/s.txt", b"s")
    await provider.write_file("/a/d.txt", b"d")
    assert await provider.move_node("/a/s.txt", "/a/d.txt") is False
    assert await provider.read_file("/a/d.txt") == b"d"
    # missing source
    assert await provider.move_node("/a/nope", "/a/d2.txt") is False
    # missing destination parent
    assert await provider.move_node("/a/s.txt", "/nope/s.txt") is False


async def test_move_onto_itself_is_idempotent(provider):
    await _mkdir(provider, "/a")
    await _mkfile(provider, "/a/f.txt")
    await provider.write_file("/a/f.txt", b"x")
    assert await provider.move_node("/a/f.txt", "/a/f.txt") is True
    assert await provider.read_file("/a/f.txt") == b"x"
    assert await provider.move_node("/a", "/a") is True


async def test_move_directory_into_own_subtree_rejected(provider):
    await _mkdir(provider, "/a")
    await _mkdir(provider, "/a/child")
    await _mkfile(provider, "/a/child/f.txt")
    await provider.write_file("/a/child/f.txt", b"x")
    # /a -> /a/child/moved would put /a inside itself
    assert await provider.move_node("/a", "/a/child/moved") is False
    assert await provider.move_node("/a", "/a/child") is False
    # tree unchanged
    assert await provider.read_file("/a/child/f.txt") == b"x"
    assert (await provider.get_node_info("/a")).is_dir


async def test_move_destination_race_keeps_joined_transaction_usable(
    provider, external_conn, monkeypatch
):
    """A destination created after move validation rolls back only its savepoint."""
    await _mkfile(provider, "/source.txt")
    await _mkdir(provider, "/destination")

    joined = PostgresStorageProvider(conn=external_conn)
    assert await joined.initialize()
    original_child = joined._child
    destination_checked = asyncio.Event()
    resume_move = asyncio.Event()

    async def _pause_after_destination_check(conn, parent_id, name):
        row = await original_child(conn, parent_id, name)
        if name == "target.txt" and row is None:
            destination_checked.set()
            await asyncio.wait_for(resume_move.wait(), timeout=2)
        return row

    monkeypatch.setattr(joined, "_child", _pause_after_destination_check)

    async with external_conn.transaction():
        move = asyncio.create_task(joined.move_node("/source.txt", "/destination/target.txt"))
        await asyncio.wait_for(destination_checked.wait(), timeout=2)
        assert await provider.create_node(
            EnhancedNodeInfo(name="target.txt", is_dir=False, parent_path="/destination")
        )
        resume_move.set()

        assert await move is False
        probe = await (await external_conn.execute("SELECT 1")).fetchone()
        assert probe == (1,)

    assert await provider.exists("/source.txt")
    assert await provider.exists("/destination/target.txt")


async def test_delete_semantics_and_cascade(provider):
    await _mkdir(provider, "/d")
    await _mkfile(provider, "/d/f.bin")
    await provider.write_file("/d/f.bin", b"x" * (2 * CHUNK))

    # non-empty directory cannot be deleted
    assert await provider.delete_node("/d") is False
    assert await provider.delete_node("/d/f.bin") is True
    assert await provider.delete_node("/d") is True
    assert not await provider.exists("/d")
    assert await provider.delete_node("/d") is False  # already gone

    # chunk rows cascade with the node
    stats = await provider.get_storage_stats()
    assert stats["chunk_count"] == 0
    assert stats["file_count"] == 0


# ----------------------------------------------------------------------
# metadata
# ----------------------------------------------------------------------


async def test_metadata_roundtrip_and_merge(provider):
    await _mkfile(provider, "/m.txt")
    assert await provider.set_metadata("/m.txt", {"owner": "ajung", "project": "vfs"})
    assert await provider.get_metadata("/m.txt") == {
        "owner": "ajung",
        "project": "vfs",
    }
    # partial update merges
    assert await provider.set_metadata("/m.txt", {"stage": "prototype"})
    assert await provider.get_metadata("/m.txt") == {
        "owner": "ajung",
        "project": "vfs",
        "stage": "prototype",
    }
    assert await provider.get_metadata("/missing") == {}
    assert await provider.set_metadata("/missing", {"a": 1}) is False


async def test_metadata_json_types(provider):
    await _mkfile(provider, "/j.txt")
    meta = {"tags": ["a", "b"], "count": 3, "flag": True, "nested": {"k": "v"}}
    assert await provider.set_metadata("/j.txt", meta)
    assert await provider.get_metadata("/j.txt") == meta


# ----------------------------------------------------------------------
# transaction join (the killer feature)
# ----------------------------------------------------------------------


async def test_atomic_transaction_with_business_tables(provider, external_conn):
    # deterministic start: business_docs is outside the VFS clean-up scope
    await external_conn.execute("DROP TABLE IF EXISTS business_docs")
    await external_conn.execute("CREATE TABLE business_docs (id serial PRIMARY KEY, name text)")
    await external_conn.commit()  # leave IDLE so transaction() is a real tx

    try:
        # rollback path: aborting the transaction undoes VFS write + business row
        with pytest.raises(RuntimeError):
            async with external_conn.transaction():
                joined = PostgresStorageProvider(conn=external_conn)
                await joined.initialize()
                assert await joined.create_node(
                    EnhancedNodeInfo(name="tx.pdf", is_dir=False, parent_path="/")
                )
                assert await joined.write_file("/tx.pdf", b"tx-content")
                await external_conn.execute("INSERT INTO business_docs (name) VALUES ('tx.pdf')")
                raise RuntimeError("abort")  # trigger rollback
        assert not await provider.exists("/tx.pdf")
        rows = await (await external_conn.execute("SELECT COUNT(*) FROM business_docs")).fetchone()
        assert rows[0] == 0
        await external_conn.commit()  # clear implicit tx -> next transaction() is real

        # commit path: both persist
        async with external_conn.transaction():
            joined2 = PostgresStorageProvider(conn=external_conn)
            await joined2.initialize()
            assert await joined2.create_node(
                EnhancedNodeInfo(name="tx.pdf", is_dir=False, parent_path="/")
            )
            assert await joined2.write_file("/tx.pdf", b"tx-content")
            await external_conn.execute("INSERT INTO business_docs (name) VALUES ('tx.pdf')")
        assert await provider.read_file("/tx.pdf") == b"tx-content"
        rows = await (await external_conn.execute("SELECT COUNT(*) FROM business_docs")).fetchone()
        assert rows[0] == 1
    finally:
        await external_conn.execute("DROP TABLE IF EXISTS business_docs")
        await external_conn.commit()  # cleanup must persist, not roll back


async def test_duplicate_create_keeps_joined_transaction_usable(provider, external_conn):
    """Expected VFS conflicts must not abort a caller-owned transaction."""
    await external_conn.execute("DROP TABLE IF EXISTS conflict_business_docs")
    await external_conn.execute(
        "CREATE TABLE conflict_business_docs (id serial PRIMARY KEY, name text)"
    )
    await external_conn.commit()

    node = EnhancedNodeInfo(name="duplicate.txt", is_dir=False, parent_path="/")
    try:
        async with external_conn.transaction():
            joined = PostgresStorageProvider(conn=external_conn)
            await joined.initialize()
            assert await joined.create_node(node) is True
            assert await joined.create_node(node) is False

            probe = await (await external_conn.execute("SELECT 1")).fetchone()
            assert probe == (1,)
            await external_conn.execute(
                "INSERT INTO conflict_business_docs (name) VALUES ('committed')"
            )

        count = await (
            await external_conn.execute("SELECT COUNT(*) FROM conflict_business_docs")
        ).fetchone()
        assert count == (1,)
        assert await provider.exists("/duplicate.txt")
    finally:
        await external_conn.execute("DROP TABLE IF EXISTS conflict_business_docs")
        await external_conn.commit()


# ----------------------------------------------------------------------
# chuk AsyncVirtualFileSystem integration
# ----------------------------------------------------------------------


async def test_vfs_high_level_api(vfs):
    assert await vfs.mkdir("/projects")
    assert await vfs.mkdir("/projects/test")
    assert await vfs.write_file("/projects/test/hello.txt", "Hello")
    assert await vfs.read_text("/projects/test/hello.txt") == "Hello"
    assert await vfs.ls("/projects/test") == ["hello.txt"]
    assert await vfs.is_file("/projects/test/hello.txt")
    assert await vfs.is_dir("/projects")
    assert await vfs.mv("/projects/test/hello.txt", "/projects/hello.txt")
    assert not await vfs.exists("/projects/test/hello.txt")
    assert await vfs.cp("/projects/hello.txt", "/projects/test/hello.txt")
    assert await vfs.rm("/projects/test/hello.txt")
    assert await vfs.rmdir("/projects/test")
    # touch creates empty file
    assert await vfs.touch("/empty.txt")
    assert await vfs.read_binary("/empty.txt") == b""


async def test_vfs_metadata_via_set_metadata(vfs):
    await vfs.touch("/meta.txt")
    assert await vfs.set_metadata("/meta.txt", {"kind": "test"})
    assert await vfs.get_metadata("/meta.txt") == {"kind": "test"}


# ----------------------------------------------------------------------
# create_directory (provider-level, not the VFS-level mkdir)
# ----------------------------------------------------------------------


async def test_create_directory_root_is_noop(provider):
    assert await provider.create_directory("/")


async def test_create_directory_single_level(provider):
    assert await provider.create_directory("/d1")
    node = await provider.get_node_info("/d1")
    assert node is not None and node.is_dir
    assert node.mime_type == "inode/directory"


async def test_create_directory_with_parents(provider):
    assert await provider.create_directory("/a/b/c/d")
    for p in ["/a/b/c/d", "/a/b/c", "/a/b", "/a"]:
        node = await provider.get_node_info(p)
        assert node is not None and node.is_dir


async def test_create_directory_idempotent(provider):
    assert await provider.create_directory("/deep/nested")
    assert await provider.create_directory("/deep/nested")
    node = await provider.get_node_info("/deep/nested")
    assert node is not None and node.is_dir


async def test_create_directory_over_existing_file_fails(provider):
    await _mkfile(provider, "/f.txt")
    assert await provider.create_directory("/f.txt/sub") is False


async def test_create_directory_existing_path_is_file(provider):
    await _mkfile(provider, "/f.txt")
    assert await provider.create_directory("/f.txt") is False


async def test_create_directory_missing_parent_root(provider):
    """Impossible: root is always present, but cover the _resolve(None) branch."""
    pass  # root always exists because schema creates it


# ----------------------------------------------------------------------
# write_file_atomic edge cases
# ----------------------------------------------------------------------


async def test_write_file_atomic_str_content(provider):
    assert await provider.write_file_atomic("/str.txt", "héllo")
    assert await provider.read_file("/str.txt") == "héllo".encode()


async def test_write_file_atomic_on_existing_directory(provider):
    await _mkdir(provider, "/d")
    assert await provider.write_file_atomic("/d", b"x") is False


async def test_write_file_atomic_missing_parent(provider):
    assert await provider.write_file_atomic("/nope/x.txt", b"x") is False


async def test_write_file_atomic_exclusive_existing(provider):
    await _mkfile(provider, "/e.txt")
    await provider.write_file("/e.txt", b"old")
    assert await provider.write_file_atomic("/e.txt", b"new", exclusive=True) is False
    assert await provider.read_file("/e.txt") == b"old"


async def test_write_file_atomic_creates_missing_node(provider):
    assert await provider.write_file_atomic("/created.txt", b"hello")
    assert await provider.read_file("/created.txt") == b"hello"
    node = await provider.get_node_info("/created.txt")
    assert node is not None and not node.is_dir


async def test_write_file_atomic_overwrite(provider):
    await _mkfile(provider, "/ow.txt")
    await provider.write_file("/ow.txt", b"v1")
    assert await provider.write_file_atomic("/ow.txt", b"v2")
    assert await provider.read_file("/ow.txt") == b"v2"


# ----------------------------------------------------------------------
# staged streaming uploads
# ----------------------------------------------------------------------


async def test_staged_upload_parts_cross_chunk_boundaries(dsn):
    p = PostgresStorageProvider(dsn=dsn, chunk_size=4)
    assert await p.initialize()
    try:
        await _mkfile(p, "/staged.bin")
        assert await p.write_file("/staged.bin", b"old")

        upload_id = await p.start_upload("/staged.bin")
        assert await p.upload_part(upload_id, b"abc")
        assert await p.upload_part(upload_id, b"defgh")

        # Parts are durable but the old target remains the only visible data.
        assert await p.read_file("/staged.bin") == b"old"
        async with p._acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                SELECT c.chunk_no, c.data FROM vfs_upload_chunks c
                JOIN vfs_uploads u ON u.upload_id = c.upload_id
                JOIN vfs_nodes root ON root.node_id = u.root_id
                 WHERE c.upload_id = %s AND root.filesystem_id = %s
                 ORDER BY c.chunk_no
                """,
                (upload_id, p.filesystem_id),
            )
            assert await cur.fetchall() == [(0, b"abcd"), (1, b"efgh")]

        content = b"abcdefgh"
        assert await p.finish_upload(upload_id, len(content), hashlib.sha256(content).hexdigest())
        assert await p.read_file("/staged.bin") == content
        async with p._acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                SELECT COUNT(*) FROM vfs_uploads u
                JOIN vfs_nodes root ON root.node_id = u.root_id
                WHERE root.filesystem_id = %s
                """,
                (p.filesystem_id,),
            )
            assert await cur.fetchone() == (0,)
            await cur.execute(
                """
                SELECT COUNT(*) FROM vfs_upload_chunks c
                JOIN vfs_uploads u ON u.upload_id = c.upload_id
                JOIN vfs_nodes root ON root.node_id = u.root_id
                WHERE root.filesystem_id = %s
                """,
                (p.filesystem_id,),
            )
            assert await cur.fetchone() == (0,)
    finally:
        await p.close()


async def test_abort_and_failed_finish_leave_target_unchanged(provider):
    await _mkfile(provider, "/keep.bin")
    assert await provider.write_file("/keep.bin", b"old")

    aborted = await provider.start_upload("/new.bin")
    assert await provider.upload_part(aborted, b"never visible")
    assert not await provider.exists("/new.bin")
    assert await provider.abort_upload(aborted)

    failed = await provider.start_upload("/keep.bin")
    assert await provider.upload_part(failed, b"new")
    assert not await provider.finish_upload(failed, size=99)
    assert await provider.read_file("/keep.bin") == b"old"

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


async def test_empty_staged_upload_creates_empty_file(provider):
    upload_id = await provider.start_upload("/empty-staged.bin")
    assert await provider.finish_upload(upload_id, size=0, sha256=hashlib.sha256(b"").hexdigest())
    assert await provider.read_file("/empty-staged.bin") == b""


async def test_append_rechunks_across_different_provider_chunk_sizes(dsn):
    writer = PostgresStorageProvider(dsn=dsn, chunk_size=4)
    appender = PostgresStorageProvider(dsn=dsn, chunk_size=7)
    assert await writer.initialize()
    assert await appender.initialize()
    try:
        assert await writer.write_file_atomic("/mixed-append.bin", b"abcdef")
        suffix = b"ghijklmnop"
        upload_id = await appender.start_upload("/mixed-append.bin", append=True)
        assert await appender.upload_part(upload_id, suffix[:3])
        assert await appender.upload_part(upload_id, suffix[3:])
        assert await appender.finish_upload(upload_id, size=len(suffix))
        expected = b"abcdef" + suffix
        assert await writer.read_file("/mixed-append.bin") == expected
        node = await writer.get_node_info("/mixed-append.bin")
        assert node is not None
        assert node.sha256 == hashlib.sha256(expected).hexdigest()

        missing = await appender.start_upload("/new-append.bin", append=True)
        assert await appender.upload_part(missing, b"created by append")
        assert await appender.finish_upload(missing, size=len(b"created by append"))
        assert await appender.read_file("/new-append.bin") == b"created by append"
    finally:
        await writer.close()
        await appender.close()


async def test_failed_upload_part_aborts_session(provider):
    upload_id = await provider.start_upload("/invalid-part.bin")
    with pytest.raises(TypeError, match="bytes-like"):
        await provider.upload_part(upload_id, "not bytes")  # type: ignore[arg-type]
    assert not await provider.exists("/invalid-part.bin")
    assert not await provider.abort_upload(upload_id)


async def test_cleanup_removes_only_stale_staging_uploads(provider):
    stale = await provider.start_upload("/stale.bin")
    fresh = await provider.start_upload("/fresh.bin")
    assert await provider.upload_part(stale, b"stale bytes")
    assert await provider.upload_part(fresh, b"fresh")
    async with provider._acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE vfs_uploads u
               SET created_at = now() - interval '25 hours'
              FROM vfs_nodes root
             WHERE u.upload_id = %s AND root.node_id = u.root_id
               AND root.filesystem_id = %s
            """,
            (stale, provider.filesystem_id),
        )

    assert await provider.cleanup() == {
        "files_removed": 0,
        "bytes_freed": len(b"stale bytes"),
        "expired_removed": 1,
    }
    assert not await provider.abort_upload(stale)
    assert await provider.abort_upload(fresh)


# ----------------------------------------------------------------------
# default DSN / external_connection property
# ----------------------------------------------------------------------


def test_default_dsn():
    p = PostgresStorageProvider()
    assert p.dsn is not None
    assert "postgresql://" in p.dsn
    assert p.external_connection is None
    assert p._external_conn is None


async def test_provider_with_external_conn_has_external_connection(external_conn):
    joined = PostgresStorageProvider(conn=external_conn)
    assert joined.external_connection is external_conn
    assert joined._external_conn is external_conn


# ----------------------------------------------------------------------
# internal helpers
# ----------------------------------------------------------------------


def test_split_root_path():
    p = PostgresStorageProvider()
    assert p._split("/") == ("/", "")
    assert p._split("/foo/bar") == ("/foo", "bar")
    assert p._split("foo") == ("/", "foo")
    assert p._normalize("/foo/") == "/foo"
    assert p._normalize("foo//./bar") == "/foo/bar"
    assert p._normalize("///foo///bar//") == "/foo/bar"
    assert p._normalize("") == "/"
    assert p._normalize("/") == "/"


@pytest.mark.parametrize("path", ["..", "/../escape", "/safe/../escape", "safe/.."])
def test_provider_path_rejects_parent_components(path):
    with pytest.raises(ValueError, match=r"'\.\.' components"):
        PostgresStorageProvider._normalize(path)


def test_provider_path_rejects_nul_and_non_string_values():
    with pytest.raises(ValueError, match="NUL"):
        PostgresStorageProvider._normalize("/bad\x00path")
    with pytest.raises(TypeError, match="path must be a string"):
        PostgresStorageProvider._normalize(None)  # type: ignore[arg-type]


# ----------------------------------------------------------------------
# internal: _resolve root-none, _init_lock double-check
# ----------------------------------------------------------------------


async def test_initialize_concurrent_same_instance_hits_double_check(dsn):
    """Two concurrent initialize() calls on the *same* instance: the second
    waits on the lock and hits the double-check (line 149)."""
    provider = PostgresStorageProvider(dsn=dsn)
    try:
        results = await asyncio.gather(provider.initialize(), provider.initialize())
        assert results == [True, True]
        assert provider._initialized
    finally:
        await provider.close()


async def test_resolve_returns_none_when_root_missing(provider):
    """_resolve returns None when _root_row returns None (line 306)."""
    orig = provider._root_row

    async def _no_root(conn):
        return None

    provider._root_row = _no_root
    try:
        async with provider._acquire() as conn:
            assert await provider._resolve(conn, "/some/path") is None
    finally:
        provider._root_row = orig


async def test_create_directory_returns_false_when_parent_missing(provider):
    """create_directory returns False when a parent in the chain is missing
    (line 704 — parent_row is None). This can't happen with a functioning
    root, so we mock _resolve to simulate it."""
    orig = provider._resolve

    async def _mock_resolve(conn, path):
        if path == "/":
            return await orig(conn, path)
        return None  # any non-root path fails

    provider._resolve = _mock_resolve
    try:
        assert await provider.create_directory("/missing/parent") is False
    finally:
        provider._resolve = orig


# ----------------------------------------------------------------------
# error paths: pool open failure, duplicate siblings, loop-mismatch close
# ----------------------------------------------------------------------


async def test_initialize_failure_cleans_up_pool():
    """Pool open failure triggers cleanup path (lines 165-170)."""
    p = PostgresStorageProvider(
        dsn="postgresql://invalid@127.0.0.1:1/nonexistent?connect_timeout=1"
    )
    try:
        with pytest.raises(psycopg.OperationalError):
            await p.initialize()
        assert p._pool is None
        assert p._initialized is False
    finally:
        await p.close()


async def test_acquire_rejects_inconsistent_initialized_state():
    provider = PostgresStorageProvider()
    provider._initialized = True
    with pytest.raises(RuntimeError, match="not initialized"):
        async with provider._acquire():
            pass


async def test_schema_guard_reports_duplicate_siblings():
    class DuplicateCursor:
        async def execute(self, query, params=None):
            return None

        async def fetchall(self):
            return [("tenant", "parent", "name", 2)]

    provider = PostgresStorageProvider()
    with pytest.raises(RuntimeError, match=r"tenant:parent/name \(x2\)"):
        await provider._ensure_schema_locked(DuplicateCursor())


def test_upload_id_validation_rejects_non_uuid_values():
    with pytest.raises(TypeError, match="UUID or UUID string"):
        PostgresStorageProvider._coerce_upload_id(42)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="badly formed hexadecimal UUID string"):
        PostgresStorageProvider._coerce_upload_id("not-a-uuid")


async def test_chunk_helpers_validate_sources_and_integrity(provider, monkeypatch):
    invalid = provider._iter_chunk_data(None, "unknown", "owner", None)
    with pytest.raises(ValueError, match="unsupported chunk source"):
        await anext(invalid)

    async def complete_with_extra_rows(conn, table, owner_column, owner_id):
        if table == "vfs_chunks":
            yield b"abc"
            yield b"ignored"
        else:
            yield b"d"
            yield b"ignored"

    monkeypatch.setattr(provider, "_iter_chunk_data", complete_with_extra_rows)
    assert (
        await provider._append_sha256(None, "node", 3, uuid4(), 1)
        == hashlib.sha256(b"abcd").hexdigest()
    )

    async def short_existing(conn, table, owner_column, owner_id):
        yield b"a"

    monkeypatch.setattr(provider, "_iter_chunk_data", short_existing)
    with pytest.raises(RuntimeError, match="existing file chunks are shorter"):
        await provider._append_sha256(None, "node", 2, uuid4(), 0)

    async def short_staged(conn, table, owner_column, owner_id):
        yield b"ab" if table == "vfs_chunks" else b"c"

    monkeypatch.setattr(provider, "_iter_chunk_data", short_staged)
    with pytest.raises(RuntimeError, match="staged chunks are shorter"):
        await provider._append_sha256(None, "node", 2, uuid4(), 2)

    await provider._append_staged_chunks(None, {}, {"size": 0})


async def test_append_rejects_missing_existing_tail(provider):
    await _mkfile(provider, "/broken-tail.bin")
    assert await provider.write_file("/broken-tail.bin", b"abc")
    upload_id = await provider.start_upload("/broken-tail.bin", append=True)
    assert await provider.upload_part(upload_id, b"d")

    async with provider._acquire() as conn, provider._tx(conn):
        node = await provider._resolve(conn, "/broken-tail.bin")
        upload = await provider._lock_upload(conn, upload_id)
        assert node is not None and upload is not None
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM vfs_chunks WHERE node_id = %s", (node["node_id"],))
        with pytest.raises(RuntimeError, match="missing its final chunk"):
            await provider._append_staged_chunks(conn, node, upload)

    assert await provider.abort_upload(upload_id)


async def test_staged_upload_validation_and_empty_part(provider, monkeypatch):
    with pytest.raises(ValueError, match="root directory"):
        await provider.start_upload("/")
    with pytest.raises(ValueError, match="mutually exclusive"):
        await provider.start_upload("/invalid.bin", exclusive=True, append=True)

    original_root_row = provider._root_row

    async def _missing_root(conn):
        return None

    monkeypatch.setattr(provider, "_root_row", _missing_root)
    with pytest.raises(RuntimeError, match="root node is missing"):
        await provider.start_upload("/missing-root.bin")
    monkeypatch.setattr(provider, "_root_row", original_root_row)

    upload_id = await provider.start_upload("/empty-part.bin")
    assert await provider.upload_part(upload_id, b"")
    assert await provider.abort_upload(upload_id)


async def test_corrupt_partial_upload_is_aborted(provider):
    upload_id = await provider.start_upload("/corrupt-partial.bin")
    assert await provider.upload_part(upload_id, b"abc")
    async with provider._acquire() as conn, provider._tx(conn), conn.cursor() as cur:
        await cur.execute("DELETE FROM vfs_upload_chunks WHERE upload_id = %s", (upload_id,))

    with pytest.raises(RuntimeError, match="missing its partial chunk"):
        await provider.upload_part(upload_id, b"d")
    assert not await provider.abort_upload(upload_id)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"size": True}, "non-negative integer"),
        ({"size": -1}, "non-negative integer"),
        ({"sha256": "short"}, "64-character hexadecimal"),
        ({"sha256": "g" * 64}, "64-character hexadecimal"),
    ],
)
async def test_finish_upload_rejects_invalid_metadata_and_aborts(provider, kwargs, message):
    upload_id = await provider.start_upload("/invalid-metadata.bin")
    with pytest.raises(ValueError, match=message):
        await provider.finish_upload(upload_id, **kwargs)
    assert not await provider.abort_upload(upload_id)


async def test_finish_upload_discards_directory_and_missing_parent_targets(provider):
    directory_upload = await provider.start_upload("/target-directory")
    assert await provider.create_directory("/target-directory")
    assert not await provider.finish_upload(directory_upload, size=0)
    assert not await provider.abort_upload(directory_upload)

    missing_parent_upload = await provider.start_upload("/missing-parent/file.bin")
    assert not await provider.finish_upload(missing_parent_upload, size=0)
    assert not await provider.abort_upload(missing_parent_upload)
