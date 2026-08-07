"""Tests for the PostgreSQL storage provider (chuk_vfs_postgres).

Covers the full chuk provider API: lifecycle, schema, nodes, content
write/read, chunk-aware range reads, move/delete semantics, metadata,
transaction joining and the chuk AsyncVirtualFileSystem integration.
"""

from __future__ import annotations

import hashlib
import random

import pytest

from chuk_virtual_fs.node_info import EnhancedNodeInfo

from chuk_vfs_postgres import PostgresStorageProvider

CHUNK = 1024 * 1024


async def _mkfile(provider, path: str) -> None:
    parent = "/" if "/" not in path[1:] else path.rsplit("/", 1)[0]
    name = path.rsplit("/", 1)[-1]
    assert await provider.create_node(
        EnhancedNodeInfo(name=name, is_dir=False, parent_path=parent)
    )


async def _mkdir(provider, path: str) -> None:
    parent = "/" if "/" not in path[1:] else path.rsplit("/", 1)[0]
    name = path.rsplit("/", 1)[-1]
    assert await provider.create_node(
        EnhancedNodeInfo(name=name, is_dir=True, parent_path=parent)
    )


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


async def test_double_initialize_is_safe(provider):
    assert await provider.initialize() is True


async def test_double_close_is_safe(provider):
    await provider.close()
    await provider.close()  # must not raise
    # provider is usable again after re-initialize
    assert await provider.initialize() is True


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


# ----------------------------------------------------------------------
# nodes
# ----------------------------------------------------------------------


async def test_create_node_requires_existing_parent(provider):
    assert (
        await provider.create_node(
            EnhancedNodeInfo(name="x", is_dir=True, parent_path="/nope")
        )
        is False
    )
    # parent must be a directory
    await _mkfile(provider, "/f.txt")
    assert (
        await provider.create_node(
            EnhancedNodeInfo(name="x", is_dir=True, parent_path="/f.txt")
        )
        is False
    )


async def test_duplicate_node_rejected(provider):
    await _mkdir(provider, "/a")
    assert (
        await provider.create_node(
            EnhancedNodeInfo(name="a", is_dir=True, parent_path="/")
        )
        is False
    )


async def test_root_node_ops_rejected(provider):
    assert (
        await provider.create_node(
            EnhancedNodeInfo(name="", is_dir=True, parent_path="/")
        )
        is False
    )
    assert await provider.delete_node("/") is False
    assert await provider.move_node("/", "/x") is False
    assert await provider.move_node("/x", "/") is False


async def test_path_trailing_slash_normalized(provider):
    await _mkfile(provider, "/f.bin")
    assert await provider.write_file("/f.bin/", b"x")  # "/f.bin/" -> "/f.bin"
    assert await provider.read_file("/f.bin//") == b"x"
    assert await provider.exists("/f.bin/")


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
    assert (
        await provider.read_range("/range.bin", cs - 10, cs + 10)
        == content[cs - 10 : cs + 10]
    )
    assert (
        await provider.read_range("/range.bin", cs + 5, cs + 5 + 100)
        == content[cs + 5 : cs + 105]
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
    await external_conn.execute(
        "CREATE TABLE business_docs (id serial PRIMARY KEY, name text)"
    )
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
                await external_conn.execute(
                    "INSERT INTO business_docs (name) VALUES ('tx.pdf')"
                )
                raise RuntimeError("abort")  # trigger rollback
        assert not await provider.exists("/tx.pdf")
        rows = await (
            await external_conn.execute("SELECT COUNT(*) FROM business_docs")
        ).fetchone()
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
            await external_conn.execute(
                "INSERT INTO business_docs (name) VALUES ('tx.pdf')"
            )
        assert await provider.read_file("/tx.pdf") == b"tx-content"
        rows = await (
            await external_conn.execute("SELECT COUNT(*) FROM business_docs")
        ).fetchone()
        assert rows[0] == 1
    finally:
        await external_conn.execute("DROP TABLE IF EXISTS business_docs")


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
