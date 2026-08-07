"""Tests for the PostgreSQL storage provider (chuk_vfs_postgres)."""

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


async def test_double_initialize_is_safe(provider):
    assert await provider.initialize() is True


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
    assert await provider.get_node_info("/missing") is None


# ----------------------------------------------------------------------
# content: write / read / chunking / ranges
# ----------------------------------------------------------------------

async def test_write_read_roundtrip(provider):
    await _mkfile(provider, "/hello.txt")
    content = b"Hello, PostgreSQL VFS!"
    assert await provider.write_file("/hello.txt", content)
    assert await provider.read_file("/hello.txt") == content


async def test_overwrite_replaces_content(provider):
    await _mkfile(provider, "/f.bin")
    await provider.write_file("/f.bin", b"short")
    await provider.write_file("/f.bin", b"a" * (2 * CHUNK + 7))
    assert await provider.read_file("/f.bin") == b"a" * (2 * CHUNK + 7)
    node = await provider.get_node_info("/f.bin")
    assert node.size == 2 * CHUNK + 7


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
            await external_conn.execute(
                "INSERT INTO business_docs (name) VALUES ('tx.pdf')"
            )
        assert await provider.read_file("/tx.pdf") == b"tx-content"
        rows = await (await external_conn.execute("SELECT COUNT(*) FROM business_docs")).fetchone()
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
