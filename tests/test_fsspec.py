"""Tests for the fsspec adapter (chuk_fsspec) over the postgres provider.

The adapter exposes fsspec's *sync* API (async_impl=True, sync wrappers), so
these tests run synchronously against a dedicated event loop.
"""

import asyncio
import random

import fsspec
import pytest

from chuk_fsspec import ChukFileSystem
from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem

from conftest import DSN

# register once so `fsspec.filesystem("chuk", ...)` / `fsspec.open("chuk://...")` work
fsspec.register_implementation("chuk", ChukFileSystem)

CHUNK = 1024 * 1024


@pytest.fixture
def fs():
    """A ChukFileSystem over postgres, driven by fsspec's IO loop thread."""
    from fsspec.asyn import get_loop

    loop = get_loop()  # dedicated daemon thread running the loop forever

    async def _make() -> AsyncVirtualFileSystem:
        vfs = AsyncVirtualFileSystem("postgres", dsn=DSN)
        await vfs.initialize()
        return vfs

    vfs = asyncio.run_coroutine_threadsafe(_make(), loop).result()
    chuk_fs = ChukFileSystem(vfs)  # self.loop == the same fsspec IO loop
    yield chuk_fs
    asyncio.run_coroutine_threadsafe(vfs.close(), loop).result()


# ----------------------------------------------------------------------
# protocol plumbing
# ----------------------------------------------------------------------

def test_registered_protocol(fs):
    chuk_fs = fsspec.filesystem("chuk", vfs=fs.vfs)
    assert chuk_fs.protocol == "chuk"
    assert isinstance(chuk_fs, ChukFileSystem)


def test_info_and_ls(fs):
    fs.pipe_file("/projects/test/hello.txt", b"Hello")
    assert fs.exists("/projects/test/hello.txt")
    assert fs.isfile("/projects/test/hello.txt")
    assert fs.isdir("/projects")

    info = fs.info("/projects/test/hello.txt")
    assert info["name"] == "/projects/test/hello.txt"
    assert info["type"] == "file"
    assert info["size"] == 5
    assert info["sha256"] is not None

    assert fs.ls("/projects", detail=False) == ["test"]
    assert fs.ls("/projects/test", detail=False) == ["hello.txt"]
    # fsspec 2026.x default: detail=True -> info dicts
    entries = fs.ls("/projects/test")
    assert entries[0]["type"] == "file"

    with pytest.raises(FileNotFoundError):
        fs.info("/nope")


# ----------------------------------------------------------------------
# content through the fsspec API
# ----------------------------------------------------------------------

def test_pipe_and_cat(fs):
    fs.pipe_file("/a.txt", b"alpha")
    fs.pipe_file("/dir/b.txt", b"beta")  # parent dirs auto-created
    assert fs.cat_file("/a.txt") == b"alpha"
    assert fs.cat_file("/dir/b.txt") == b"beta"


def test_cat_file_range(fs):
    content = bytes(range(256)) * (CHUNK // 256) * 2
    fs.pipe_file("/big.bin", content)
    assert fs.cat_file("/big.bin", start=100, end=200) == content[100:200]
    assert fs.cat_file("/big.bin", start=CHUNK - 10, end=CHUNK + 10) == content[
        CHUNK - 10 : CHUNK + 10
    ]
    assert fs.cat_file("/big.bin", start=CHUNK + 5) == content[CHUNK + 5 :]


def test_open_write_then_read(fs):
    with fs.open("/f.bin", "wb") as f:
        f.write(b"hello ")
        f.write(b"world")
    assert fs.cat_file("/f.bin") == b"hello world"

    with fs.open("/f.bin", "rb") as f:
        assert f.read() == b"hello world"


def test_open_multiblock_write(fs):
    # > DEFAULT_BLOCK_SIZE (5 MiB) forces multiple _upload_chunk calls
    rng = random.Random(7)
    content = bytes(rng.getrandbits(8) for _ in range(6 * CHUNK))
    with fs.open("/large.bin", "wb") as f:
        f.write(content)
    assert fs.cat_file("/large.bin") == content


def test_open_append(fs):
    fs.pipe_file("/log.txt", b"line1\n")
    with fs.open("/log.txt", "ab") as f:
        f.write(b"line2\n")
    assert fs.cat_file("/log.txt") == b"line1\nline2\n"


def test_open_seek_range_read(fs):
    rng = random.Random(11)
    content = bytes(rng.getrandbits(8) for _ in range(3 * CHUNK))
    fs.pipe_file("/seek.bin", content)

    with fs.open("/seek.bin", "rb") as f:
        f.seek(1_000_000)
        assert f.read(64 * 1024) == content[1_000_000 : 1_000_000 + 64 * 1024]
        f.seek(2 * CHUNK - 50)
        assert f.read(100) == content[2 * CHUNK - 50 : 2 * CHUNK + 50]
        f.seek(0)
        assert f.read(10) == content[:10]


def test_open_missing_file_raises(fs):
    with pytest.raises(FileNotFoundError):
        fs.open("/missing.bin", "rb")
    with pytest.raises(FileNotFoundError):
        fs.cat_file("/missing.bin")


def test_open_unsupported_mode(fs):
    with pytest.raises(NotImplementedError):
        fs.open("/x.txt", "r")


# ----------------------------------------------------------------------
# tree operations
# ----------------------------------------------------------------------

def test_mkdir_parents_and_rm(fs):
    assert fs.mkdir("/a/b/c")  # create_parents=True default
    assert fs.isdir("/a/b/c")
    # rm file
    fs.pipe_file("/a/b/c/f.txt", b"x")
    assert fs.rm("/a/b/c/f.txt")
    assert not fs.exists("/a/b/c/f.txt")
    # non-recursive rm of non-empty dir fails
    fs.pipe_file("/a/b/c/g.txt", b"x")
    with pytest.raises(ValueError):
        fs.rm("/a/b/c")
    # recursive rm
    assert fs.rm("/a", recursive=True)
    assert not fs.exists("/a")
    with pytest.raises(FileNotFoundError):
        fs.rm("/a")


def test_mv(fs):
    fs.pipe_file("/src.txt", b"move me")
    fs.mv("/src.txt", "/dst.txt")  # fsspec mv returns None
    assert not fs.exists("/src.txt")
    assert fs.cat_file("/dst.txt") == b"move me"
    with pytest.raises(FileNotFoundError):
        fs.mv("/nope", "/x.txt")


def test_duplicate_mkdir_is_noop(fs):
    fs.mkdir("/d")
    assert fs.mkdir("/d")  # idempotent


# ----------------------------------------------------------------------
# fsspec.open with a real URL
# ----------------------------------------------------------------------

def test_fsspec_open_url(fs):
    with fsspec.open("chuk:///datasets/data.csv", "wb", vfs=fs.vfs) as f:
        f.write(b"a,b\n1,2\n")

    with fsspec.open("chuk:///datasets/data.csv", "rb", vfs=fs.vfs) as f:
        assert f.read() == b"a,b\n1,2\n"

    assert fs.ls("/datasets", detail=False) == ["data.csv"]
