"""Tests for the fsspec adapter (chuk_fsspec) over the postgres provider.

The adapter exposes fsspec's *sync* API (async_impl=True, sync wrappers), so
these tests run synchronously against a dedicated event loop.
"""

from __future__ import annotations

import asyncio
import hashlib
import random

import fsspec
import pytest
from fsspec.asyn import get_loop

from chuk_fsspec import ChukFileSystem
from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem

# register once so `fsspec.filesystem("chuk", ...)` / `fsspec.open("chuk://...")` work
fsspec.register_implementation("chuk", ChukFileSystem)

CHUNK = 1024 * 1024
BLOCK = 5 * CHUNK  # fsspec DEFAULT_BLOCK_SIZE


@pytest.fixture
def fs(dsn: str):
    """A ChukFileSystem over postgres, driven by fsspec's IO loop thread."""
    loop = get_loop()  # dedicated daemon thread running the loop forever

    async def _make() -> AsyncVirtualFileSystem:
        vfs = AsyncVirtualFileSystem("postgres", dsn=dsn)
        await vfs.initialize()
        return vfs

    vfs = asyncio.run_coroutine_threadsafe(_make(), loop).result(timeout=30)
    chuk_fs = ChukFileSystem(vfs)  # self.loop == the same fsspec IO loop
    yield chuk_fs
    asyncio.run_coroutine_threadsafe(vfs.close(), loop).result(timeout=30)


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
    assert fs.exists("/") is True

    info = fs.info("/projects/test/hello.txt")
    assert info["name"] == "/projects/test/hello.txt"
    assert info["type"] == "file"
    assert info["size"] == 5
    assert info["sha256"] == hashlib.sha256(b"Hello").hexdigest()
    assert info["mtime"] is not None

    # fsspec convention: non-detailed ls returns full paths
    assert fs.ls("/projects", detail=False) == ["/projects/test"]
    assert fs.ls("/projects/test", detail=False) == ["/projects/test/hello.txt"]
    # fsspec 2026.x default: detail=True -> info dicts
    entries = fs.ls("/projects/test")
    assert entries[0]["type"] == "file"

    with pytest.raises(FileNotFoundError):
        fs.info("/nope")
    with pytest.raises(FileNotFoundError):
        fs.ls("/nope")


# ----------------------------------------------------------------------
# content through the fsspec API
# ----------------------------------------------------------------------


def test_pipe_and_cat(fs):
    fs.pipe_file("/a.txt", b"alpha")
    fs.pipe_file("/dir/b.txt", b"beta")  # parent dirs auto-created
    assert fs.cat_file("/a.txt") == b"alpha"
    assert fs.cat_file("/dir/b.txt") == b"beta"
    with pytest.raises(FileNotFoundError):
        fs.cat_file("/missing.bin")


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


def test_open_exclusive_mode(fs):
    with fs.open("/excl.bin", "xb") as f:
        f.write(b"exclusive")
    assert fs.cat_file("/excl.bin") == b"exclusive"
    # second creation attempt must fail
    with pytest.raises(FileExistsError):
        fs.open("/excl.bin", "xb")


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


def test_mv_directory_keeps_children(fs):
    fs.pipe_file("/dir/f.txt", b"x")
    fs.mv("/dir", "/renamed")
    assert fs.cat_file("/renamed/f.txt") == b"x"
    assert not fs.exists("/dir")


def test_cp(fs):
    fs.pipe_file("/orig.bin", b"copy-me")
    fs.cp("/orig.bin", "/copy.bin")
    assert fs.cat_file("/copy.bin") == b"copy-me"


def test_duplicate_mkdir_is_noop(fs):
    fs.mkdir("/d")
    assert fs.mkdir("/d")  # idempotent


def test_mkdir_create_parents_false(fs):
    with pytest.raises(FileNotFoundError):
        fs.mkdir("/a/b/c", create_parents=False)
    # with the parent present it works
    fs.mkdir("/a")
    assert fs.mkdir("/a/b/c", create_parents=False) is True


def test_mkdir_over_file_raises(fs):
    fs.pipe_file("/f.txt", b"x")
    with pytest.raises(FileExistsError):
        fs.mkdir("/f.txt/sub")
    with pytest.raises(FileExistsError):
        fs.mkdir("/f.txt")


# ----------------------------------------------------------------------
# fsspec.open with a real URL
# ----------------------------------------------------------------------


def test_fsspec_open_url(fs):
    with fsspec.open("chuk:///datasets/data.csv", "wb", vfs=fs.vfs) as f:
        f.write(b"a,b\n1,2\n")

    with fsspec.open("chuk:///datasets/data.csv", "rb", vfs=fs.vfs) as f:
        assert f.read() == b"a,b\n1,2\n"

    assert fs.ls("/datasets", detail=False) == ["/datasets/data.csv"]


# ----------------------------------------------------------------------
# large files: chunk + block boundaries
# ----------------------------------------------------------------------


def test_large_file_boundaries_and_streamed_read(fs):
    size = 6 * CHUNK + 123  # crosses the 5 MiB fsspec block
    rng = random.Random(1234)
    content = rng.randbytes(size)

    with fs.open("/large.bin", "wb") as f:
        for i in range(0, size, CHUNK):
            f.write(content[i : i + CHUNK])

    assert fs.info("/large.bin")["size"] == size
    assert fs.cat_file("/large.bin") == content

    # ranges across the 5 MiB block boundary and the 1 MiB chunk boundary
    for start, end in [(BLOCK - 8, BLOCK + 8), (CHUNK - 8, CHUNK + 8), (size - 16, size)]:
        assert fs.cat_file("/large.bin", start=start, end=end) == content[start:end]

    # streamed read through the buffered file
    with fs.open("/large.bin", "rb") as f:
        got = b""
        while True:
            part = f.read(CHUNK)
            if not part:
                break
            got += part
    assert got == content


# ----------------------------------------------------------------------
# exclusivity + registration
# ----------------------------------------------------------------------


def test_concurrent_xb_single_winner(fs):
    """Two racing exclusive creates: exactly one succeeds."""
    from concurrent.futures import ThreadPoolExecutor

    def _create():
        with fs.open("/race.bin", "xb") as f:
            f.write(b"winner")
        return True

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: _create(), range(2)))
    assert results.count(True) == 1
    assert fs.cat_file("/race.bin") == b"winner"


def test_entry_point_registered():
    """The chuk:// protocol is discoverable via fsspec's entry points."""
    from importlib.metadata import entry_points

    matches = [ep for ep in entry_points(group="fsspec.specs") if ep.name == "chuk"]
    assert matches, "no fsspec.specs entry point for chuk"
    assert matches[0].value == "chuk_fsspec:ChukFileSystem"
