"""Tests for the fsspec adapter (chuk_fsspec) over the postgres provider.

The adapter exposes fsspec's *sync* API (async_impl=True, sync wrappers), so
these tests run synchronously against a dedicated event loop.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import os
import random
import tempfile

import fsspec
import pytest
from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem
from fsspec.asyn import get_loop

from chuk_fsspec import ChukFileSystem
from chuk_fsspec.fs import ChukBufferedFile

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


def _tree_manifest(filesystem, root: str) -> dict[str, tuple[str, int | None, str | None]]:
    """Describe a tree using portable structure and content attributes."""
    root_normalized = filesystem._strip_protocol(str(root)).replace("\\", "/").rstrip("/")
    details = filesystem.find(root, withdirs=True, detail=True)
    manifest: dict[str, tuple[str, int | None, str | None]] = {}

    for path, info in details.items():
        normalized = filesystem._strip_protocol(str(path)).replace("\\", "/").rstrip("/")
        assert normalized == root_normalized or normalized.startswith(f"{root_normalized}/")
        relative = "." if normalized == root_normalized else normalized[len(root_normalized) + 1 :]
        entry_type = info["type"]
        if entry_type == "directory":
            manifest[relative] = (entry_type, None, None)
            continue

        digest = hashlib.sha256()
        size = 0
        with filesystem.open(path, "rb") as handle:
            while block := handle.read(256 * 1024):
                digest.update(block)
                size += len(block)
        assert size == info["size"]
        manifest[relative] = (entry_type, size, digest.hexdigest())

    return manifest


# ----------------------------------------------------------------------
# protocol plumbing
# ----------------------------------------------------------------------


def test_registered_protocol(fs):
    chuk_fs = fsspec.filesystem("chuk", vfs=fs.vfs)
    assert chuk_fs.protocol == "chuk"
    assert isinstance(chuk_fs, ChukFileSystem)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("chuk://foo/bar", "/foo/bar"),
        ("chuk:///foo/bar", "/foo/bar"),
        ("chuk:////foo//./bar/", "/foo/bar"),
        ("foo//./bar", "/foo/bar"),
        ("/", "/"),
        ("", "/"),
    ],
)
def test_strip_protocol_returns_canonical_absolute_path(path, expected):
    assert ChukFileSystem._strip_protocol(path) == expected


def test_strip_protocol_handles_path_lists():
    assert ChukFileSystem._strip_protocol(
        ["chuk://one/file", "chuk:///two//./file"]
    ) == ["/one/file", "/two/file"]


def test_strip_protocol_rejects_parent_nul_and_non_string_paths():
    with pytest.raises(ValueError, match=r"'\.\.' components"):
        ChukFileSystem._strip_protocol("chuk://safe/../escape")
    with pytest.raises(ValueError, match="NUL"):
        ChukFileSystem._strip_protocol("chuk:///bad\x00path")
    with pytest.raises(TypeError, match="path must be a string"):
        ChukFileSystem._strip_protocol(42)  # type: ignore[arg-type]


def test_double_and_triple_slash_urls_address_the_same_file(fs):
    fs.pipe_file("chuk://urls/data.txt", b"canonical URL")

    assert fs.cat_file("chuk:///urls/data.txt") == b"canonical URL"
    assert fs.cat_file("/urls/data.txt") == b"canonical URL"
    assert fs.ls("chuk://urls", detail=False) == ["/urls/data.txt"]
    with fsspec.open("chuk://urls/data.txt", "rb", vfs=fs.vfs) as handle:
        assert handle.read() == b"canonical URL"
    with fsspec.open("chuk:///urls/data.txt", "rb", vfs=fs.vfs) as handle:
        assert handle.read() == b"canonical URL"


def test_all_path_entry_points_share_canonical_semantics(fs, tmp_path):
    fs.mkdir("chuk://ops//./source")
    fs.pipe_file("chuk:///ops/source//./data.bin", b"payload")

    assert fs.info("ops///source/./data.bin")["name"] == "/ops/source/data.bin"
    assert fs.ls("chuk://ops//source", detail=False) == ["/ops/source/data.bin"]
    assert fs.cat_file("///ops/source//data.bin") == b"payload"
    with fs.open("chuk://ops/./source/data.bin", "rb") as handle:
        assert handle.read() == b"payload"

    fs.cp("chuk:///ops/source/data.bin", "ops//./copy.bin")
    assert fs.cat_file("/ops/copy.bin") == b"payload"
    fs.mv("ops///copy.bin", "chuk:///ops/./moved.bin")
    assert fs.cat_file("/ops/moved.bin") == b"payload"

    destination = tmp_path / "download.bin"
    fs.get("chuk://ops//./moved.bin", destination)
    assert destination.read_bytes() == b"payload"

    assert fs.rm("chuk:///ops//moved.bin")
    assert not fs.exists("/ops/moved.bin")


def test_move_of_two_equivalent_spellings_is_a_noop(fs):
    fs.pipe_file("/same/file.txt", b"keep me")

    fs.mv("chuk://same/file.txt", "chuk:///same//./file.txt")

    assert fs.cat_file("/same/file.txt") == b"keep me"


def test_all_path_entry_points_reject_parent_traversal(fs, tmp_path):
    fs.pipe_file("/safe/source.txt", b"safe")
    invalid = "chuk://safe/../escape.txt"
    operations = [
        lambda: fs.info(invalid),
        lambda: fs.ls(invalid),
        lambda: fs.cat_file(invalid),
        lambda: fs.pipe_file(invalid, b"escape"),
        lambda: fs.mkdir(invalid),
        lambda: fs.rm(invalid),
        lambda: fs.cp(invalid, "/copy.txt"),
        lambda: fs.cp("/safe/source.txt", invalid),
        lambda: fs.mv(invalid, "/moved.txt"),
        lambda: fs.mv("/safe/source.txt", invalid),
        lambda: fs.get(invalid, tmp_path / "escape.txt"),
        lambda: fs.open(invalid, "rb"),
    ]

    for operation in operations:
        with pytest.raises(ValueError, match=r"'\.\.' components"):
            operation()

    assert fs.cat_file("/safe/source.txt") == b"safe"
    assert not fs.exists("/escape.txt")


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


def test_open_multiblock_uses_staging_without_retaining_parts(fs):
    content = random.Random(70).randbytes(BLOCK + CHUNK + 17)
    provider, _local = fs.vfs._get_provider_for_path("/streamed.bin")

    async def _staging_counts():
        async with provider._acquire() as conn, conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM vfs_uploads")
            uploads = (await cur.fetchone())[0]
            await cur.execute("SELECT COUNT(*) FROM vfs_upload_chunks")
            chunks = (await cur.fetchone())[0]
        return uploads, chunks

    with fs.open("/streamed.bin", "wb") as f:
        f.write(content)
        assert not hasattr(f, "_parts") or f._parts == []
        assert not fs.exists("/streamed.bin")
        uploads, chunks = _run_async(fs, _staging_counts())
        assert uploads == 1
        assert chunks >= 2

    assert fs.cat_file("/streamed.bin") == content
    assert _run_async(fs, _staging_counts()) == (0, 0)


def test_staged_upload_aborts_when_with_block_raises(fs):
    with (
        pytest.raises(RuntimeError, match="application failure"),
        fs.open("/aborted.bin", "wb") as f,
    ):
        f.write(b"x" * (BLOCK + 1))
        raise RuntimeError("application failure")

    assert not fs.exists("/aborted.bin")
    provider, _local = fs.vfs._get_provider_for_path("/aborted.bin")

    async def _upload_count():
        async with provider._acquire() as conn, conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM vfs_uploads")
            return (await cur.fetchone())[0]

    assert _run_async(fs, _upload_count()) == 0


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
    # fsspec's sync mv is copy+rm; directories need recursive=True
    fs.pipe_file("/dir/f.txt", b"x")
    fs.mv("/dir", "/renamed", recursive=True)
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
    # with the direct parent present it works
    fs.mkdir("/a")
    fs.mkdir("/a/b")
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
    """Two racing exclusive creates: exactly one succeeds, the other gets
    FileExistsError (enforced by the DB unique constraint at commit time)."""
    from concurrent.futures import ThreadPoolExecutor

    def _create():
        try:
            with fs.open("/race.bin", "xb") as f:
                f.write(b"winner")
            return True
        except FileExistsError:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: _create(), range(2)))
    assert sorted(results) == [False, True]
    assert fs.cat_file("/race.bin") == b"winner"


def test_entry_point_registered():
    """The chuk:// protocol is discoverable via fsspec's entry points."""
    from importlib.metadata import entry_points

    matches = [ep for ep in entry_points(group="fsspec.specs") if ep.name == "chuk"]
    assert matches, "no fsspec.specs entry point for chuk"
    assert matches[0].value == "chuk_fsspec:ChukFileSystem"


# ----------------------------------------------------------------------
# recursive export to a local filesystem
# ----------------------------------------------------------------------


def test_recursive_get_exports_complex_tree_in_sync(fs, tmp_path):
    source_root = "/export-source"
    fs.mkdir(f"{source_root}/empty-dir/nested-empty")

    files = {
        ".hidden": b"hidden\n",
        "README.txt": "Grüße from PostgreSQL\n".encode(),
        "empty.bin": b"",
        "spaces/hello world.txt": b"paths with spaces\n",
        "unicode/Grüße 東京.txt": "Unicode payload: ☃\n".encode(),
        "nested/a/b/config.json": b'{"enabled": true, "items": [1, 2, 3]}\n',
        "binary/payload.bin": random.Random(20260807).randbytes(2 * CHUNK + 123),
    }
    for relative, content in files.items():
        fs.pipe_file(f"{source_root}/{relative}", content)

    expected_paths = {
        ".",
        ".hidden",
        "README.txt",
        "binary",
        "binary/payload.bin",
        "empty-dir",
        "empty-dir/nested-empty",
        "empty.bin",
        "nested",
        "nested/a",
        "nested/a/b",
        "nested/a/b/config.json",
        "spaces",
        "spaces/hello world.txt",
        "unicode",
        "unicode/Grüße 東京.txt",
    }

    destination = tmp_path / "exported"
    fs.get(source_root, str(destination), recursive=True, chunk_size=CHUNK // 2)

    local_fs = fsspec.filesystem("file")
    postgres_manifest = _tree_manifest(fs, source_root)
    local_manifest = _tree_manifest(local_fs, str(destination))

    assert set(postgres_manifest) == expected_paths
    assert local_manifest == postgres_manifest


def test_get_file_supports_writable_file_object(fs):
    content = b"streamed into an in-memory destination"
    fs.pipe_file("/download.bin", content)
    destination = io.BytesIO()

    fs.get_file("/download.bin", destination, chunk_size=7)

    assert destination.closed is False
    assert destination.getvalue() == content


# ----------------------------------------------------------------------
# coverage: constructor, tree ops, edge cases
# ----------------------------------------------------------------------


def test_fs_constructor_rejects_none_vfs():
    with pytest.raises(ValueError, match="requires a chuk"):
        ChukFileSystem(vfs=None)


def test_mkdir_root_is_noop(fs):
    assert fs.mkdir("/") is True


def test_mv_source_missing_raises(fs):
    with pytest.raises(FileNotFoundError):
        fs.mv("/no/such/file", "/dst.txt")


def test_get_file_dir_to_filelike_raises(fs):
    fs.mkdir("/mydir")
    dest = io.BytesIO()
    with pytest.raises(IsADirectoryError):
        fs.get_file("/mydir", dest)


def test_get_file_chunk_size_zero_raises(fs):
    fs.pipe_file("/f.bin", b"data")
    with pytest.raises(ValueError, match="chunk_size must be greater"):
        fs.get_file("/f.bin", "/dev/null", chunk_size=0)


# ----------------------------------------------------------------------
# coverage: mocked / internal paths
# ----------------------------------------------------------------------


def _run_async(fs, coro, timeout=10):
    loop = get_loop()
    return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=timeout)


# _mv (fsspec mv() does copy+rm, not _mv)
def test_mv_direct_success_and_source_missing(fs):
    fs.pipe_file("/s.txt", b"data")
    assert _run_async(fs, fs._mv("/s.txt", "/d.txt")) is True
    assert not fs.exists("/s.txt")
    assert fs.cat_file("/d.txt") == b"data"
    with pytest.raises(FileNotFoundError):
        _run_async(fs, fs._mv("/no/such", "/dst.txt"))


# _cat_file: ranger returns None
def test_cat_file_ranger_returns_none(fs):
    fs.pipe_file("/f.bin", b"hello")
    provider, _local = fs.vfs._get_provider_for_path("/f.bin")
    orig = provider.read_range

    async def _none(*args, **kwargs):
        return None

    provider.read_range = _none
    try:
        with pytest.raises(FileNotFoundError):
            fs.cat_file("/f.bin", start=0, end=5)
    finally:
        provider.read_range = orig


# _cat_file: no ranger + slice
def test_cat_file_no_ranger_slice(fs):
    fs.pipe_file("/f.bin", b"0123456789")
    provider, _local = fs.vfs._get_provider_for_path("/f.bin")
    saved = provider.read_range
    provider.read_range = None
    try:
        assert fs.cat_file("/f.bin", start=2, end=7) == b"23456"
    finally:
        provider.read_range = saved


# _get_file: short local write
class _ShortWriter(io.RawIOBase):
    def write(self, b):
        return max(0, len(b) - 1)


def test_get_file_short_local_write_raises(fs):
    fs.pipe_file("/f.bin", b"hello world")
    dest = _ShortWriter()
    with pytest.raises(OSError, match="short local write"):
        fs.get_file("/f.bin", dest, chunk_size=64 * 1024)


# _get_file: no ranger fallback (file path)
def test_get_file_no_ranger_fallback(fs):
    rng = random.Random(42)
    content = rng.randbytes(1024)
    fs.pipe_file("/big.bin", content)
    provider, _local = fs.vfs._get_provider_for_path("/big.bin")
    saved = provider.read_range
    provider.read_range = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        try:
            fs.get_file("/big.bin", tmp_path, chunk_size=256)
            with open(tmp_path, "rb") as f:
                assert f.read() == content
        finally:
            os.unlink(tmp_path)
    finally:
        provider.read_range = saved


# _get_file: no ranger + file-like
def test_get_file_no_ranger_filelike(fs):
    content = b"no-ranger filelike test"
    fs.pipe_file("/nr.bin", content)
    provider, _local = fs.vfs._get_provider_for_path("/nr.bin")
    saved = provider.read_range
    provider.read_range = None
    try:
        dest = io.BytesIO()
        fs.get_file("/nr.bin", dest, chunk_size=64 * 1024)
        assert dest.getvalue() == content
    finally:
        provider.read_range = saved


# _get_file: ranger returns None mid-stream
def test_get_file_ranger_returns_none_midstream(fs):
    content = random.Random(7).randbytes(3 * 1024 * 1024)
    fs.pipe_file("/mid.bin", content)
    provider, _local = fs.vfs._get_provider_for_path("/mid.bin")
    orig = provider.read_range
    call_count = [0]

    async def _fail_second(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 2:
            return None
        return await orig(*args, **kwargs)

    provider.read_range = _fail_second
    try:
        with pytest.raises(FileNotFoundError):
            fs.get_file("/mid.bin", io.BytesIO(), chunk_size=1024 * 1024)
    finally:
        provider.read_range = orig


# _get_file: short source read
def test_get_file_short_source_read_raises(fs):
    fs.pipe_file("/short.bin", b"x" * 2000)
    provider, _local = fs.vfs._get_provider_for_path("/short.bin")
    orig = provider.read_range

    async def _short(*args, **kwargs):
        return b"short"

    provider.read_range = _short
    try:
        with pytest.raises(OSError, match="short source read"):
            fs.get_file("/short.bin", io.BytesIO(), chunk_size=100)
    finally:
        provider.read_range = orig


# _commit: generic fallback (no write_file_atomic)
def test_commit_generic_fallback_success(fs):
    provider, _local = fs.vfs._get_provider_for_path("/")
    saved = provider.write_file_atomic
    provider.write_file_atomic = None
    try:
        fs.pipe_file("/gen.txt", b"generic")
        assert fs.cat_file("/gen.txt") == b"generic"
    finally:
        provider.write_file_atomic = saved


# _pipe_file: commit fails
def test_pipe_file_commit_fails(fs):
    fs.pipe_file("/pf.bin", b"old")
    provider, _local = fs.vfs._get_provider_for_path("/pf.bin")
    orig = provider.write_file_atomic

    async def _fail(path, content, *, exclusive=False):
        return 0

    provider.write_file_atomic = _fail
    try:
        with pytest.raises(OSError, match="write failed"):
            fs.pipe_file("/pf.bin", b"new")
    finally:
        provider.write_file_atomic = orig


# ChukBufferedFile._upload_chunk: OSError on non-x write failure
def test_upload_chunk_write_failure_raises_oserror(fs):
    bf = ChukBufferedFile(fs, "/wb_fail.bin", mode="wb")
    bf.buffer.write(b"data")
    orig = fs.commit

    def _fail_commit(path, content, *, exclusive=False):
        return False

    fs.commit = _fail_commit
    try:
        with pytest.raises(OSError, match="write failed"):
            bf._upload_chunk(final=True)
    finally:
        fs.commit = orig


# _mkdir failure
def test_mkdir_vfs_failure_returns_false(fs):
    orig = fs.vfs.mkdir

    async def _fail_mkdir(path):
        if path == "/fail-dir":
            return False
        return await orig(path)

    fs.vfs.mkdir = _fail_mkdir
    try:
        assert fs.mkdir("/fail-dir") is False
    finally:
        fs.vfs.mkdir = orig


# _get_file: no-ranger path, read_binary returns None (line 246)
def test_get_file_no_ranger_read_binary_none(fs):
    fs.pipe_file("/nb.bin", b"data")
    provider, _local = fs.vfs._get_provider_for_path("/nb.bin")
    saved = provider.read_range
    provider.read_range = None
    orig_read = fs.vfs.read_binary

    async def _none(path):
        return None

    fs.vfs.read_binary = _none
    try:
        with pytest.raises(FileNotFoundError):
            fs.get_file("/nb.bin", io.BytesIO())
    finally:
        fs.vfs.read_binary = orig_read
        provider.read_range = saved


# _get_file: no-ranger path, size mismatch (line 248-249)
def test_get_file_no_ranger_size_mismatch(fs):
    fs.pipe_file("/sm.bin", b"hello world")
    provider, _local = fs.vfs._get_provider_for_path("/sm.bin")
    saved = provider.read_range
    provider.read_range = None
    orig_read = fs.vfs.read_binary

    async def _wrong(path):
        return b"short"

    fs.vfs.read_binary = _wrong
    try:
        with pytest.raises(OSError, match="source changed"):
            fs.get_file("/sm.bin", io.BytesIO())
    finally:
        fs.vfs.read_binary = orig_read
        provider.read_range = saved


# _commit: generic fallback, exclusive + exists (line 307-308)
def test_commit_generic_exclusive_exists(fs):
    fs.pipe_file("/ex.txt", b"old")
    provider, _local = fs.vfs._get_provider_for_path("/")
    saved = provider.write_file_atomic
    provider.write_file_atomic = None
    try:
        with pytest.raises(FileExistsError):
            fs.open("/ex.txt", "xb")
    finally:
        provider.write_file_atomic = saved


# _commit generic fallback: direct exclusive=True call (line 308)
def test_commit_generic_exclusive_direct(fs):
    fs.pipe_file("/cex.txt", b"old")
    provider, _local = fs.vfs._get_provider_for_path("/")
    saved = provider.write_file_atomic
    provider.write_file_atomic = None
    try:
        result = _run_async(fs, fs._commit("/cex.txt", b"new", exclusive=True))
        assert result is False
    finally:
        provider.write_file_atomic = saved
