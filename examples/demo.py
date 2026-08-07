"""End-to-end demo: chuk AsyncVirtualFileSystem -> PostgresStorageProvider -> fsspec.

Stores and reads a very large file (default 256 MiB, override with
VFS_DEMO_SIZE_MB=128..300) and reports store/read throughput.

Run with:  uv run python examples/demo.py
"""

import asyncio
import hashlib
import os
import time

import fsspec
from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem

import chuk_vfs_postgres  # noqa: F401  (registers the "postgres" provider)
from chuk_fsspec import ChukFileSystem

DSN = os.environ.get("VFS_PG_DSN", "postgresql://vfs:vfs@localhost:5432/vfs")

SIZE_MB = int(os.environ.get("VFS_DEMO_SIZE_MB", "256"))
CHUNK = 1024 * 1024  # 1 MiB write/read blocks


async def main() -> None:
    # 1) provider + chuk VFS
    vfs = AsyncVirtualFileSystem("postgres", dsn=DSN)
    await vfs.initialize()
    provider = vfs.provider
    assert provider is not None

    # 2) thin fsspec adapter on top
    fs = ChukFileSystem(vfs)
    fsspec.register_implementation("chuk", ChukFileSystem)

    # 3) small-file smoke test (plain fsspec usage)
    fs.pipe_file("/projects/test/hello.txt", b"Hello from fsspec!\n")
    fs.pipe_file("/datasets/data.csv", b"a,b\n1,2\n3,4\n")
    hello = fs.cat_file("/projects/test/hello.txt")
    assert isinstance(hello, bytes)  # narrows str|bytes from fsspec stubs
    print("cat hello.txt:", hello.decode().strip())
    print("ls /datasets:", fs.ls("/datasets"))

    # 4) very large file: store + read + performance
    size = SIZE_MB * CHUNK
    path = "/datasets/large.bin"

    print(f"\n=== large file: {size / CHUNK:.0f} MiB ({size / 1e6:.1f} MB) ===")

    # 4a) generate incompressible content (no TOAST shortcut, honest numbers)
    t0 = time.perf_counter()
    large = os.urandom(size)
    t_gen = time.perf_counter() - t0

    # 4b) store: 1 MiB streaming writes via fs.open("wb")
    t0 = time.perf_counter()
    with fs.open(path, "wb") as f:
        for i in range(0, size, CHUNK):
            f.write(large[i : i + CHUNK])  # type: ignore[arg-type]  # fsspec stubs type write(str)
    t_store = time.perf_counter() - t0

    info = fs.info(path)
    assert info["size"] == size

    # 4c) read: single-shot
    t0 = time.perf_counter()
    got = fs.cat_file(path)
    assert isinstance(got, bytes)
    t_read = time.perf_counter() - t0
    ok = hashlib.sha256(got).hexdigest() == info["sha256"]

    # 4d) read: streamed in 1 MiB blocks (exercises chunk-aware range reads)
    sha = hashlib.sha256()
    total = 0
    t0 = time.perf_counter()
    with fs.open(path, "rb") as f:
        while True:
            block = f.read(CHUNK)
            if not block:
                break
            assert isinstance(block, bytes)  # narrows str|bytes from fsspec stubs
            sha.update(block)
            total += len(block)
    t_stream = time.perf_counter() - t0
    assert total == size and sha.hexdigest() == info["sha256"]

    # 4e) range reads across chunk (1 MiB) and block (5 MiB) boundaries
    cs = CHUNK
    bs = 5 * CHUNK  # fsspec DEFAULT_BLOCK_SIZE
    windows = [
        (0, 16),               # head
        (cs - 8, 32),          # spans chunk 0 -> 1
        (bs - 8, 64),          # spans fsspec block 0 -> 1
        (size // 2, 4096),     # middle of file
        (size - 16, 16),       # tail
    ]
    for start, n in windows:
        part = fs.cat_file(path, start=start, end=start + n)
        assert part == large[start : start + n], f"mismatch at {start}"
        print(f"range {start}:{start + n} ok")

    # 4f) performance report
    def mbps(seconds: float) -> float:
        return size / seconds / CHUNK

    print(f"generate  : {t_gen:6.2f}s")
    print(
        f"store     : {t_store:6.2f}s  ({mbps(t_store):6.0f} MiB/s)"
        "  1 MiB writes via fs.open('wb')"
    )
    print(
        f"read      : {t_read:6.2f}s  ({mbps(t_read):6.0f} MiB/s)"
        f"  single-shot fs.cat_file, sha256 match: {ok}"
    )
    print(
        f"read stream: {t_stream:6.2f}s  ({mbps(t_stream):6.0f} MiB/s)"
        "  1 MiB reads via fs.open('rb'), sha256 match: True"
    )

    print("\nstats:", await provider.get_storage_stats())

    await vfs.close()


if __name__ == "__main__":
    asyncio.run(main())
