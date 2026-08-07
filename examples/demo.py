"""End-to-end demo: chuk AsyncVirtualFileSystem -> PostgresStorageProvider -> fsspec.

Run with:  uv run python examples/demo.py
"""

import asyncio

import fsspec

import chuk_vfs_postgres  # noqa: F401  (registers the "postgres" provider)
from chuk_fsspec import ChukFileSystem
from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem

DSN = "postgresql://vfs:vfs@localhost:5432/vfs"


async def main() -> None:
    # 1) provider + chuk VFS
    vfs = AsyncVirtualFileSystem("postgres", dsn=DSN)
    await vfs.initialize()

    # 2) thin fsspec adapter on top
    fs = ChukFileSystem(vfs)
    fsspec.register_implementation("chuk", ChukFileSystem)

    # 3) plain fsspec usage
    fs.pipe_file("/projects/test/hello.txt", b"Hello from fsspec!\n")
    fs.pipe_file("/datasets/data.csv", b"a,b\n1,2\n3,4\n")

    with fs.open("/datasets/data.csv", "rb") as f:
        print("open() read:", f.read().decode().strip())

    print("ls /datasets:", fs.ls("/datasets"))
    print("cat /projects/test/hello.txt:", fs.cat_file("/projects/test/hello.txt").decode().strip())

    # 4) URL-based access
    with fsspec.open("chuk:///datasets/data.csv", "rb", vfs=vfs) as f:
        print("fsspec.open:", f.read().decode().strip())

    # 5) the richer chuk API stays available
    print("chuk ls:", await vfs.ls("/projects"))
    print("stats:", await vfs.provider.get_storage_stats())

    # 6) multi-chunk file + range read
    big = b"X" * (3 * 1024 * 1024)
    fs.pipe_file("/big.bin", big)
    print("range read:", len(fs.cat_file("/big.bin", start=2 * 1024 * 1024, end=2 * 1024 * 1024 + 16)), "bytes")

    await vfs.close()


if __name__ == "__main__":
    asyncio.run(main())
