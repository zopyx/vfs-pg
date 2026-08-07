"""Stress test: concurrent readers and writers on the Postgres provider.

Writers store random files of 10-50 MiB (incompressible, seed-based content)
for a fixed duration; readers pick random registered files and either read
them fully (sha256 verified) or read a random range (length verified).
Writes verify the provider-stored size + sha256 via node metadata.

Run with:  uv run python examples/stress.py

Env knobs:
  VFS_STRESS_SECONDS   duration (default 60)
  VFS_STRESS_WRITERS   concurrent writers (default 3)
  VFS_STRESS_READERS   concurrent readers (default 4)
  VFS_STRESS_MIN_MB / VFS_STRESS_MAX_MB  file size range (default 10 / 50)
"""

import asyncio
import hashlib
import os
import random
import time
from dataclasses import dataclass

import chuk_vfs_postgres  # noqa: F401  (registers the "postgres" provider)
from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem

DSN = os.environ.get("VFS_PG_DSN", "postgresql://vfs:vfs@localhost:5432/vfs")

SECONDS = int(os.environ.get("VFS_STRESS_SECONDS", "60"))
WRITERS = int(os.environ.get("VFS_STRESS_WRITERS", "3"))
READERS = int(os.environ.get("VFS_STRESS_READERS", "4"))
MIN_MB = int(os.environ.get("VFS_STRESS_MIN_MB", "10"))
MAX_MB = int(os.environ.get("VFS_STRESS_MAX_MB", "50"))
MI = 1024 * 1024

RUN_ID = f"{os.getpid()}_{int(time.time())}"
ROOT = f"/stress/{RUN_ID}"  # unique per run -> repeated/concurrent runs never collide

# path -> (size, sha256) of successfully written files
registry: dict[str, tuple[int, str]] = {}
registry_lock = asyncio.Lock()


@dataclass
class Counter:
    ops: int = 0
    bytes: int = 0
    errors: int = 0


def _content(seed: int, size: int) -> bytes:
    """Deterministic incompressible content (seed-based)."""
    return random.Random(seed).randbytes(size)


async def writer_loop(vfs, name: str, counter: Counter, deadline: float) -> None:
    seq = 0
    while time.perf_counter() < deadline:
        seq += 1
        size = random.randint(MIN_MB, MAX_MB) * MI
        path = f"{ROOT}/{name}_{seq:04d}.bin"
        try:
            content = _content(random.randrange(1 << 30), size)
            sha = hashlib.sha256(content).hexdigest()
            ok = await vfs.write_file(path, content)
            if ok:
                node = await vfs.get_node_info(path)
                ok = node is not None and node.size == size and node.sha256 == sha
            if ok:
                async with registry_lock:
                    registry[path] = (size, sha)
        except asyncio.CancelledError:
            raise
        except Exception:
            ok = False
        counter.ops += 1
        counter.bytes += size
        counter.errors += 0 if ok else 1


async def reader_loop(vfs, provider, counter: Counter, deadline: float) -> None:
    while time.perf_counter() < deadline:
        async with registry_lock:
            if not registry:
                await asyncio.sleep(0.02)
                continue
            path, (size, sha) = random.choice(list(registry.items()))
        try:
            if random.random() < 0.3:
                # random range read (30% of ops)
                start = random.randrange(size)
                length = min(random.randint(1, MI), size - start)
                data = await provider.read_range(path, start, start + length)
                ok = data is not None and len(data) == length
            else:
                # full read + sha256 integrity check
                data = await vfs.read_binary(path)
                ok = (
                    data is not None
                    and len(data) == size
                    and hashlib.sha256(data).hexdigest() == sha
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            data = None
            ok = False
        counter.ops += 1
        counter.bytes += len(data) if data is not None else 0
        counter.errors += 0 if ok else 1


async def main() -> None:
    vfs = AsyncVirtualFileSystem("postgres", dsn=DSN)
    await vfs.initialize()
    provider = vfs.provider
    assert provider is not None

    await vfs.mkdir(ROOT)
    deadline = time.perf_counter() + SECONDS

    w_counters = [Counter() for _ in range(WRITERS)]
    r_counters = [Counter() for _ in range(READERS)]
    writers = [
        asyncio.create_task(writer_loop(vfs, f"w{i}", c, deadline))
        for i, c in enumerate(w_counters)
    ]
    readers = [
        asyncio.create_task(reader_loop(vfs, provider, c, deadline))
        for i, c in enumerate(r_counters)
    ]

    print(
        f"=== stress: {SECONDS}s | {WRITERS} writers + {READERS} readers "
        f"| files {MIN_MB}-{MAX_MB} MiB | run {RUN_ID} ==="
    )

    t0 = time.perf_counter()
    while time.perf_counter() < deadline:
        await asyncio.sleep(10)
        elapsed = time.perf_counter() - t0
        wb = sum(c.bytes for c in w_counters)
        rb = sum(c.bytes for c in r_counters)
        print(
            f"  t={elapsed:3.0f}s  write {wb / MI / elapsed:6.1f} MiB/s "
            f"({sum(c.ops for c in w_counters):3d} ops) | "
            f"read {rb / MI / elapsed:6.1f} MiB/s ({sum(c.ops for c in r_counters):3d} ops)"
        )

    await asyncio.gather(*writers, *readers)
    elapsed = time.perf_counter() - t0

    w_ops = sum(c.ops for c in w_counters)
    w_bytes = sum(c.bytes for c in w_counters)
    w_err = sum(c.errors for c in w_counters)
    r_ops = sum(c.ops for c in r_counters)
    r_bytes = sum(c.bytes for c in r_counters)
    r_err = sum(c.errors for c in r_counters)

    print("\nresults:")
    print(
        f"  writers : {w_ops:3d} ops, {w_bytes / MI:7.0f} MiB, "
        f"{w_bytes / MI / elapsed:6.1f} MiB/s, {w_err} errors"
    )
    print(
        f"  readers : {r_ops:3d} ops, {r_bytes / MI:7.0f} MiB, "
        f"{r_bytes / MI / elapsed:6.1f} MiB/s, {r_err} errors"
    )
    print(f"  files   : {len(registry)} written, {len(await vfs.ls(ROOT))} on disk")
    print("  storage :", await provider.get_storage_stats())

    # cleanup: remove this run's files, then its directory
    names = await vfs.ls(ROOT)
    for name in names:
        await vfs.rm(f"{ROOT}/{name}")
    await vfs.rm(ROOT)
    print(f"  cleaned : removed {len(names)} files")
    print("  storage after cleanup:", await provider.get_storage_stats())

    await vfs.close()


if __name__ == "__main__":
    asyncio.run(main())
