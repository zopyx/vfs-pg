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
from collections import Counter as ExcCounter
from dataclasses import dataclass, field

from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem

import chuk_vfs_postgres  # noqa: F401  (registers the "postgres" provider)

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
    bytes_attempted: int = 0
    bytes_ok: int = 0
    errors: int = 0
    exceptions: ExcCounter[str] = field(default_factory=ExcCounter)


def _make_content(seed: int, size: int) -> tuple[bytes, str]:
    """Deterministic incompressible content + its sha256 (blocking, CPU)."""
    content = random.Random(seed).randbytes(size)
    return content, hashlib.sha256(content).hexdigest()


async def writer_loop(vfs, name: str, counter: Counter, deadline: float) -> None:
    seq = 0
    while time.perf_counter() < deadline:
        seq += 1
        size = random.randint(MIN_MB, MAX_MB) * MI
        path = f"{ROOT}/{name}_{seq:04d}.bin"
        ok = False
        try:
            # generation + hashing are CPU-bound -> keep them off the loop
            content, sha = await asyncio.to_thread(
                _make_content, random.randrange(1 << 30), size
            )
            ok = await vfs.write_file(path, content)
            if ok:
                node = await vfs.get_node_info(path)
                ok = node is not None and node.size == size and node.sha256 == sha
            if ok:
                async with registry_lock:
                    registry[path] = (size, sha)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - stress runs must not die
            counter.exceptions[type(exc).__name__] += 1
        counter.ops += 1
        counter.bytes_attempted += size
        if ok:
            counter.bytes_ok += size
        else:
            counter.errors += 1


async def reader_loop(vfs, provider, counter: Counter, deadline: float) -> None:
    while time.perf_counter() < deadline:
        # never sleep while holding the lock
        if not registry:
            await asyncio.sleep(0.02)
            continue
        async with registry_lock:
            if not registry:
                continue
            path, (size, sha) = random.choice(list(registry.items()))
        data: bytes | None = None
        ok = False
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
        except Exception as exc:  # noqa: BLE001 - stress runs must not die
            counter.exceptions[type(exc).__name__] += 1
        counter.ops += 1
        counter.bytes_attempted += size
        if ok:
            counter.bytes_ok += len(data) if data is not None else 0
        else:
            counter.errors += 1


async def main() -> None:
    vfs = AsyncVirtualFileSystem("postgres", dsn=DSN)
    await vfs.initialize()
    provider = vfs.provider
    assert provider is not None

    w_counters = [Counter() for _ in range(WRITERS)]
    r_counters = [Counter() for _ in range(READERS)]

    try:
        await vfs.mkdir(ROOT)
        deadline = time.perf_counter() + SECONDS

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
        interval = min(10, max(1, SECONDS // 3))
        while time.perf_counter() < deadline:
            await asyncio.sleep(interval)
            elapsed = time.perf_counter() - t0
            wb = sum(c.bytes_ok for c in w_counters)
            rb = sum(c.bytes_ok for c in r_counters)
            print(
                f"  t={elapsed:3.0f}s  write {wb / MI / elapsed:6.1f} MiB/s "
                f"({sum(c.ops for c in w_counters):3d} ops) | "
                f"read {rb / MI / elapsed:6.1f} MiB/s ({sum(c.ops for c in r_counters):3d} ops)"
            )

        await asyncio.gather(*writers, *readers)
        elapsed = time.perf_counter() - t0

        w_ops = sum(c.ops for c in w_counters)
        w_ok = sum(c.bytes_ok for c in w_counters)
        w_att = sum(c.bytes_attempted for c in w_counters)
        w_err = sum(c.errors for c in w_counters)
        r_ops = sum(c.ops for c in r_counters)
        r_ok = sum(c.bytes_ok for c in r_counters)
        r_att = sum(c.bytes_attempted for c in r_counters)
        r_err = sum(c.errors for c in r_counters)
        exceptions = sum((c.exceptions for c in w_counters + r_counters), ExcCounter())

        print("\nresults:")
        print(
            f"  writers : {w_ops:3d} ops, {w_ok / MI:7.0f} MiB ok / "
            f"{w_att / MI:7.0f} MiB attempted, {w_ok / MI / elapsed:6.1f} MiB/s, "
            f"{w_err} errors"
        )
        print(
            f"  readers : {r_ops:3d} ops, {r_ok / MI:7.0f} MiB ok / "
            f"{r_att / MI:7.0f} MiB attempted, {r_ok / MI / elapsed:6.1f} MiB/s, "
            f"{r_err} errors"
        )
        print(f"  files   : {len(registry)} written, {len(await vfs.ls(ROOT))} on disk")
        if exceptions:
            print("  exceptions:", dict(exceptions))
        print("  storage :", await provider.get_storage_stats())
    finally:
        # cleanup runs even when the run was cancelled or crashed
        try:
            names = await vfs.ls(ROOT)
            for name in names:
                await vfs.rm(f"{ROOT}/{name}")
            await vfs.rm(ROOT)
            print(f"  cleaned : removed {len(names)} files")
        except Exception as exc:  # noqa: BLE001
            print(f"  cleanup failed: {type(exc).__name__}: {exc}")
        finally:
            await vfs.close()


if __name__ == "__main__":
    asyncio.run(main())
