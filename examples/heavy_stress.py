"""Five-minute mixed-file PostgreSQL VFS stress harness.

The default workload runs small and large writers alongside full-file and
range readers for five minutes. Writers rotate through fixed slots, which
bounds the live data set while still exercising repeated atomic overwrites.
Readers verify every byte (SHA-256 for full reads, deterministic bytes for
range reads). A report is printed every ten seconds.

Typical run::

    uv run python examples/heavy_stress.py

Short smoke run::

    uv run python examples/heavy_stress.py --duration 10 \
        --small-writers 1 --large-writers 1 --readers 2 \
        --large-min-mib 1 --large-max-mib 2 --report-interval 2
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import random
import signal
import time
from collections import Counter as ExceptionCounter
from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem

import chuk_vfs_postgres  # noqa: F401  (registers the "postgres" provider)

KIB = 1024
MIB = 1024 * KIB
CONTENT_BLOCK = 64 * KIB
DEFAULT_DSN = "postgresql://vfs:vfs@localhost:5432/vfs"


@dataclass(frozen=True)
class FileRecord:
    """The committed content version currently stored in one slot."""

    path: str
    kind: str
    generation: int
    seed: int
    size: int
    sha256: str


@dataclass
class FileSlot:
    """A bounded file location shared by one writer and many readers."""

    path: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    record: FileRecord | None = None


@dataclass(frozen=True)
class MetricSnapshot:
    ops: int
    bytes_attempted: int
    bytes_ok: int
    errors: int


@dataclass
class Metrics:
    """Cumulative activity counters for one operation class."""

    name: str
    ops: int = 0
    bytes_attempted: int = 0
    bytes_ok: int = 0
    errors: int = 0
    exceptions: ExceptionCounter[str] = field(default_factory=ExceptionCounter)
    latencies_ms: deque[float] = field(default_factory=lambda: deque(maxlen=10_000))

    def record(
        self,
        *,
        attempted: int,
        transferred: int,
        started: float,
        error: str | None = None,
    ) -> None:
        self.ops += 1
        self.bytes_attempted += attempted
        self.bytes_ok += transferred
        self.latencies_ms.append((time.perf_counter() - started) * 1000)
        if error is not None:
            self.errors += 1
            self.exceptions[error] += 1

    def snapshot(self) -> MetricSnapshot:
        return MetricSnapshot(
            ops=self.ops,
            bytes_attempted=self.bytes_attempted,
            bytes_ok=self.bytes_ok,
            errors=self.errors,
        )

    def percentile(self, fraction: float) -> float:
        if not self.latencies_ms:
            return 0.0
        ordered = sorted(self.latencies_ms)
        return ordered[round((len(ordered) - 1) * fraction)]


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


def _content_block(seed: int, block_number: int) -> bytes:
    payload = seed.to_bytes(16, "big") + block_number.to_bytes(8, "big")
    return hashlib.shake_256(payload).digest(CONTENT_BLOCK)


def _content_slice(seed: int, start: int, end: int) -> bytes:
    """Generate an arbitrary range without generating the preceding bytes."""
    if end <= start:
        return b""
    first = start // CONTENT_BLOCK
    last = (end - 1) // CONTENT_BLOCK
    blocks = b"".join(_content_block(seed, number) for number in range(first, last + 1))
    offset = start - first * CONTENT_BLOCK
    return blocks[offset : offset + (end - start)]


def _make_content(seed: int, size: int) -> tuple[bytes, str]:
    content = _content_slice(seed, 0, size)
    return content, hashlib.sha256(content).hexdigest()


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _build_slots(root: str, kind: str, writers: int, slots_per_writer: int) -> list[list[FileSlot]]:
    return [
        [
            FileSlot(path=f"{root}/{kind}/writer-{writer:02d}-slot-{slot:02d}.bin")
            for slot in range(slots_per_writer)
        ]
        for writer in range(writers)
    ]


async def _ensure_directory(vfs: Any, path: str) -> None:
    if await vfs.exists(path):
        if not await vfs.is_dir(path):
            raise RuntimeError(f"stress path exists and is not a directory: {path}")
        return
    if not await vfs.mkdir(path):
        raise RuntimeError(f"could not create stress directory: {path}")


async def writer_loop(
    vfs: Any,
    *,
    kind: str,
    worker_number: int,
    slots: list[FileSlot],
    minimum_size: int,
    maximum_size: int,
    metrics: Metrics,
    stop: asyncio.Event,
    deadline: float,
) -> None:
    rng = random.Random(f"{kind}-writer-{worker_number}-{time.time_ns()}")
    generation = 0

    while not stop.is_set() and time.perf_counter() < deadline:
        slot = slots[generation % len(slots)]
        generation += 1
        seed = rng.getrandbits(128)
        size = rng.randint(minimum_size, maximum_size)
        started = time.perf_counter()
        error: str | None = None
        transferred = 0

        try:
            content, digest = await asyncio.to_thread(_make_content, seed, size)
            async with slot.lock:
                ok = await vfs.write_file(slot.path, content)
                if not ok:
                    error = "WriteRejected"
                else:
                    node = await vfs.get_node_info(slot.path)
                    if node is None or node.size != size or node.sha256 != digest:
                        error = "WriteMetadataMismatch"
                    else:
                        slot.record = FileRecord(
                            path=slot.path,
                            kind=kind,
                            generation=generation,
                            seed=seed,
                            size=size,
                            sha256=digest,
                        )
                        transferred = size
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - one failure must not stop the run
            error = type(exc).__name__

        metrics.record(
            attempted=size,
            transferred=transferred,
            started=started,
            error=error,
        )


async def reader_loop(
    vfs: Any,
    provider: Any,
    *,
    worker_number: int,
    slots: list[FileSlot],
    full_metrics: Metrics,
    range_metrics: Metrics,
    range_ratio: float,
    maximum_range: int,
    stop: asyncio.Event,
    deadline: float,
) -> None:
    rng = random.Random(f"reader-{worker_number}-{time.time_ns()}")

    while not stop.is_set() and time.perf_counter() < deadline:
        available = [slot for slot in slots if slot.record is not None]
        if not available:
            await asyncio.sleep(0.02)
            continue

        slot = rng.choice(available)
        async with slot.lock:
            record = slot.record
            if record is None:
                continue

            use_range = record.size > 0 and rng.random() < range_ratio
            metrics = range_metrics if use_range else full_metrics
            started = time.perf_counter()
            error: str | None = None
            attempted = record.size
            transferred = 0

            try:
                if use_range:
                    start = rng.randrange(record.size)
                    maximum = min(maximum_range, record.size - start)
                    length = rng.randint(1, maximum)
                    attempted = length
                    expected = await asyncio.to_thread(
                        _content_slice, record.seed, start, start + length
                    )
                    data = await provider.read_range(record.path, start, start + length)
                    if data != expected:
                        error = "RangeIntegrityMismatch"
                    else:
                        transferred = length
                else:
                    data = await vfs.read_binary(record.path)
                    if data is None:
                        error = "FullReadMissing"
                    elif len(data) != record.size:
                        error = "FullReadSizeMismatch"
                    elif await asyncio.to_thread(_sha256, data) != record.sha256:
                        error = "FullReadHashMismatch"
                    else:
                        transferred = record.size
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - one failure must not stop the run
                error = type(exc).__name__

            metrics.record(
                attempted=attempted,
                transferred=transferred,
                started=started,
                error=error,
            )


def _delta(current: MetricSnapshot, previous: MetricSnapshot) -> MetricSnapshot:
    return MetricSnapshot(
        ops=current.ops - previous.ops,
        bytes_attempted=current.bytes_attempted - previous.bytes_attempted,
        bytes_ok=current.bytes_ok - previous.bytes_ok,
        errors=current.errors - previous.errors,
    )


async def print_report(
    provider: Any,
    metrics: list[Metrics],
    previous: dict[str, MetricSnapshot],
    *,
    interval_seconds: float,
    elapsed: float,
    duration: float,
    slots: list[FileSlot],
    tasks: list[asyncio.Task[Any]],
    final: bool = False,
) -> dict[str, MetricSnapshot]:
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    remaining = max(0.0, duration - elapsed)
    live_files = sum(slot.record is not None for slot in slots)
    active_tasks = sum(not task.done() for task in tasks)
    heading = "FINAL" if final else "REPORT"
    print(
        f"\n[{timestamp}] {heading} t={elapsed:6.1f}s remaining={remaining:6.1f}s "
        f"live_files={live_files} active_tasks={active_tasks}",
        flush=True,
    )

    current_snapshots: dict[str, MetricSnapshot] = {}
    for item in metrics:
        current = item.snapshot()
        current_snapshots[item.name] = current
        change = _delta(current, previous[item.name])
        interval_ok_mib = change.bytes_ok / MIB
        interval_attempted_mib = change.bytes_attempted / MIB
        total_ok_mib = current.bytes_ok / MIB
        total_attempted_mib = current.bytes_attempted / MIB
        mib_per_second = change.bytes_ok / MIB / max(interval_seconds, 0.001)
        total_mib_per_second = current.bytes_ok / MIB / max(elapsed, 0.001)
        print(
            f"  {item.name:12} interval={change.ops:6d} ops "
            f"ok={interval_ok_mib:8.1f}/{interval_attempted_mib:8.1f} MiB "
            f"{mib_per_second:8.1f} MiB/s errors={change.errors:3d} | "
            f"total={current.ops:7d} ops "
            f"ok={total_ok_mib:9.1f}/{total_attempted_mib:9.1f} MiB "
            f"{total_mib_per_second:8.1f} MiB/s errors={current.errors:4d} "
            f"p50={item.percentile(0.50):7.1f}ms "
            f"p95={item.percentile(0.95):7.1f}ms",
            flush=True,
        )

    exceptions = sum((item.exceptions for item in metrics), ExceptionCounter())
    if exceptions:
        print(f"  exceptions   {dict(exceptions)}", flush=True)
    try:
        storage = await provider.get_storage_stats()
        print(f"  postgres     {storage}", flush=True)
    except Exception as exc:  # noqa: BLE001 - reporting must not stop the workload
        print(f"  postgres     stats failed: {type(exc).__name__}: {exc}", flush=True)

    return current_snapshots


async def _remove_tree(vfs: Any, path: str) -> int:
    node = await vfs.get_node_info(path)
    if node is None:
        return 0
    removed = 0
    if node.is_dir:
        for name in await vfs.ls(path):
            removed += await _remove_tree(vfs, f"{path}/{name}")
    if not await vfs.rm(path):
        raise RuntimeError(f"could not remove {path}")
    return removed + 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run parallel small/large writes and integrity-checked reads.",
        epilog="""Environment equivalents:
  VFS_PG_DSN
  VFS_HEAVY_SECONDS, VFS_HEAVY_REPORT_SECONDS
  VFS_HEAVY_SMALL_WRITERS, VFS_HEAVY_LARGE_WRITERS, VFS_HEAVY_READERS
  VFS_HEAVY_SLOTS, VFS_HEAVY_SMALL_MIN_KIB, VFS_HEAVY_SMALL_MAX_KIB
  VFS_HEAVY_LARGE_MIN_MIB, VFS_HEAVY_LARGE_MAX_MIB
  VFS_HEAVY_RANGE_RATIO, VFS_HEAVY_MAX_RANGE_KIB, VFS_HEAVY_POOL_MAX
  VFS_HEAVY_KEEP_DATA=1""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dsn", default=os.environ.get("VFS_PG_DSN", DEFAULT_DSN))
    parser.add_argument(
        "--duration",
        type=float,
        default=_env_float("VFS_HEAVY_SECONDS", 300),
        help="workload duration in seconds (default: 300)",
    )
    parser.add_argument(
        "--report-interval",
        type=float,
        default=_env_float("VFS_HEAVY_REPORT_SECONDS", 10),
    )
    parser.add_argument("--small-writers", type=int, default=_env_int("VFS_HEAVY_SMALL_WRITERS", 4))
    parser.add_argument("--large-writers", type=int, default=_env_int("VFS_HEAVY_LARGE_WRITERS", 2))
    parser.add_argument("--readers", type=int, default=_env_int("VFS_HEAVY_READERS", 8))
    parser.add_argument("--slots-per-writer", type=int, default=_env_int("VFS_HEAVY_SLOTS", 4))
    parser.add_argument("--small-min-kib", type=int, default=_env_int("VFS_HEAVY_SMALL_MIN_KIB", 1))
    parser.add_argument(
        "--small-max-kib", type=int, default=_env_int("VFS_HEAVY_SMALL_MAX_KIB", 256)
    )
    parser.add_argument(
        "--large-min-mib", type=int, default=_env_int("VFS_HEAVY_LARGE_MIN_MIB", 16)
    )
    parser.add_argument(
        "--large-max-mib", type=int, default=_env_int("VFS_HEAVY_LARGE_MAX_MIB", 64)
    )
    parser.add_argument(
        "--range-ratio",
        type=float,
        default=_env_float("VFS_HEAVY_RANGE_RATIO", 0.4),
        help="fraction of reads that are exact range reads (default: 0.4)",
    )
    parser.add_argument(
        "--max-range-kib",
        type=int,
        default=_env_int("VFS_HEAVY_MAX_RANGE_KIB", 1024),
    )
    parser.add_argument(
        "--pool-max",
        type=int,
        default=_env_int("VFS_HEAVY_POOL_MAX", 0),
        help="PostgreSQL pool size; 0 chooses workers + 2",
    )
    parser.add_argument(
        "--keep-data",
        action="store_true",
        default=os.environ.get("VFS_HEAVY_KEEP_DATA") == "1",
        help="keep the run directory instead of deleting it",
    )
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    positive = {
        "duration": args.duration,
        "report interval": args.report_interval,
        "readers": args.readers,
        "slots per writer": args.slots_per_writer,
        "small max KiB": args.small_max_kib,
        "large min MiB": args.large_min_mib,
        "large max MiB": args.large_max_mib,
        "max range KiB": args.max_range_kib,
    }
    for label, value in positive.items():
        if value <= 0:
            parser.error(f"{label} must be greater than zero")
    if args.small_writers < 0 or args.large_writers < 0:
        parser.error("writer counts cannot be negative")
    if args.small_writers + args.large_writers == 0:
        parser.error("at least one writer is required")
    if args.small_min_kib < 0 or args.small_min_kib > args.small_max_kib:
        parser.error("small size bounds are invalid")
    if args.large_min_mib > args.large_max_mib:
        parser.error("large size bounds are invalid")
    if not 0 <= args.range_ratio <= 1:
        parser.error("range ratio must be between zero and one")
    if args.pool_max < 0:
        parser.error("pool max cannot be negative")


async def run(args: argparse.Namespace) -> int:
    run_id = f"{os.getpid()}-{int(time.time())}"
    base_root = "/heavy-stress"
    root = f"{base_root}/{run_id}"
    total_workers = args.small_writers + args.large_writers + args.readers
    pool_max = args.pool_max or total_workers + 2
    vfs = AsyncVirtualFileSystem(
        "postgres",
        dsn=args.dsn,
        pool_min=min(2, pool_max),
        pool_max=pool_max,
    )

    metrics = [
        Metrics("small-write"),
        Metrics("large-write"),
        Metrics("full-read"),
        Metrics("range-read"),
    ]
    metric_by_name = {item.name: item for item in metrics}
    small_groups = _build_slots(root, "small", args.small_writers, args.slots_per_writer)
    large_groups = _build_slots(root, "large", args.large_writers, args.slots_per_writer)
    all_slots = [slot for group in small_groups + large_groups for slot in group]
    stop = asyncio.Event()
    tasks: list[asyncio.Task[Any]] = []
    initialized = False
    cleanup_error = False
    exit_code = 1

    try:
        await vfs.initialize()
        initialized = True
        provider = vfs.provider
        if provider is None:
            raise RuntimeError("VFS provider was not initialized")

        for directory in (base_root, root, f"{root}/small", f"{root}/large"):
            await _ensure_directory(vfs, directory)

        started = time.perf_counter()
        deadline = started + args.duration
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            with suppress(NotImplementedError):
                loop.add_signal_handler(signum, stop.set)

        for number, slots in enumerate(small_groups):
            tasks.append(
                asyncio.create_task(
                    writer_loop(
                        vfs,
                        kind="small",
                        worker_number=number,
                        slots=slots,
                        minimum_size=args.small_min_kib * KIB,
                        maximum_size=args.small_max_kib * KIB,
                        metrics=metric_by_name["small-write"],
                        stop=stop,
                        deadline=deadline,
                    ),
                    name=f"small-writer-{number}",
                )
            )
        for number, slots in enumerate(large_groups):
            tasks.append(
                asyncio.create_task(
                    writer_loop(
                        vfs,
                        kind="large",
                        worker_number=number,
                        slots=slots,
                        minimum_size=args.large_min_mib * MIB,
                        maximum_size=args.large_max_mib * MIB,
                        metrics=metric_by_name["large-write"],
                        stop=stop,
                        deadline=deadline,
                    ),
                    name=f"large-writer-{number}",
                )
            )
        for number in range(args.readers):
            tasks.append(
                asyncio.create_task(
                    reader_loop(
                        vfs,
                        provider,
                        worker_number=number,
                        slots=all_slots,
                        full_metrics=metric_by_name["full-read"],
                        range_metrics=metric_by_name["range-read"],
                        range_ratio=args.range_ratio,
                        maximum_range=args.max_range_kib * KIB,
                        stop=stop,
                        deadline=deadline,
                    ),
                    name=f"reader-{number}",
                )
            )

        print(
            f"=== heavy VFS stress | run={run_id} duration={args.duration:.0f}s "
            f"small_writers={args.small_writers} "
            f"({args.small_min_kib}-{args.small_max_kib} KiB) "
            f"large_writers={args.large_writers} "
            f"({args.large_min_mib}-{args.large_max_mib} MiB) "
            f"readers={args.readers} slots={len(all_slots)} pool_max={pool_max} ===",
            flush=True,
        )
        print(f"run directory: {root}", flush=True)

        previous = {item.name: item.snapshot() for item in metrics}
        previous_report = started
        while not stop.is_set():
            now = time.perf_counter()
            remaining = deadline - now
            if remaining <= 0:
                break
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=min(args.report_interval, remaining))
            now = time.perf_counter()
            if stop.is_set() or now >= deadline:
                break
            interval = now - previous_report
            previous = await print_report(
                provider,
                metrics,
                previous,
                interval_seconds=interval,
                elapsed=now - started,
                duration=args.duration,
                slots=all_slots,
                tasks=tasks,
            )
            previous_report = now

        stop.set()
        await asyncio.gather(*tasks)
        finished = time.perf_counter()
        await print_report(
            provider,
            metrics,
            previous,
            interval_seconds=max(finished - previous_report, 0.001),
            elapsed=finished - started,
            duration=args.duration,
            slots=all_slots,
            tasks=tasks,
            final=True,
        )

        total_errors = sum(item.errors for item in metrics)
        exit_code = 1 if total_errors else 0
    finally:
        stop.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if initialized:
            if args.keep_data:
                print(f"keeping run data at {root}", flush=True)
            else:
                try:
                    removed = await _remove_tree(vfs, root)
                    print(f"cleanup: removed {removed} nodes from {root}", flush=True)
                    if not await vfs.ls(base_root):
                        await vfs.rm(base_root)
                except Exception as exc:  # noqa: BLE001 - still close the provider
                    cleanup_error = True
                    print(
                        f"cleanup failed: {type(exc).__name__}: {exc}",
                        flush=True,
                    )
            await vfs.close()

        if cleanup_error:
            print("WARNING: cleanup was incomplete", flush=True)
            exit_code = 1

    return exit_code


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(parser, args)
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
