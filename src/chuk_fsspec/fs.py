"""Thin fsspec adapter over chuk-virtual-fs.

Implements the ``chuk://`` protocol on top of any chuk provider:

    fsspec.open()            fs.ls() / fs.info() / ...
         │                              │
         ▼                              ▼
    ChukBufferedFile            ChukFileSystem (AsyncFileSystem)
         │                              │
         ├── _fetch_range               └── chuk AsyncVirtualFileSystem
         └── _upload_chunk(final=True)        └── any provider
                │
                ▼
          provider.read_range / write_file

Sync users get the plain fsspec API; internals are async (psycopg pool).
"""

from __future__ import annotations

import os
import posixpath
from typing import Any

from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem
from fsspec.asyn import AsyncFileSystem, sync_wrapper
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import isfilelike

DEFAULT_BLOCK_SIZE = 5 * 1024 * 1024  # 5 MiB


class ChukBufferedFile(AbstractBufferedFile):
    """Buffered file object backed by the chuk VFS.

    Writes are buffered in memory and flushed to the VFS on close
    (``_upload_chunk(final=True)``); reads use ``_fetch_range`` which maps
    to chunk-aware range reads on the provider.
    """

    DEFAULT_BLOCK_SIZE = DEFAULT_BLOCK_SIZE

    def __init__(
        self,
        fs: ChukFileSystem,
        path: str,
        mode: str = "rb",
        block_size: int | str = "default",
        size: int | None = None,
        details: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(fs, path, mode=mode, block_size=block_size, size=size, **kwargs)
        if details is not None:
            self.details = details
        self._parts: list[bytes] = []
        self._append = "a" in mode

    def _initiate_upload(self) -> None:
        # content is written atomically on final close
        pass

    def _upload_chunk(self, final: bool = False) -> None:
        self._parts.append(self.buffer.getvalue())
        if final:
            content = b"".join(self._parts)
            self._parts = []
            if self._append:
                existing = (
                    self.fs.cat_file(self.path)
                    if self.fs.exists(self.path)
                    else b""
                )
                content = existing + content
            ok = self.fs.commit(self.path, content, exclusive="x" in self.mode)
            if not ok:
                # commit-time exclusivity enforcement: the file appeared after
                # _open() ran (or a concurrent exclusive create won)
                if "x" in self.mode:
                    raise FileExistsError(self.path)
                raise OSError(f"write failed: {self.path}")

    # reads: default AbstractBufferedFile._fetch_range calls
    # self.fs.cat_file(path, start=start, end=end) -> our async _cat_file


class ChukFileSystem(AsyncFileSystem):
    """fsspec filesystem exposing a chuk AsyncVirtualFileSystem as ``chuk://``."""

    protocol = "chuk"
    root_marker = "/"
    async_impl = True
    DEFAULT_BLOCK_SIZE = DEFAULT_BLOCK_SIZE

    def __init__(self, vfs: AsyncVirtualFileSystem | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if vfs is None:
            raise ValueError("ChukFileSystem requires a chuk AsyncVirtualFileSystem (vfs=...)")
        self.vfs = vfs

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        node = await self.vfs.get_node_info(path)
        if node is None:
            raise FileNotFoundError(path)
        import datetime

        mtime = (
            datetime.datetime.fromisoformat(node.modified_at).timestamp()
            if node.modified_at
            else None
        )
        return {
            "name": path,
            "size": node.size,
            "type": "directory" if node.is_dir else "file",
            "mtime": mtime,
            "sha256": node.sha256,
            "mode": 0o755 if node.is_dir else 0o644,
            "custom_meta": node.custom_meta,
        }

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[Any]:
        if not await self.vfs.exists(path):
            raise FileNotFoundError(path)
        entries = await self.vfs.ls(path)
        if detail:
            return [
                await self._info(posixpath.join(path, name))
                for name in entries
            ]
        # fsspec convention: non-detailed ls returns full paths
        return [posixpath.join(path, name) for name in entries]

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> bool:
        parts = [p for p in path.split("/") if p]
        if not parts:
            return True
        if not create_parents:
            parent = posixpath.dirname(path) or "/"
            if parent != "/" and not await self.vfs.exists(parent):
                raise FileNotFoundError(parent)
        current = ""
        for part in parts:
            current = f"{current}/{part}"
            if await self.vfs.exists(current):
                if not await self.vfs.is_dir(current):
                    raise FileExistsError(f"not a directory: {current}")
                continue
            if not await self.vfs.mkdir(current):
                return False
        return True

    async def _rm(self, path: str, recursive: bool = False, **kwargs: Any) -> bool:
        node = await self.vfs.get_node_info(path)
        if node is None:
            raise FileNotFoundError(path)
        if node.is_dir:
            children = await self.vfs.ls(path)
            if children and not recursive:
                raise ValueError(
                    f"Cannot delete non-empty directory: {path} (use recursive=True)"
                )
            for name in children:
                await self._rm(posixpath.join(path, name), recursive=True)
        return bool(await self.vfs.rm(path))

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file (fsspec's mv/copy route through this)."""
        node = await self.vfs.get_node_info(path1)
        if node is None:
            raise FileNotFoundError(path1)
        if node.is_dir:
            # recursive copy calls cp_file on the directory itself
            await self.vfs.mkdir(path2)
            return
        data = await self._cat_file(path1)
        await self._pipe_file(path2, data)

    async def _mv(self, path1: str, path2: str, **kwargs: Any) -> bool:
        if not await self.vfs.exists(path1):
            raise FileNotFoundError(path1)
        return bool(await self.vfs.mv(path1, path2))

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        # mount-aware: use the local path on the owning provider
        provider, local = self.vfs._get_provider_for_path(path)
        ranger = getattr(provider, "read_range", None)
        if ranger is not None and (start is not None or end is not None):
            data = await ranger(local, start or 0, end)
            if data is None:
                raise FileNotFoundError(path)
            return data
        data = await self.vfs.read_binary(path)
        if data is None:
            raise FileNotFoundError(path)
        if start is None and end is None:
            return data
        return data[start:end]

    async def _get_file(
        self,
        rpath: str,
        lpath: Any,
        chunk_size: int = DEFAULT_BLOCK_SIZE,
        callback: Any = DEFAULT_CALLBACK,
        **kwargs: Any,
    ) -> None:
        """Export one VFS entry to a local path or writable file object."""
        info = await self._info(rpath)
        if info["type"] == "directory":
            if isfilelike(lpath):
                raise IsADirectoryError(rpath)
            os.makedirs(lpath, exist_ok=True)
            return

        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than zero")

        size = info["size"]
        callback.set_size(size)

        async def export_to(output: Any) -> None:
            def write_block(data: bytes) -> None:
                written = output.write(data)
                if written is not None and written != len(data):
                    raise OSError(
                        f"short local write for {rpath}: "
                        f"expected {len(data)}, wrote {written}"
                    )
                callback.relative_update(len(data))

            provider, local = self.vfs._get_provider_for_path(rpath)
            ranger = getattr(provider, "read_range", None)
            if ranger is None:
                # Generic chuk providers may not expose range reads. Read once
                # rather than reloading the complete file for every output block.
                data = await self.vfs.read_binary(rpath)
                if data is None:
                    raise FileNotFoundError(rpath)
                if len(data) != size:
                    raise OSError(
                        f"source changed while exporting {rpath}: "
                        f"expected {size} bytes, read {len(data)}"
                    )
                write_block(data)
                return

            offset = 0
            while offset < size:
                end = min(offset + chunk_size, size)
                data = await ranger(local, offset, end)
                if data is None:
                    raise FileNotFoundError(rpath)
                expected = end - offset
                if len(data) != expected:
                    raise OSError(
                        f"short source read for {rpath}: "
                        f"expected {expected} bytes, read {len(data)}"
                    )
                write_block(data)
                offset = end

        if isfilelike(lpath):
            await export_to(lpath)
            return

        local_path = os.fspath(lpath)
        parent = os.path.dirname(local_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(local_path, "wb") as output:  # noqa: ASYNC230
            await export_to(output)

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        ok = await self._commit(path, value, exclusive=False)
        if not ok:
            raise OSError(f"write failed: {path}")

    async def _commit(self, path: str, content: bytes, *, exclusive: bool) -> bool:
        """Write content, preferring the provider's atomic create-or-replace.

        Ensures the parent chain exists (matching pipe/open semantics) and,
        when the provider supports it, creates a missing node and writes the
        content in one transaction (no touch round-trip), enforcing
        ``exclusive`` with the database's unique constraint.
        """
        parent = posixpath.dirname(path) or "/"
        if parent != "/":
            parts = [p for p in parent.split("/") if p]
            current = ""
            for part in parts:
                current = f"{current}/{part}"
                if not await self.vfs.exists(current):
                    await self.vfs.mkdir(current)
        provider, local = self.vfs._get_provider_for_path(path)
        atomic = getattr(provider, "write_file_atomic", None)
        if atomic is not None:
            return bool(await atomic(local, content, exclusive=exclusive))
        # generic chuk fallback (touch semantics at the VFS level)
        if exclusive and await self.vfs.exists(path):
            return False
        return bool(await self.vfs.write_file(path, content))

    # sync entry point for ChukBufferedFile._upload_chunk (sync context);
    # fsspec only auto-generates sync wrappers for its known async_methods
    commit = sync_wrapper(_commit)

    async def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | str | None = None,
        **kwargs: Any,
    ) -> ChukBufferedFile:
        if mode not in ("rb", "wb", "xb", "ab"):
            raise NotImplementedError(f"File mode not supported: {mode}")
        if mode == "xb" and await self.vfs.exists(path):
            raise FileExistsError(path)
        # resolve info asynchronously (never call sync wrappers from inside
        # the event loop) and hand the size to the buffered file so it skips
        # its own (sync) fs.info() lookup
        details = size = None
        if mode == "rb":
            details = await self._info(path)  # raises FileNotFoundError
            size = details["size"]
        return ChukBufferedFile(
            self,
            path,
            mode=mode,
            block_size=block_size if block_size is not None else "default",
            size=size,
            details=details,
            **kwargs,
        )
