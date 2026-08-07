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

import posixpath
from typing import Any

from fsspec.asyn import AsyncFileSystem
from fsspec.spec import AbstractBufferedFile

from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem

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
        fs: "ChukFileSystem",
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
            if self._append:
                existing = (
                    self.fs.cat_file(self.path)
                    if self.fs.exists(self.path)
                    else b""
                )
                content = existing + content
            self.fs.pipe_file(self.path, content)
            self._parts = []

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

    async def _provider(self, path: str) -> Any:
        provider, _local = self.vfs._get_provider_for_path(path)
        return provider

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
        return entries

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> bool:
        parts = [p for p in path.split("/") if p]
        current = ""
        for part in parts:
            current = f"{current}/{part}"
            if await self.vfs.exists(current):
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
        """Copy a file (fsspec's mv routes through copy + rm)."""
        data = await self._cat_file(path1)
        await self._pipe_file(path2, data)

    async def _mv(self, path1: str, path2: str, **kwargs: Any) -> bool:
        if not await self.vfs.exists(path1):
            raise FileNotFoundError(path1)
        return bool(await self.vfs.mv(path1, path2))

    async def _cat_file(self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any) -> bytes:
        provider = await self._provider(path)
        ranger = getattr(provider, "read_range", None)
        if ranger is not None and (start is not None or end is not None):
            data = await ranger(path, start or 0, end)
            if data is None:
                raise FileNotFoundError(path)
            return data
        data = await self.vfs.read_binary(path)
        if data is None:
            raise FileNotFoundError(path)
        if start is None and end is None:
            return data
        return data[start:end]

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        parent = posixpath.dirname(path) or "/"
        if parent != "/":
            # ensure parent chain exists
            parts = [p for p in parent.split("/") if p]
            current = ""
            for part in parts:
                current = f"{current}/{part}"
                if not await self.vfs.exists(current):
                    await self.vfs.mkdir(current)
        # vfs.write_file auto-creates the file node (touch semantics)
        ok = await self.vfs.write_file(path, value)
        if not ok:
            raise OSError(f"write failed: {path}")

    async def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | str | None = None,
        **kwargs: Any,
    ) -> ChukBufferedFile:
        if mode not in ("rb", "wb", "xb", "ab"):
            raise NotImplementedError(f"File mode not supported: {mode}")
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
