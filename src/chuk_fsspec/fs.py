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

import contextlib
import hashlib
import io
import os
import posixpath
from typing import Any, overload

from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem
from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import isfilelike

DEFAULT_BLOCK_SIZE = 5 * 1024 * 1024  # 5 MiB


class ChukBufferedFile(AbstractBufferedFile):
    """Buffered file object backed by the chuk VFS.

    Providers exposing the staging-upload extension receive each fsspec block
    immediately; only generic providers retain blocks until close. Reads use
    ``_fetch_range`` which maps to chunk-aware range reads on the provider.
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
        self._append = "a" in mode
        provider, _local = fs.vfs._get_provider_for_path(path)
        self._streaming_upload = all(
            callable(getattr(provider, method, None))
            for method in (
                "start_upload",
                "upload_part",
                "finish_upload",
                "abort_upload",
            )
        )
        self._upload_id: Any | None = None
        self._upload_size = 0
        self._upload_sha256 = None if self._append else hashlib.sha256()
        if not self._streaming_upload:
            self._parts: list[bytes] = []

    def _initiate_upload(self) -> None:
        if not self._streaming_upload:
            return
        self._upload_id = self.fs.start_upload(
            self.path, exclusive="x" in self.mode, append=self._append
        )
        if self._upload_id is None:
            raise OSError(f"could not start upload: {self.path}")

    def _upload_chunk(self, final: bool = False) -> None:
        # AbstractBufferedFile normally calls _initiate_upload first. Keeping
        # this guard also preserves the direct/internal generic call pattern.
        if self._streaming_upload and self._upload_id is not None:
            data = self.buffer.getbuffer()
            try:
                if data:
                    ok = self.fs.upload_part(self.path, self._upload_id, data)
                    if not ok:
                        raise OSError(f"upload part failed: {self.path}")
                    if self._upload_sha256 is not None:
                        self._upload_sha256.update(data)
                    self._upload_size += len(data)
                if final:
                    if not self.autocommit:
                        return
                    self.commit()
                return
            except Exception:
                upload_id, self._upload_id = self._upload_id, None
                with contextlib.suppress(Exception):
                    self.fs.abort_upload(self.path, upload_id)
                self.closed = True
                raise

        parts = getattr(self, "_parts", None)
        if parts is None:
            parts = self._parts = []
        parts.append(self.buffer.getvalue())
        if not final:
            return
        if not self.autocommit:
            return

        self.commit()

    def commit(self) -> None:
        """Publish a closed transactional write."""
        if self._streaming_upload:
            upload_id = self._upload_id
            if upload_id is not None:
                digest = self._upload_sha256.hexdigest() if self._upload_sha256 is not None else None
                ok = self.fs.finish_upload(
                    self.path,
                    upload_id,
                    size=self._upload_size,
                    sha256=digest,
                )
                if not ok:
                    if "x" in self.mode:
                        raise FileExistsError(self.path)
                    raise OSError(f"write failed: {self.path}")
                self._upload_id = None
                return

        parts = getattr(self, "_parts", [])
        content = b"".join(parts)
        self._parts = []
        if self._append:
            existing = self.fs.cat_file(self.path) if self.fs.exists(self.path) else b""
            content = existing + content
        ok = self.fs.commit(self.path, content, exclusive="x" in self.mode)
        if not ok:
            # commit-time exclusivity enforcement: the file appeared after
            # _open() ran (or a concurrent exclusive create won)
            if "x" in self.mode:
                raise FileExistsError(self.path)
            raise OSError(f"write failed: {self.path}")

    def discard(self) -> None:
        """Discard a closed transactional write."""
        if self._streaming_upload:
            upload_id, self._upload_id = self._upload_id, None
            if upload_id is not None:
                self.fs.abort_upload(self.path, upload_id)
            return
        self._parts = []

    def __exit__(self, *args: Any) -> None:
        """Discard an in-flight staged upload when the with-block fails."""
        if args and args[0] is not None and self.mode != "rb" and self._streaming_upload:
            upload_id, self._upload_id = self._upload_id, None
            if upload_id is not None:
                with contextlib.suppress(Exception):
                    self.fs.abort_upload(self.path, upload_id)
            self.closed = True
            return None
        return super().__exit__(*args)

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

    def open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | str | None = None,
        cache_options: dict[str, Any] | None = None,
        compression: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Open a file while retaining fsspec transaction semantics."""
        path = self._strip_protocol(path)
        assert isinstance(path, str)
        if "b" not in mode:
            mode = mode.replace("t", "") + "b"
            text_kwargs = {
                key: kwargs.pop(key) for key in ("encoding", "errors", "newline") if key in kwargs
            }
            return io.TextIOWrapper(
                self.open(
                    path,
                    mode,
                    block_size=block_size,
                    cache_options=cache_options,
                    compression=compression,
                    **kwargs,
                ),
                **text_kwargs,
            )

        autocommit = kwargs.pop("autocommit", not self._intrans)
        handle = sync(
            self.loop,
            self._open,
            path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )
        if compression is not None:
            from fsspec.compression import compr
            from fsspec.core import get_compression

            handle = compr[get_compression(path, compression)](handle, mode=mode[0])
        if not autocommit and "r" not in mode:
            self.transaction.files.append(handle)
        return handle

    @classmethod
    def _normalize_path(cls, path: str) -> str:
        """Return the canonical absolute POSIX form of a VFS path."""
        if not isinstance(path, str):
            raise TypeError(f"path must be a string, got {type(path).__name__}")
        if "\x00" in path:
            raise ValueError("path must not contain NUL bytes")

        parts: list[str] = []
        for part in path.split("/"):
            if not part or part == ".":
                continue
            if part == "..":
                raise ValueError("path must not contain '..' components")
            parts.append(part)
        return f"/{'/'.join(parts)}" if parts else "/"

    @overload
    @classmethod
    def _strip_protocol(cls, path: str) -> str: ...

    @overload
    @classmethod
    def _strip_protocol(cls, path: list[str]) -> list[str]: ...

    @classmethod
    def _strip_protocol(cls, path: str | list[str]) -> str | list[str]:
        """Strip ``chuk`` URL syntax and canonicalize the VFS path.

        In a chuk URL the apparent URL authority is the first path component,
        so both ``chuk://foo/bar`` and ``chuk:///foo/bar`` address
        ``/foo/bar``. fsspec also calls this hook with path lists during bulk
        operations.
        """
        if isinstance(path, list):
            return [cls._strip_protocol(item) for item in path]
        if not isinstance(path, str):
            raise TypeError(f"path must be a string, got {type(path).__name__}")
        if path.startswith("chuk://"):
            path = path[len("chuk://") :]
        elif path.startswith("chuk::"):
            path = path[len("chuk::") :]
        return cls._normalize_path(path)

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        path = self._strip_protocol(path)
        assert isinstance(path, str)
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
        path = self._strip_protocol(path)
        assert isinstance(path, str)
        if not await self.vfs.exists(path):
            raise FileNotFoundError(path)
        entries = await self.vfs.ls(path)
        if detail:
            return [await self._info(posixpath.join(path, name)) for name in entries]
        # fsspec convention: non-detailed ls returns full paths
        return [posixpath.join(path, name) for name in entries]

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> bool:
        path = self._strip_protocol(path)
        assert isinstance(path, str)
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

    async def _rm(
        self,
        path: str | list[str],
        recursive: bool = False,
        batch_size: int | None = None,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> bool | list[bool]:
        is_single_path = isinstance(path, str)
        paths = await self._expand_path(path, recursive=recursive, maxdepth=maxdepth)
        results = [await self._rm_file(entry, **kwargs) for entry in reversed(paths)]
        return all(results) if is_single_path else results

    async def _rm_file(self, path: str, **kwargs: Any) -> bool:
        path = self._strip_protocol(path)
        assert isinstance(path, str)
        node = await self.vfs.get_node_info(path)
        if node is None:
            raise FileNotFoundError(path)
        if node.is_dir:
            children = await self.vfs.ls(path)
            if children:
                raise ValueError(f"Cannot delete non-empty directory: {path} (use recursive=True)")
        return bool(await self.vfs.rm(path))

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file (fsspec's mv/copy route through this)."""
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        assert isinstance(path1, str) and isinstance(path2, str)
        node = await self.vfs.get_node_info(path1)
        if node is None:
            raise FileNotFoundError(path1)
        if node.is_dir:
            # recursive copy calls cp_file on the directory itself
            await self._ensure_parent_directories(path2)
            created = await self.vfs.mkdir(path2)
            if not created and not (await self.vfs.exists(path2) and await self.vfs.is_dir(path2)):
                raise OSError(f"could not create target directory: {path2}")
            return
        data = await self._cat_file(path1)
        await self._pipe_file(path2, data)

    async def _mv(self, path1: str, path2: str, **kwargs: Any) -> bool:
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        assert isinstance(path1, str) and isinstance(path2, str)
        if not await self.vfs.exists(path1):
            raise FileNotFoundError(path1)
        return bool(await self.vfs.mv(path1, path2))

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        path = self._strip_protocol(path)
        assert isinstance(path, str)
        # mount-aware: use the local path on the owning provider
        provider, local = self.vfs._get_provider_for_path(path)
        ranger = getattr(provider, "read_range", None)
        if ranger is not None and (start is not None or end is not None):
            if (start is not None and start < 0) or (end is not None and end < 0):
                data = await self.vfs.read_binary(path)
                if data is None:
                    raise FileNotFoundError(path)
                return data[start:end]
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
        rpath = self._strip_protocol(rpath)
        assert isinstance(rpath, str)
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
                        f"short local write for {rpath}: expected {len(data)}, wrote {written}"
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

    async def _pipe_file(
        self, path: str, value: bytes, mode: str = "overwrite", **kwargs: Any
    ) -> None:
        path = self._strip_protocol(path)
        assert isinstance(path, str)
        if mode not in {"overwrite", "create"}:
            raise ValueError(f"unsupported pipe mode: {mode}")
        exclusive = mode == "create"
        ok = await self._commit(path, value, exclusive=exclusive)
        if not ok:
            if exclusive:
                raise FileExistsError(path)
            raise OSError(f"write failed: {path}")

    async def _ensure_parent_directories(self, path: str) -> None:
        """Create the target's missing parent chain using VFS mount semantics."""
        parent = posixpath.dirname(path) or "/"
        if parent == "/":
            return
        current = ""
        for part in (part for part in parent.split("/") if part):
            current = f"{current}/{part}"
            if await self.vfs.exists(current):
                if not await self.vfs.is_dir(current):
                    raise FileExistsError(f"not a directory: {current}")
                continue
            created = await self.vfs.mkdir(current)
            # Recursive fsspec copies may schedule the directory entry and a
            # child concurrently. A false create is success when the other
            # task won the same-directory race.
            if not created and not (
                await self.vfs.exists(current) and await self.vfs.is_dir(current)
            ):
                raise OSError(f"could not create parent directory: {current}")

    async def _start_upload(
        self, path: str, *, exclusive: bool = False, append: bool = False
    ) -> Any:
        path = self._strip_protocol(path)
        assert isinstance(path, str)
        await self._ensure_parent_directories(path)
        provider, local = self.vfs._get_provider_for_path(path)
        starter = getattr(provider, "start_upload", None)
        if not callable(starter):
            return None
        return await starter(local, exclusive=exclusive, append=append)

    async def _upload_part(self, path: str, upload_id: Any, content: bytes) -> bool:
        path = self._strip_protocol(path)
        assert isinstance(path, str)
        provider, _local = self.vfs._get_provider_for_path(path)
        uploader = getattr(provider, "upload_part", None)
        if not callable(uploader):
            return False
        return bool(await uploader(upload_id, content))

    async def _finish_upload(
        self,
        path: str,
        upload_id: Any,
        *,
        size: int,
        sha256: str | None,
    ) -> bool:
        path = self._strip_protocol(path)
        assert isinstance(path, str)
        provider, _local = self.vfs._get_provider_for_path(path)
        finisher = getattr(provider, "finish_upload", None)
        if not callable(finisher):
            return False
        return bool(await finisher(upload_id, size=size, sha256=sha256))

    async def _abort_upload(self, path: str, upload_id: Any) -> bool:
        path = self._strip_protocol(path)
        assert isinstance(path, str)
        provider, _local = self.vfs._get_provider_for_path(path)
        aborter = getattr(provider, "abort_upload", None)
        if not callable(aborter):
            return False
        return bool(await aborter(upload_id))

    async def _commit(self, path: str, content: bytes, *, exclusive: bool) -> bool:
        """Write content, preferring the provider's atomic create-or-replace.

        Ensures the parent chain exists (matching pipe/open semantics) and,
        when the provider supports it, creates a missing node and writes the
        content in one transaction (no touch round-trip), enforcing
        ``exclusive`` with the database's unique constraint.
        """
        path = self._strip_protocol(path)
        assert isinstance(path, str)
        await self._ensure_parent_directories(path)
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
    start_upload = sync_wrapper(_start_upload)
    upload_part = sync_wrapper(_upload_part)
    finish_upload = sync_wrapper(_finish_upload)
    abort_upload = sync_wrapper(_abort_upload)

    async def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | str | None = None,
        autocommit: bool = True,
        cache_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> ChukBufferedFile:
        path = self._strip_protocol(path)
        assert isinstance(path, str)
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
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    def mv(
        self,
        path1: str,
        path2: str,
        recursive: bool = False,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Move paths after canonicalizing both sides of fsspec's sync flow."""
        source = self._strip_protocol(path1)
        destination = self._strip_protocol(path2)
        assert isinstance(source, str) and isinstance(destination, str)
        return super().mv(
            source,
            destination,
            recursive=recursive,
            maxdepth=maxdepth,
            **kwargs,
        )
