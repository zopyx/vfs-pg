"""PostgreSQL storage provider for chuk-virtual-fs.

Schema design (per the chuk architecture discussion):

    vfs_nodes                  vfs_chunks
    ──────────────────         ──────────────────────
    node_id     uuid PK        node_id  uuid FK -> vfs_nodes
    parent_id   uuid FK        chunk_no int
    name        text           data     bytea
    is_dir      bool           PK (node_id, chunk_no)
    size        bigint
    sha256      text
    metadata    jsonb

Nodes are linked via ``parent_id + name`` (proper filesystem semantics:
rename/move is a single UPDATE, no path rewriting). File content is stored
in fixed-size chunks (default 1 MiB) so range reads touch only the chunks
that overlap the requested window.
"""

from __future__ import annotations

import contextlib
import hashlib
import posixpath
from collections.abc import AsyncIterator
from typing import Any

import psycopg
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from chuk_virtual_fs.node_info import EnhancedNodeInfo
from chuk_virtual_fs.provider_base import AsyncStorageProvider

DEFAULT_DSN = "postgresql://vfs:vfs@localhost:5432/vfs"
DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MiB

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS vfs_nodes (
    node_id     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_id   uuid REFERENCES vfs_nodes(node_id) ON DELETE CASCADE,
    name        text NOT NULL,
    is_dir      boolean NOT NULL DEFAULT false,
    size        bigint NOT NULL DEFAULT 0,
    mime_type   text NOT NULL DEFAULT 'application/octet-stream',
    sha256      text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    modified_at timestamptz NOT NULL DEFAULT now(),
    metadata    jsonb NOT NULL DEFAULT '{}'::jsonb
);

-- one root per database
CREATE UNIQUE INDEX IF NOT EXISTS uq_vfs_nodes_root
    ON vfs_nodes ((parent_id IS NULL)) WHERE parent_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_vfs_nodes_parent
    ON vfs_nodes (parent_id);

CREATE TABLE IF NOT EXISTS vfs_chunks (
    node_id   uuid NOT NULL REFERENCES vfs_nodes(node_id) ON DELETE CASCADE,
    chunk_no  integer NOT NULL,
    data      bytea NOT NULL,
    PRIMARY KEY (node_id, chunk_no)
);

INSERT INTO vfs_nodes (parent_id, name, is_dir)
SELECT NULL, '', true
WHERE NOT EXISTS (SELECT 1 FROM vfs_nodes WHERE parent_id IS NULL);
"""


class PostgresStorageProvider(AsyncStorageProvider):
    """Async storage provider backed by PostgreSQL.

    Either owns a connection pool (``dsn=...``) or joins an existing
    connection (``conn=...``). The latter enables atomic commits spanning
    business tables and VFS content in one transaction.
    """

    def __init__(
        self,
        dsn: str | None = None,
        conn: AsyncConnection | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        pool_min: int = 1,
        pool_max: int = 10,
    ) -> None:
        super().__init__()
        if conn is None and dsn is None:
            dsn = DEFAULT_DSN
        self.dsn = dsn
        self._external_conn = conn
        self.chunk_size = chunk_size
        self.pool_min = pool_min
        self.pool_max = pool_max
        self._pool: AsyncConnectionPool | None = None
        self._initialized = False

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> bool:
        if self._external_conn is not None:
            conn = self._external_conn
            await self._ensure_schema(conn)
            self._initialized = True
            return True

        self._pool = AsyncConnectionPool(
            self.dsn, min_size=self.pool_min, max_size=self.pool_max, open=False
        )
        await self._pool.open(wait=False, timeout=10)
        # validate connectivity
        async with self._pool.connection() as conn:
            await self._ensure_schema(conn)
        self._initialized = True
        return True

    async def close(self) -> None:
        if self._pool is not None:
            pool = self._pool
            self._pool = None
            try:
                await pool.close()
            except Exception:
                # Loop-mismatch teardown (e.g. pytest-asyncio closing loops
                # before fixture finalizers run): the pool's worker tasks are
                # bound to an already-closed loop. Fall back to closing the
                # underlying connections directly.
                for conn in list(getattr(pool, "_pool", ())):
                    with contextlib.suppress(Exception):
                        await conn.close()
        self._initialized = False

    @property
    def external_connection(self) -> AsyncConnection | None:
        """The injected connection, if this provider runs transaction-joined."""
        return self._external_conn

    # ------------------------------------------------------------------
    # connection / transaction handling
    # ------------------------------------------------------------------

    @contextlib.asynccontextmanager
    async def _acquire(self) -> AsyncIterator[AsyncConnection]:
        if self._external_conn is not None:
            yield self._external_conn
            return
        if self._pool is None:
            raise RuntimeError("provider not initialized")
        async with self._pool.connection() as conn:
            yield conn

    @contextlib.asynccontextmanager
    async def _tx(self, conn: AsyncConnection) -> AsyncIterator[None]:
        if self._external_conn is not None:
            # caller owns the transaction -> join it
            yield
            return
        async with conn.transaction():
            yield

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    async def _ensure_schema(self, conn: AsyncConnection) -> None:
        async with conn.cursor() as cur:
            # Concurrent initialize() calls (e.g. parallel processes starting
            # up) deadlock on SCHEMA_SQL: CREATE TABLE IF NOT EXISTS takes a
            # RowExclusiveLock on vfs_nodes while the root INSERT waits on the
            # other session's lock. Serialize schema init with a session-level
            # advisory lock (auto-released if the session dies).
            await cur.execute("SELECT pg_advisory_lock(83710001)")
            try:
                await cur.execute(SCHEMA_SQL)
            finally:
                await cur.execute("SELECT pg_advisory_unlock(83710001)")

    @staticmethod
    def _normalize(path: str) -> str:
        if not path or path == "/":
            return "/"
        return path.rstrip("/") or "/"

    def _split(self, path: str) -> tuple[str, str]:
        path = self._normalize(path)
        if path == "/":
            return "/", ""
        parent = posixpath.dirname(path) or "/"
        name = posixpath.basename(path)
        return parent, name

    async def _child(
        self, conn: AsyncConnection, parent_id: Any, name: str
    ) -> dict[str, Any] | None:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM vfs_nodes WHERE parent_id IS NOT DISTINCT FROM %s AND name = %s",
                (parent_id, name),
            )
            return await cur.fetchone()

    async def _root_row(self, conn: AsyncConnection) -> dict[str, Any] | None:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT * FROM vfs_nodes WHERE parent_id IS NULL")
            return await cur.fetchone()

    async def _resolve(self, conn: AsyncConnection, path: str) -> dict[str, Any] | None:
        """Resolve a path to its node row by walking parent_id links."""
        path = self._normalize(path)
        if path == "/":
            return await self._root_row(conn)

        # walk starts at the root node's id; children of the root carry
        # parent_id = root.node_id (NULL only identifies the root itself)
        parent_id: Any = None
        root = await self._root_row(conn)
        if root is None:
            return None
        parent_id = root["node_id"]
        for name in path.split("/")[1:]:
            row = await self._child(conn, parent_id, name)
            if row is None:
                return None
            parent_id = row["node_id"]
        return row

    async def _resolve_parent(
        self, conn: AsyncConnection, path: str
    ) -> tuple[dict[str, Any] | None, str]:
        parent, name = self._split(path)
        return await self._resolve(conn, parent), name

    @staticmethod
    def _to_node_info(row: dict[str, Any], path: str) -> EnhancedNodeInfo:
        parent, name = posixpath.dirname(path) or "/", posixpath.basename(path)
        if path == "/":
            parent, name = "/", ""
        return EnhancedNodeInfo(
            name=name,
            is_dir=row["is_dir"],
            parent_path=parent,
            size=row["size"],
            mime_type=row["mime_type"],
            sha256=row["sha256"],
            created_at=row["created_at"].isoformat(),
            modified_at=row["modified_at"].isoformat(),
            custom_meta=dict(row["metadata"] or {}),
            provider="postgres",
            permissions="755" if row["is_dir"] else "644",
        )

    # ------------------------------------------------------------------
    # chuk provider API
    # ------------------------------------------------------------------

    async def create_node(self, node_info: EnhancedNodeInfo) -> bool:
        path = self._normalize(node_info.get_path())
        if path == "/":
            return False

        async with self._acquire() as conn:
            async with self._tx(conn):
                parent_row, name = await self._resolve_parent(conn, path)
                if parent_row is None:
                    return False
                if not parent_row["is_dir"]:
                    return False
                if await self._child(conn, parent_row["node_id"], name):
                    return False
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO vfs_nodes (parent_id, name, is_dir, mime_type)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            parent_row["node_id"],
                            name,
                            node_info.is_dir,
                            "inode/directory" if node_info.is_dir else "application/octet-stream",
                        ),
                    )
                return True

    async def delete_node(self, path: str) -> bool:
        path = self._normalize(path)
        if path == "/":
            return False

        async with self._acquire() as conn:
            async with self._tx(conn):
                row = await self._resolve(conn, path)
                if row is None:
                    return False
                if row["is_dir"]:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "SELECT 1 FROM vfs_nodes WHERE parent_id = %s LIMIT 1",
                            (row["node_id"],),
                        )
                        if await cur.fetchone():
                            return False
                async with conn.cursor() as cur:
                    await cur.execute(
                        "DELETE FROM vfs_nodes WHERE node_id = %s", (row["node_id"],)
                    )
                return True

    async def get_node_info(self, path: str) -> EnhancedNodeInfo | None:
        path = self._normalize(path)
        async with self._acquire() as conn:
            row = await self._resolve(conn, path)
            if row is None:
                return None
            return self._to_node_info(row, path)

    async def list_directory(self, path: str) -> list[str]:
        path = self._normalize(path)
        async with self._acquire() as conn:
            row = await self._resolve(conn, path)
            if row is None or not row["is_dir"]:
                return []
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT name FROM vfs_nodes WHERE parent_id = %s ORDER BY name",
                    (row["node_id"],),
                )
                return [r[0] for r in await cur.fetchall()]

    async def write_file(self, path: str, content: bytes) -> bool:
        path = self._normalize(path)
        if isinstance(content, str):
            content = content.encode("utf-8")

        sha256 = hashlib.sha256(content).hexdigest()
        chunks = [
            content[i : i + self.chunk_size]
            for i in range(0, len(content), self.chunk_size)
        ]

        async with self._acquire() as conn:
            async with self._tx(conn):
                row = await self._resolve(conn, path)
                if row is None or row["is_dir"]:
                    return False
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE vfs_nodes
                           SET size = %s, sha256 = %s, modified_at = now()
                         WHERE node_id = %s
                        """,
                        (len(content), sha256, row["node_id"]),
                    )
                    await cur.execute(
                        "DELETE FROM vfs_chunks WHERE node_id = %s",
                        (row["node_id"],),
                    )
                    await cur.executemany(
                        "INSERT INTO vfs_chunks (node_id, chunk_no, data) VALUES (%s, %s, %s)",
                        [
                            (row["node_id"], i, chunk)
                            for i, chunk in enumerate(chunks)
                        ],
                    )
                return True

    async def read_file(self, path: str) -> bytes | None:
        path = self._normalize(path)
        async with self._acquire() as conn:
            row = await self._resolve(conn, path)
            if row is None or row["is_dir"]:
                return None
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT data FROM vfs_chunks WHERE node_id = %s ORDER BY chunk_no",
                    (row["node_id"],),
                )
                return b"".join(r[0] for r in await cur.fetchall())

    async def read_range(self, path: str, start: int, end: int) -> bytes | None:
        """Chunk-aware range read: only fetches overlapping chunks.

        Extension method (not part of the chuk protocol) used by the fsspec
        adapter for efficient ``seek``/partial reads.
        """
        path = self._normalize(path)
        start = max(0, start)
        if end is not None and end <= start:
            return b""

        async with self._acquire() as conn:
            row = await self._resolve(conn, path)
            if row is None or row["is_dir"]:
                return None

            size = row["size"]
            if start >= size:
                return b""
            if end is None or end > size:
                end = size

            first = start // self.chunk_size
            last = (end - 1) // self.chunk_size
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT chunk_no, data FROM vfs_chunks
                     WHERE node_id = %s AND chunk_no BETWEEN %s AND %s
                     ORDER BY chunk_no
                    """,
                    (row["node_id"], first, last),
                )
                parts = await cur.fetchall()

            data = b"".join(p[1] for p in parts)
            offset = start - first * self.chunk_size
            return data[offset : offset + (end - start)]

    async def exists(self, path: str) -> bool:
        path = self._normalize(path)
        async with self._acquire() as conn:
            return await self._resolve(conn, path) is not None

    async def get_metadata(self, path: str) -> dict[str, Any]:
        path = self._normalize(path)
        async with self._acquire() as conn:
            row = await self._resolve(conn, path)
            if row is None:
                return {}
            return dict(row["metadata"] or {})

    async def set_metadata(self, path: str, metadata: dict[str, Any]) -> bool:
        path = self._normalize(path)
        async with self._acquire() as conn:
            async with self._tx(conn):
                row = await self._resolve(conn, path)
                if row is None:
                    return False
                merged = dict(row["metadata"] or {})
                merged.update(metadata)
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE vfs_nodes SET metadata = %s WHERE node_id = %s",
                        (psycopg.types.json.Jsonb(merged), row["node_id"]),
                    )
                return True

    async def move_node(self, source: str, destination: str) -> bool:
        """Atomic rename/move: a single UPDATE on parent_id + name.

        Unlike the base-class copy+delete, children of a moved directory
        keep their node ids — no content is copied.
        """
        source = self._normalize(source)
        destination = self._normalize(destination)
        if source == "/" or destination == "/":
            return False

        async with self._acquire() as conn:
            async with self._tx(conn):
                src_row = await self._resolve(conn, source)
                if src_row is None:
                    return False
                dest_parent, dest_name = await self._resolve_parent(conn, destination)
                if dest_parent is None or not dest_parent["is_dir"]:
                    return False
                if await self._child(conn, dest_parent["node_id"], dest_name):
                    return False
                if source == destination:
                    return True
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE vfs_nodes
                           SET parent_id = %s, name = %s, modified_at = now()
                         WHERE node_id = %s
                        """,
                        (dest_parent["node_id"], dest_name, src_row["node_id"]),
                    )
                return True

    async def create_directory(
        self, path: str, mode: int = 0o755, owner_id: int = 1000, group_id: int = 1000
    ) -> bool:
        """Create a directory, creating missing parents (idempotent)."""
        path = self._normalize(path)
        if path == "/":
            return True

        parts = path.split("/")[1:]
        current = "/"
        async with self._acquire() as conn:
            async with self._tx(conn):
                for name in parts:
                    parent_row = await self._resolve(conn, current)
                    if parent_row is None:
                        return False
                    child = await self._child(conn, parent_row["node_id"], name)
                    if child is not None:
                        if not child["is_dir"]:
                            return False
                    else:
                        async with conn.cursor() as cur:
                            await cur.execute(
                                """
                                INSERT INTO vfs_nodes (parent_id, name, is_dir, mime_type)
                                VALUES (%s, %s, true, 'inode/directory')
                                """,
                                (parent_row["node_id"], name),
                            )
                    current = posixpath.join(current, name)
        return True

    async def get_storage_stats(self) -> dict[str, Any]:
        async with self._acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE is_dir)      AS dirs,
                        COUNT(*) FILTER (WHERE NOT is_dir)   AS files,
                        COALESCE(SUM(size) FILTER (WHERE NOT is_dir), 0) AS total_bytes
                    FROM vfs_nodes
                    """
                )
                row = await cur.fetchone()
                await cur.execute(
                    "SELECT COUNT(*) FROM vfs_chunks"
                )
                chunk_count = (await cur.fetchone())[0]
            return {
                "total_size_bytes": row[2],
                "file_count": row[1],
                "directory_count": row[0],
                "chunk_count": chunk_count,
            }

    async def cleanup(self) -> dict[str, Any]:
        # no TTL/expiry support in the prototype
        return {"files_removed": 0, "bytes_freed": 0, "expired_removed": 0}
