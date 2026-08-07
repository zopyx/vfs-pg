"""PostgreSQL storage provider for chuk-virtual-fs.

Schema design (per the chuk architecture discussion):

    vfs_nodes                  vfs_chunks
    ──────────────────         ──────────────────────
    node_id     uuid PK        node_id  uuid FK -> vfs_nodes
    parent_id   uuid FK        chunk_no int
    name        text           data     bytea
    is_dir      bool           PK (node_id, chunk_no)
    size        bigint
    chunk_size  integer        (persisted per file, see read_range)
    sha256      text
    metadata    jsonb

Nodes are linked via ``parent_id + name`` (proper filesystem semantics:
rename/move is a single UPDATE, no path rewriting). File content is stored
in fixed-size chunks; the chunk size is persisted per file so range reads
always use the size the file was written with, independent of the reading
provider instance's configuration. A unique index on ``(parent_id, name)``
enforces sibling uniqueness at the database level, so concurrent creates
of the same path can never produce duplicate nodes.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import posixpath
from collections.abc import AsyncIterator
from typing import Any

import psycopg
from chuk_virtual_fs.node_info import EnhancedNodeInfo
from chuk_virtual_fs.provider_base import AsyncStorageProvider
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

DEFAULT_DSN = "postgresql://vfs:vfs@localhost:5432/vfs"
DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MiB
MOVE_TOPOLOGY_LOCK_NAMESPACE = "chuk_vfs_postgres:vfs_nodes:move"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS vfs_nodes (
    node_id     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_id   uuid REFERENCES vfs_nodes(node_id) ON DELETE CASCADE,
    name        text NOT NULL,
    is_dir      boolean NOT NULL DEFAULT false,
    size        bigint NOT NULL DEFAULT 0,
    chunk_size  integer NOT NULL DEFAULT 1048576,
    mime_type   text NOT NULL DEFAULT 'application/octet-stream',
    sha256      text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    modified_at timestamptz NOT NULL DEFAULT now(),
    metadata    jsonb NOT NULL DEFAULT '{}'::jsonb
);

-- migration for pre-0.2.0 databases
ALTER TABLE vfs_nodes ADD COLUMN IF NOT EXISTS chunk_size integer NOT NULL DEFAULT 1048576;

-- one root per database
CREATE UNIQUE INDEX IF NOT EXISTS uq_vfs_nodes_root
    ON vfs_nodes ((parent_id IS NULL)) WHERE parent_id IS NULL;

-- no duplicate siblings (filesystem invariant, enforced by the database)
CREATE UNIQUE INDEX IF NOT EXISTS uq_vfs_nodes_sibling
    ON vfs_nodes (parent_id, name) WHERE parent_id IS NOT NULL;

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

# detects existing duplicate siblings before the unique index is installed
DUPLICATE_SIBLINGS_SQL = """
SELECT p.name AS parent, c.name AS name, COUNT(*) AS n
  FROM vfs_nodes c
  JOIN vfs_nodes p ON p.node_id = c.parent_id
 WHERE c.parent_id IS NOT NULL
 GROUP BY c.parent_id, c.name, p.name
HAVING COUNT(*) > 1
"""


class PostgresStorageProvider(AsyncStorageProvider):
    """Async storage provider backed by PostgreSQL.

    Either owns a connection pool (``dsn=...``) or joins an existing
    connection (``conn=...``). The latter enables atomic commits spanning
    business tables and VFS content in one transaction.

    Args:
        dsn: PostgreSQL connection string. Required unless ``conn`` is given.
        conn: an existing async connection to join (transaction-join mode).
        chunk_size: file chunk size in bytes (positive integer). The value is
            persisted per file; readers always use the writer's chunk size.
        pool_min / pool_max: connection pool size bounds.
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
        if not isinstance(chunk_size, int) or chunk_size <= 0:
            raise ValueError(f"chunk_size must be a positive integer, got {chunk_size!r}")
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
        """Create the schema if needed and make the provider usable.

        Idempotent: repeated calls on an already-initialized provider are
        no-ops. Concurrent calls on the same instance are serialized via an
        asyncio lock; concurrent calls from *different* instances are
        serialized by a PostgreSQL advisory lock around schema creation.
        """
        if self._initialized:
            return True
        async with self._init_lock():
            if self._initialized:
                return True
            if self._external_conn is not None:
                await self._ensure_schema(self._external_conn)
                self._initialized = True
                return True

            pool: AsyncConnectionPool[Any] = AsyncConnectionPool(
                self.dsn, min_size=self.pool_min, max_size=self.pool_max, open=False
            )
            assert self.dsn is not None
            self._pool = pool
            try:
                await pool.open(wait=False, timeout=10)
                # validate connectivity + schema
                async with pool.connection() as conn:
                    await self._ensure_schema(conn)
            except Exception:
                # never leave a half-open pool behind on failure
                self._pool = None
                with contextlib.suppress(Exception):
                    await pool.close()
                raise
            self._initialized = True
            return True

    async def close(self) -> None:
        """Close the connection pool. Idempotent."""
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

    @contextlib.asynccontextmanager
    async def _init_lock(self) -> AsyncIterator[None]:
        """Serialize concurrent initialize() on the same instance."""
        lock = getattr(self, "_init_lock_obj", None)
        if lock is None:
            lock = self._init_lock_obj = asyncio.Lock()
        async with lock:
            yield

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
        """Create/upgrade the schema, serialized by an advisory lock.

        Uses a *transaction-scoped* advisory lock: concurrent initialization
        cannot deadlock, and any schema error releases the lock when the
        transaction rolls back instead of masking the original error with an
        unlock failure.
        """
        in_tx = conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE
        if in_tx:
            # caller owns the transaction -> the xact lock joins it
            async with conn.cursor() as cur:
                await self._ensure_schema_locked(cur)
            return
        async with conn.transaction(), conn.cursor() as cur:
            await self._ensure_schema_locked(cur)

    async def _ensure_schema_locked(self, cur: Any) -> None:
        await cur.execute("SELECT pg_advisory_xact_lock(83710001)")
        # migration guard: never install the sibling unique index on dirty data
        # (to_regclass -> NULL instead of an error on fresh databases, which
        # would abort the surrounding transaction)
        await cur.execute("SELECT to_regclass('vfs_nodes') IS NOT NULL")
        if (await cur.fetchone())[0]:
            await cur.execute(DUPLICATE_SIBLINGS_SQL)
            dupes = await cur.fetchall()
            if dupes:
                detail = ", ".join(f"{p}/{n} (x{c})" for p, n, c in dupes)
                raise RuntimeError(
                    "duplicate sibling nodes detected; refusing to install the "
                    f"unique index (deduplicate first): {detail}"
                )
        await cur.execute(SCHEMA_SQL)

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

    async def _child_for_update(
        self, conn: AsyncConnection, parent_id: Any, name: str
    ) -> dict[str, Any] | None:
        """Return and lock a child that another transaction may have created."""
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT *
                  FROM vfs_nodes
                 WHERE parent_id = %s AND name = %s
                   FOR UPDATE
                """,
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

    async def _is_within(self, conn: AsyncConnection, node_id: Any, ancestor_id: Any) -> bool:
        """True if ``ancestor_id`` is an ancestor (or self) of ``node_id``."""
        current = node_id
        while current is not None:
            if current == ancestor_id:
                return True
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT parent_id FROM vfs_nodes WHERE node_id = %s", (current,)
                )
                row = await cur.fetchone()
            current = row[0] if row else None
        return False

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
        """Create a file or directory node. Atomic: the unique index on
        ``(parent_id, name)`` rejects duplicates even under concurrency."""
        path = self._normalize(node_info.get_path())
        if path == "/":
            return False

        async with self._acquire() as conn, self._tx(conn):
            parent_row, name = await self._resolve_parent(conn, path)
            if parent_row is None:
                return False
            if not parent_row["is_dir"]:
                return False
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                        INSERT INTO vfs_nodes (parent_id, name, is_dir, mime_type)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (parent_id, name) WHERE parent_id IS NOT NULL
                        DO NOTHING
                        RETURNING node_id
                        """,
                    (
                        parent_row["node_id"],
                        name,
                        node_info.is_dir,
                        "inode/directory"
                        if node_info.is_dir
                        else "application/octet-stream",
                    ),
                )
                return await cur.fetchone() is not None

    async def delete_node(self, path: str) -> bool:
        """Delete a file node or an *empty* directory node."""
        path = self._normalize(path)
        if path == "/":
            return False

        async with self._acquire() as conn, self._tx(conn):
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
        """Return the node's metadata or None when the path does not exist."""
        path = self._normalize(path)
        async with self._acquire() as conn:
            row = await self._resolve(conn, path)
            if row is None:
                return None
            return self._to_node_info(row, path)

    async def list_directory(self, path: str) -> list[str]:
        """List child names of a directory, sorted; [] for missing/non-dirs."""
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
        """Replace the content of an *existing* file node.

        The write is one transaction: size/sha256 metadata and all chunks are
        replaced together, so readers never observe a partial version.
        Use :meth:`write_file_atomic` to create-or-replace in a single
        transaction without a separate touch round-trip.
        """
        path = self._normalize(path)
        if isinstance(content, str):
            content = content.encode("utf-8")

        sha256 = hashlib.sha256(content).hexdigest()
        chunks = [
            content[i : i + self.chunk_size]
            for i in range(0, len(content), self.chunk_size)
        ]

        async with self._acquire() as conn, self._tx(conn):
            row = await self._resolve(conn, path)
            if row is None or row["is_dir"]:
                return False
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                        UPDATE vfs_nodes
                           SET size = %s, sha256 = %s, chunk_size = %s, modified_at = now()
                         WHERE node_id = %s
                        """,
                    (len(content), sha256, self.chunk_size, row["node_id"]),
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

    async def write_file_atomic(
        self, path: str, content: bytes, *, exclusive: bool = False
    ) -> bool:
        """Create-or-replace a file in a single transaction.

        Unlike :meth:`write_file` this also creates a missing node, so the
        caller never needs the touch-then-write sequence (which spans two
        transactions).

        Args:
            path: target path.
            content: file content (str is encoded as UTF-8).
            exclusive: when True, fail (return False) if the node already
                exists — the database uniqueness constraint guarantees this
                even under concurrency (used for fsspec ``xb`` mode).
        """
        path = self._normalize(path)
        if isinstance(content, str):
            content = content.encode("utf-8")

        sha256 = hashlib.sha256(content).hexdigest()
        chunks = [
            content[i : i + self.chunk_size]
            for i in range(0, len(content), self.chunk_size)
        ]

        async with self._acquire() as conn, self._tx(conn):
            row = await self._resolve(conn, path)
            if row is not None and row["is_dir"]:
                return False
            if row is not None and exclusive:
                return False

            if row is None:
                parent_row, name = await self._resolve_parent(conn, path)
                if parent_row is None or not parent_row["is_dir"]:
                    return False
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                            INSERT INTO vfs_nodes (parent_id, name, is_dir)
                            VALUES (%s, %s, false)
                            ON CONFLICT (parent_id, name) WHERE parent_id IS NOT NULL
                            DO NOTHING
                            RETURNING node_id
                            """,
                        (parent_row["node_id"], name),
                    )
                    inserted = await cur.fetchone()

                if inserted is not None:
                    row = {"node_id": inserted[0]}
                elif exclusive:
                    return False  # concurrent exclusive create won
                else:
                    # ON CONFLICT waits for the winning transaction. Lock its
                    # committed node before replacing content so this remains
                    # a create-or-replace operation rather than a lost race.
                    row = await self._child_for_update(
                        conn, parent_row["node_id"], name
                    )
                    if row is None or row["is_dir"]:
                        return False

            async with conn.cursor() as cur:
                await cur.execute(
                    """
                        UPDATE vfs_nodes
                           SET size = %s, sha256 = %s, chunk_size = %s, modified_at = now()
                         WHERE node_id = %s
                        """,
                    (len(content), sha256, self.chunk_size, row["node_id"]),
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
        """Read a file's complete content; None for missing paths/dirs."""
        path = self._normalize(path)
        async with self._acquire() as conn:
            row = await self._resolve(conn, path)
            if row is None:
                return None
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT n.is_dir, n.size, n.chunk_size, c.chunk_no, c.data
                      FROM vfs_nodes n
                      LEFT JOIN vfs_chunks c ON c.node_id = n.node_id
                     WHERE n.node_id = %s
                     ORDER BY c.chunk_no
                    """,
                    (row["node_id"],),
                )
                parts = await cur.fetchall()

            # Resolution only supplies the stable node id. File metadata and
            # chunks come from the same statement snapshot, so an overwrite
            # cannot combine one version's metadata with another's chunks.
            if not parts or parts[0][0]:
                return None
            size = parts[0][1]
            return b"".join(part[4] for part in parts if part[4] is not None)[:size]

    async def read_range(self, path: str, start: int, end: int) -> bytes | None:
        """Chunk-aware range read: only fetches overlapping chunks.

        Uses the chunk size the file was *written* with (persisted on the
        node), so any provider instance can range-read any file. ``start`` is
        clamped to 0, ``end`` to EOF; ``end <= start`` yields ``b""``.
        Extension method (not part of the chuk protocol) used by the fsspec
        adapter for efficient ``seek``/partial reads.
        """
        path = self._normalize(path)
        start = max(0, start)
        if end is not None and end <= start:
            return b""

        async with self._acquire() as conn:
            row = await self._resolve(conn, path)
            if row is None:
                return None
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    WITH target AS (
                        SELECT node_id,
                               is_dir,
                               size,
                               COALESCE(NULLIF(chunk_size, 0), %s) AS chunk_size,
                               %s::bigint AS range_start,
                               LEAST(COALESCE(%s::bigint, size), size) AS range_end
                          FROM vfs_nodes
                         WHERE node_id = %s
                    )
                    SELECT target.is_dir,
                           target.size,
                           target.chunk_size,
                           target.range_start,
                           target.range_end,
                           c.chunk_no,
                           c.data
                      FROM target
                      LEFT JOIN vfs_chunks c
                        ON c.node_id = target.node_id
                       AND target.range_start < target.range_end
                       AND c.chunk_no BETWEEN
                           target.range_start / target.chunk_size
                           AND (target.range_end - 1) / target.chunk_size
                     ORDER BY c.chunk_no
                    """,
                    (self.chunk_size, start, end, row["node_id"]),
                )
                parts = await cur.fetchall()

            # The node may have been removed after path resolution. Otherwise
            # the LEFT JOIN guarantees one row even for directories, empty
            # files, and ranges at/past EOF.
            if not parts or parts[0][0]:
                return None

            chunk_size = parts[0][2]
            range_start = parts[0][3]
            range_end = parts[0][4]
            if range_start >= range_end:
                return b""

            data = b"".join(part[6] for part in parts if part[6] is not None)
            first = range_start // chunk_size
            offset = range_start - first * chunk_size
            return data[offset : offset + (range_end - range_start)]

    async def exists(self, path: str) -> bool:
        """True when the path resolves to a node."""
        path = self._normalize(path)
        async with self._acquire() as conn:
            return await self._resolve(conn, path) is not None

    async def get_metadata(self, path: str) -> dict[str, Any]:
        """Return the node's jsonb metadata ({} for missing paths)."""
        path = self._normalize(path)
        async with self._acquire() as conn:
            row = await self._resolve(conn, path)
            if row is None:
                return {}
            return dict(row["metadata"] or {})

    async def set_metadata(self, path: str, metadata: dict[str, Any]) -> bool:
        """Merge metadata into the node's jsonb.

        The merge is a single atomic SQL expression (``metadata || %s``), so
        concurrent updates of *different* keys never lose data. Same-key
        concurrent updates: last commit wins. Updates ``modified_at``.
        """
        path = self._normalize(path)
        async with self._acquire() as conn, self._tx(conn):
            row = await self._resolve(conn, path)
            if row is None:
                return False
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                        UPDATE vfs_nodes
                           SET metadata = metadata || %s::jsonb, modified_at = now()
                         WHERE node_id = %s
                        """,
                    (Jsonb(metadata), row["node_id"]),
                )
            return True

    async def move_node(self, source: str, destination: str) -> bool:
        """Atomic rename/move: a single UPDATE on parent_id + name.

        Unlike the base-class copy+delete, children of a moved directory
        keep their node ids — no content is copied. Moving a directory into
        its own subtree is rejected; moving a path onto itself is an
        idempotent success. Topology validation and mutation are serialized
        per database so concurrent moves cannot validate against the same
        stale tree and create a cycle.
        """
        source = self._normalize(source)
        destination = self._normalize(destination)
        if source == "/" or destination == "/":
            return False
        if source == destination:
            return True

        async with self._acquire() as conn, self._tx(conn):
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (MOVE_TOPOLOGY_LOCK_NAMESPACE,),
                )

            # Resolve every path only after acquiring the topology lock. At
            # READ COMMITTED this observes the preceding move's committed
            # topology before revalidating the ancestry invariant.
            src_row = await self._resolve(conn, source)
            if src_row is None:
                return False
            dest_parent, dest_name = await self._resolve_parent(conn, destination)
            if dest_parent is None or not dest_parent["is_dir"]:
                return False
            if await self._child(conn, dest_parent["node_id"], dest_name):
                return False
            if src_row["is_dir"] and await self._is_within(
                conn, dest_parent["node_id"], src_row["node_id"]
            ):
                # destination lies inside the directory being moved
                return False
            async with conn.cursor() as cur:
                try:
                    # UPDATE has no ON CONFLICT equivalent. A nested psycopg
                    # transaction is a savepoint here, keeping a joined caller
                    # transaction usable if a destination creator wins the
                    # race after the existence check above.
                    async with conn.transaction():
                        await cur.execute(
                            """
                                UPDATE vfs_nodes
                                   SET parent_id = %s, name = %s, modified_at = now()
                                 WHERE node_id = %s
                                """,
                            (dest_parent["node_id"], dest_name, src_row["node_id"]),
                        )
                except psycopg.errors.UniqueViolation:
                    return False
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
        async with self._acquire() as conn, self._tx(conn):
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
                                ON CONFLICT (parent_id, name) WHERE parent_id IS NOT NULL
                                DO NOTHING
                                RETURNING node_id
                                """,
                            (parent_row["node_id"], name),
                        )
                        inserted = await cur.fetchone()
                    if inserted is None:
                        # concurrent creation of the same directory won
                        again = await self._child(conn, parent_row["node_id"], name)
                        if again is None or not again["is_dir"]:
                            return False
                current = posixpath.join(current, name)
        return True

    async def get_storage_stats(self) -> dict[str, Any]:
        """Aggregate usage statistics (dirs, files, bytes, chunks)."""
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
                assert row is not None  # aggregates always return a row
                await cur.execute("SELECT COUNT(*) FROM vfs_chunks")
                chunk_count = (await cur.fetchone())[0]
            return {
                "total_size_bytes": row[2],
                "file_count": row[1],
                "directory_count": row[0],
                "chunk_count": chunk_count,
            }

    async def cleanup(self) -> dict[str, Any]:
        """No TTL/expiry support in the current version."""
        return {"files_removed": 0, "bytes_freed": 0, "expired_removed": 0}
