"""PostgreSQL storage provider for chuk-virtual-fs.

Schema design (per the chuk architecture discussion):

    vfs_nodes                  vfs_chunks
    ──────────────────         ──────────────────────
    node_id     uuid PK        node_id  uuid FK -> vfs_nodes
    filesystem_id text
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
provider instance's configuration. A unique index on
``(filesystem_id, parent_id, name)`` enforces sibling uniqueness at the
database level, so concurrent creates of the same path can never produce
duplicate nodes within a namespace.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import posixpath
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

import psycopg
from chuk_virtual_fs.node_info import EnhancedNodeInfo
from chuk_virtual_fs.provider_base import AsyncStorageProvider
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

DEFAULT_DSN = "postgresql://vfs:vfs@localhost:5432/vfs"
DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MiB
UPLOAD_TTL_SECONDS = 24 * 60 * 60  # abandoned staging uploads live for at most one day
MOVE_TOPOLOGY_LOCK_NAMESPACE = "chuk_vfs_postgres:vfs_nodes:move"

SCHEMA_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS vfs_nodes (
    node_id     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    filesystem_id text NOT NULL DEFAULT 'default',
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
-- migration for databases created before filesystem namespaces
ALTER TABLE vfs_nodes ADD COLUMN IF NOT EXISTS filesystem_id text NOT NULL DEFAULT 'default';

CREATE TABLE IF NOT EXISTS vfs_chunks (
    node_id   uuid NOT NULL REFERENCES vfs_nodes(node_id) ON DELETE CASCADE,
    chunk_no  integer NOT NULL,
    data      bytea NOT NULL,
    PRIMARY KEY (node_id, chunk_no)
);

CREATE TABLE IF NOT EXISTS vfs_uploads (
    upload_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    root_id      uuid NOT NULL REFERENCES vfs_nodes(node_id) ON DELETE CASCADE,
    target_path  text NOT NULL,
    exclusive    boolean NOT NULL DEFAULT false,
    append       boolean NOT NULL DEFAULT false,
    chunk_size   integer NOT NULL CHECK (chunk_size > 0),
    size         bigint NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vfs_uploads_created_at
    ON vfs_uploads (created_at);

CREATE TABLE IF NOT EXISTS vfs_upload_chunks (
    upload_id   uuid NOT NULL REFERENCES vfs_uploads(upload_id) ON DELETE CASCADE,
    chunk_no    integer NOT NULL,
    data        bytea NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (upload_id, chunk_no)
);
"""

# The two named indexes existed before filesystem namespaces. PostgreSQL's
# ``IF NOT EXISTS`` only checks the name, so replace an old definition once
# while preserving the OID of an already-correct index on later initialize().
INDEX_MIGRATION_SQL = """
DO $migration$
BEGIN
    IF to_regclass('uq_vfs_nodes_root') IS NOT NULL
       AND pg_get_indexdef(to_regclass('uq_vfs_nodes_root'))
           NOT LIKE '%(filesystem_id) WHERE (parent_id IS NULL)%'
    THEN
        DROP INDEX uq_vfs_nodes_root;
    END IF;

    IF to_regclass('uq_vfs_nodes_sibling') IS NOT NULL
       AND pg_get_indexdef(to_regclass('uq_vfs_nodes_sibling'))
           NOT LIKE '%(filesystem_id, parent_id, name) WHERE (parent_id IS NOT NULL)%'
    THEN
        DROP INDEX uq_vfs_nodes_sibling;
    END IF;
END
$migration$;
"""

SCHEMA_INDEXES_SQL = """
-- one root per filesystem namespace
CREATE UNIQUE INDEX IF NOT EXISTS uq_vfs_nodes_root
    ON vfs_nodes (filesystem_id) WHERE parent_id IS NULL;

-- no duplicate siblings within a filesystem namespace
CREATE UNIQUE INDEX IF NOT EXISTS uq_vfs_nodes_sibling
    ON vfs_nodes (filesystem_id, parent_id, name) WHERE parent_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_vfs_nodes_parent
    ON vfs_nodes (parent_id);
"""

# Public compatibility export: callers that execute the schema constant still
# receive the historical default root. Provider initialization executes its
# root statement separately because the active namespace must be parameterized.
DEFAULT_ROOT_SQL = """
INSERT INTO vfs_nodes (filesystem_id, parent_id, name, is_dir)
VALUES ('default', NULL, '', true)
ON CONFLICT (filesystem_id) WHERE parent_id IS NULL DO NOTHING;
"""
SCHEMA_SQL = SCHEMA_TABLES_SQL + INDEX_MIGRATION_SQL + SCHEMA_INDEXES_SQL + DEFAULT_ROOT_SQL

# detects existing duplicate siblings before the unique index is installed
DUPLICATE_SIBLINGS_SQL = """
SELECT c.filesystem_id, p.name AS parent, c.name AS name, COUNT(*) AS n
  FROM vfs_nodes c
  JOIN vfs_nodes p ON p.node_id = c.parent_id
 WHERE c.parent_id IS NOT NULL
 GROUP BY c.filesystem_id, c.parent_id, c.name, p.name
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
        filesystem_id: namespace stored with every node. Defaults to
            ``VFS_PG_FILESYSTEM_ID`` or ``"default"`` when the environment
            variable is unset.
        pool_min / pool_max: connection pool size bounds.
    """

    def __init__(
        self,
        dsn: str | None = None,
        conn: AsyncConnection | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        filesystem_id: str | None = None,
        pool_min: int = 1,
        pool_max: int = 10,
    ) -> None:
        super().__init__()
        if conn is None and dsn is None:
            dsn = DEFAULT_DSN
        if not isinstance(chunk_size, int) or chunk_size <= 0:
            raise ValueError(f"chunk_size must be a positive integer, got {chunk_size!r}")
        if filesystem_id is None:
            filesystem_id = os.environ.get("VFS_PG_FILESYSTEM_ID", "default")
        if not isinstance(filesystem_id, str) or not filesystem_id.strip():
            raise ValueError("filesystem_id must be a non-empty string")
        self.dsn = dsn
        self._external_conn = conn
        self.chunk_size = chunk_size
        self.filesystem_id = filesystem_id
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
        """Make the provider unusable and close its owned pool.

        The injected connection, when present, remains owned by the caller.
        Lifecycle changes share the initialization lock so a concurrent
        initialize/close pair has the state dictated by lock acquisition
        order and cannot leave an initialized provider without a pool.
        """
        async with self._init_lock():
            pool = self._pool
            self._pool = None
            self._initialized = False
            if pool is not None:
                # Pool shutdown is best-effort so close() remains idempotent,
                # including during event-loop teardown. Do not reach into
                # psycopg_pool internals: connection ownership belongs to its
                # public close() API.
                with contextlib.suppress(Exception):
                    await pool.close()

    @contextlib.asynccontextmanager
    async def _init_lock(self) -> AsyncIterator[None]:
        """Serialize lifecycle changes on the same instance."""
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

    def _require_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError("provider not initialized")

    @contextlib.asynccontextmanager
    async def _acquire(self) -> AsyncIterator[AsyncConnection]:
        self._require_initialized()
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
        await cur.execute(SCHEMA_TABLES_SQL)
        await cur.execute(INDEX_MIGRATION_SQL)
        # migration guard: never install the sibling unique index on dirty data
        await cur.execute(DUPLICATE_SIBLINGS_SQL)
        dupes = await cur.fetchall()
        if dupes:
            detail = ", ".join(f"{fs}:{p}/{n} (x{c})" for fs, p, n, c in dupes)
            raise RuntimeError(
                "duplicate sibling nodes detected; refusing to install the "
                f"unique index (deduplicate first): {detail}"
            )
        await cur.execute(SCHEMA_INDEXES_SQL)
        await cur.execute(
            """
            INSERT INTO vfs_nodes (filesystem_id, parent_id, name, is_dir)
            VALUES (%s, NULL, '', true)
            ON CONFLICT (filesystem_id) WHERE parent_id IS NULL DO NOTHING
            """,
            (self.filesystem_id,),
        )

    @staticmethod
    def _normalize(path: str) -> str:
        """Return the canonical absolute POSIX form of an internal VFS path.

        Relative provider paths are rooted at ``/``. Empty components and
        ``.`` are harmless spelling differences, while ``..`` is rejected
        rather than resolved so callers can never use normalization to cross a
        provider or mount boundary.
        """
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
                """
                SELECT * FROM vfs_nodes
                 WHERE filesystem_id = %s
                   AND parent_id IS NOT DISTINCT FROM %s
                   AND name = %s
                """,
                (self.filesystem_id, parent_id, name),
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
                 WHERE filesystem_id = %s AND parent_id = %s AND name = %s
                   FOR UPDATE
                """,
                (self.filesystem_id, parent_id, name),
            )
            return await cur.fetchone()

    async def _root_row(self, conn: AsyncConnection) -> dict[str, Any] | None:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM vfs_nodes
                 WHERE filesystem_id = %s AND parent_id IS NULL
                """,
                (self.filesystem_id,),
            )
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
                    """
                    SELECT parent_id FROM vfs_nodes
                     WHERE filesystem_id = %s AND node_id = %s
                    """,
                    (self.filesystem_id, current),
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

    @staticmethod
    def _coerce_upload_id(upload_id: UUID | str) -> UUID:
        """Validate upload ids before sending them to PostgreSQL."""
        if isinstance(upload_id, UUID):
            return upload_id
        if not isinstance(upload_id, str):
            raise TypeError("upload_id must be a UUID or UUID string")
        return UUID(upload_id)

    async def _lock_node(self, conn: AsyncConnection, node_id: Any) -> dict[str, Any] | None:
        """Fetch a node while holding its row lock until transaction end."""
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM vfs_nodes
                 WHERE filesystem_id = %s AND node_id = %s
                   FOR UPDATE
                """,
                (self.filesystem_id, node_id),
            )
            return await cur.fetchone()

    async def _lock_upload(self, conn: AsyncConnection, upload_id: UUID) -> dict[str, Any] | None:
        """Lock an upload only when its root belongs to this filesystem."""
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT u.*
                  FROM vfs_uploads u
                  JOIN vfs_nodes root ON root.node_id = u.root_id
                 WHERE u.upload_id = %s
                   AND root.filesystem_id = %s
                   AND root.parent_id IS NULL
                   FOR UPDATE OF u
                """,
                (upload_id, self.filesystem_id),
            )
            return await cur.fetchone()

    async def _delete_upload(self, conn: AsyncConnection, upload_id: UUID) -> bool:
        """Delete an upload only when its root belongs to this filesystem."""
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM vfs_uploads u
                 USING vfs_nodes root
                 WHERE u.upload_id = %s
                   AND root.node_id = u.root_id
                   AND root.filesystem_id = %s
                   AND root.parent_id IS NULL
                RETURNING u.upload_id
                """,
                (upload_id, self.filesystem_id),
            )
            return await cur.fetchone() is not None

    async def _insert_content_chunks(self, cur: Any, node_id: Any, content: bytes) -> None:
        """Insert content without constructing a second full chunk/row list."""
        view = memoryview(content)
        rows = (
            (node_id, chunk_no, view[offset : offset + self.chunk_size])
            for chunk_no, offset in enumerate(range(0, len(view), self.chunk_size))
        )
        await cur.executemany(
            "INSERT INTO vfs_chunks (node_id, chunk_no, data) VALUES (%s, %s, %s)",
            rows,
        )

    async def _iter_chunk_data(
        self,
        conn: AsyncConnection,
        table: str,
        owner_column: str,
        owner_id: Any,
    ) -> AsyncIterator[bytes]:
        """Yield ordered chunks one row at a time with bounded client memory.

        The table and owner column are selected only by internal callers. A
        one-row keyset query avoids psycopg materializing an entire result set
        and also works for transaction-joined/autocommit connections where a
        named server-side cursor may not be available.
        """
        if (table, owner_column) not in {
            ("vfs_chunks", "node_id"),
            ("vfs_upload_chunks", "upload_id"),
        }:
            raise ValueError("unsupported chunk source")
        chunk_no = -1
        while True:
            async with conn.cursor() as cur:
                if table == "vfs_chunks":
                    await cur.execute(
                        """
                        SELECT c.chunk_no, c.data
                          FROM vfs_chunks c
                          JOIN vfs_nodes n ON n.node_id = c.node_id
                         WHERE n.filesystem_id = %s
                           AND c.node_id = %s AND c.chunk_no > %s
                         ORDER BY c.chunk_no
                         LIMIT 1
                        """,
                        (self.filesystem_id, owner_id, chunk_no),
                    )
                else:
                    await cur.execute(
                        """
                        SELECT c.chunk_no, c.data
                          FROM vfs_upload_chunks c
                          JOIN vfs_uploads u ON u.upload_id = c.upload_id
                          JOIN vfs_nodes root ON root.node_id = u.root_id
                         WHERE root.filesystem_id = %s
                           AND c.upload_id = %s AND c.chunk_no > %s
                         ORDER BY c.chunk_no
                         LIMIT 1
                        """,
                        (self.filesystem_id, owner_id, chunk_no),
                    )
                row = await cur.fetchone()
            if row is None:
                return
            chunk_no = row[0]
            yield bytes(row[1])

    async def _staged_sha256(self, conn: AsyncConnection, upload_id: UUID) -> str:
        digest = hashlib.sha256()
        async for chunk in self._iter_chunk_data(conn, "vfs_upload_chunks", "upload_id", upload_id):
            digest.update(chunk)
        return digest.hexdigest()

    async def _append_sha256(
        self,
        conn: AsyncConnection,
        node_id: Any,
        existing_size: int,
        upload_id: UUID,
        staged_size: int,
    ) -> str:
        """Hash existing and appended bytes without assembling the file."""
        digest = hashlib.sha256()
        remaining = existing_size
        async for chunk in self._iter_chunk_data(conn, "vfs_chunks", "node_id", node_id):
            if remaining <= 0:
                break
            logical = chunk[:remaining]
            digest.update(logical)
            remaining -= len(logical)
        if remaining:
            raise RuntimeError("existing file chunks are shorter than node size")

        remaining = staged_size
        async for chunk in self._iter_chunk_data(conn, "vfs_upload_chunks", "upload_id", upload_id):
            if remaining <= 0:
                break
            logical = chunk[:remaining]
            digest.update(logical)
            remaining -= len(logical)
        if remaining:
            raise RuntimeError("staged chunks are shorter than upload size")
        return digest.hexdigest()

    async def _append_staged_chunks(
        self,
        conn: AsyncConnection,
        node: dict[str, Any],
        upload: dict[str, Any],
    ) -> None:
        """Append staged bytes while retaining at most two chunks in memory."""
        staged_size = int(upload["size"])
        if staged_size == 0:
            return

        node_id = node["node_id"]
        existing_size = int(node["size"])
        chunk_size = int(node["chunk_size"] or self.chunk_size)
        upload_id = upload["upload_id"]
        partial = existing_size % chunk_size
        output_no = existing_size // chunk_size
        replace_existing = False
        pending = bytearray()

        if partial:
            output_no = existing_size // chunk_size
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT c.data
                      FROM vfs_chunks c
                      JOIN vfs_nodes n ON n.node_id = c.node_id
                     WHERE n.filesystem_id = %s
                       AND c.node_id = %s AND c.chunk_no = %s
                    """,
                    (self.filesystem_id, node_id, output_no),
                )
                row = await cur.fetchone()
            if row is None or len(row[0]) < partial:
                raise RuntimeError("existing file is missing its final chunk")
            pending.extend(bytes(row[0])[:partial])
            replace_existing = True

        async def write_output(data: bytes) -> None:
            nonlocal output_no, replace_existing
            async with conn.cursor() as cur:
                if replace_existing:
                    await cur.execute(
                        """
                        UPDATE vfs_chunks SET data = %s
                         WHERE node_id = %s AND chunk_no = %s
                        """,
                        (data, node_id, output_no),
                    )
                    replace_existing = False
                else:
                    await cur.execute(
                        """
                        INSERT INTO vfs_chunks (node_id, chunk_no, data)
                        VALUES (%s, %s, %s)
                        """,
                        (node_id, output_no, data),
                    )
            output_no += 1

        async for chunk in self._iter_chunk_data(conn, "vfs_upload_chunks", "upload_id", upload_id):
            pending.extend(chunk)
            while len(pending) >= chunk_size:
                await write_output(bytes(pending[:chunk_size]))
                del pending[:chunk_size]

        if pending:
            await write_output(bytes(pending))

    # ------------------------------------------------------------------
    # chuk provider API
    # ------------------------------------------------------------------

    async def create_node(self, node_info: EnhancedNodeInfo) -> bool:
        """Create a file or directory node. Atomic: the unique index on
        ``(parent_id, name)`` rejects duplicates even under concurrency."""
        self._require_initialized()
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
                        INSERT INTO vfs_nodes
                            (filesystem_id, parent_id, name, is_dir, mime_type)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (filesystem_id, parent_id, name)
                            WHERE parent_id IS NOT NULL
                        DO NOTHING
                        RETURNING node_id
                        """,
                    (
                        self.filesystem_id,
                        parent_row["node_id"],
                        name,
                        node_info.is_dir,
                        "inode/directory" if node_info.is_dir else "application/octet-stream",
                    ),
                )
                return await cur.fetchone() is not None

    async def delete_node(self, path: str) -> bool:
        """Delete a file node or an *empty* directory node."""
        self._require_initialized()
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
                        """
                        SELECT 1 FROM vfs_nodes
                         WHERE filesystem_id = %s AND parent_id = %s LIMIT 1
                        """,
                        (self.filesystem_id, row["node_id"]),
                    )
                    if await cur.fetchone():
                        return False
            async with conn.cursor() as cur:
                await cur.execute(
                    "DELETE FROM vfs_nodes WHERE filesystem_id = %s AND node_id = %s",
                    (self.filesystem_id, row["node_id"]),
                )
            return True

    async def get_node_info(self, path: str) -> EnhancedNodeInfo | None:
        """Return the node's metadata or None when the path does not exist."""
        self._require_initialized()
        path = self._normalize(path)
        async with self._acquire() as conn:
            row = await self._resolve(conn, path)
            if row is None:
                return None
            return self._to_node_info(row, path)

    async def list_directory(self, path: str) -> list[str]:
        """List child names of a directory, sorted; [] for missing/non-dirs."""
        self._require_initialized()
        path = self._normalize(path)
        async with self._acquire() as conn:
            row = await self._resolve(conn, path)
            if row is None or not row["is_dir"]:
                return []
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT name FROM vfs_nodes
                     WHERE filesystem_id = %s AND parent_id = %s ORDER BY name
                    """,
                    (self.filesystem_id, row["node_id"]),
                )
                return [r[0] for r in await cur.fetchall()]

    async def write_file(self, path: str, content: bytes) -> bool:
        """Replace the content of an *existing* file node.

        The write is one transaction: size/sha256 metadata and all chunks are
        replaced together, so readers never observe a partial version.
        Use :meth:`write_file_atomic` to create-or-replace in a single
        transaction without a separate touch round-trip.
        """
        self._require_initialized()
        path = self._normalize(path)
        if isinstance(content, str):
            content = content.encode("utf-8")

        sha256 = hashlib.sha256(content).hexdigest()

        async with self._acquire() as conn, self._tx(conn):
            row = await self._resolve(conn, path)
            if row is None or row["is_dir"]:
                return False
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                        UPDATE vfs_nodes
                           SET size = %s, sha256 = %s, chunk_size = %s, modified_at = now()
                         WHERE filesystem_id = %s AND node_id = %s
                        """,
                    (
                        len(content),
                        sha256,
                        self.chunk_size,
                        self.filesystem_id,
                        row["node_id"],
                    ),
                )
                await cur.execute(
                    "DELETE FROM vfs_chunks WHERE node_id = %s",
                    (row["node_id"],),
                )
                await self._insert_content_chunks(cur, row["node_id"], content)
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
        self._require_initialized()
        path = self._normalize(path)
        if isinstance(content, str):
            content = content.encode("utf-8")

        sha256 = hashlib.sha256(content).hexdigest()

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
                            INSERT INTO vfs_nodes
                                (filesystem_id, parent_id, name, is_dir)
                            VALUES (%s, %s, %s, false)
                            ON CONFLICT (filesystem_id, parent_id, name)
                                WHERE parent_id IS NOT NULL
                            DO NOTHING
                            RETURNING node_id
                            """,
                        (self.filesystem_id, parent_row["node_id"], name),
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
                    row = await self._child_for_update(conn, parent_row["node_id"], name)
                    if row is None or row["is_dir"]:
                        return False

            async with conn.cursor() as cur:
                await cur.execute(
                    """
                        UPDATE vfs_nodes
                           SET size = %s, sha256 = %s, chunk_size = %s, modified_at = now()
                         WHERE filesystem_id = %s AND node_id = %s
                        """,
                    (
                        len(content),
                        sha256,
                        self.chunk_size,
                        self.filesystem_id,
                        row["node_id"],
                    ),
                )
                await cur.execute(
                    "DELETE FROM vfs_chunks WHERE node_id = %s",
                    (row["node_id"],),
                )
                await self._insert_content_chunks(cur, row["node_id"], content)
            return True

    async def start_upload(self, path: str, exclusive: bool = False, append: bool = False) -> UUID:
        """Create an invisible, durable staging upload.

        Starting and adding parts never creates or modifies the target node.
        The target is only resolved and changed by :meth:`finish_upload`.
        Staging rows abandoned for more than :data:`UPLOAD_TTL_SECONDS` are
        removed by :meth:`cleanup`.
        """
        self._require_initialized()
        path = self._normalize(path)
        if path == "/":
            raise ValueError("cannot upload file content to the root directory")
        if exclusive and append:
            raise ValueError("exclusive and append uploads are mutually exclusive")

        async with self._acquire() as conn, self._tx(conn):
            root = await self._root_row(conn)
            if root is None:
                raise RuntimeError("VFS root node is missing")
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO vfs_uploads
                        (root_id, target_path, exclusive, append, chunk_size)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING upload_id
                    """,
                    (root["node_id"], path, exclusive, append, self.chunk_size),
                )
                row = await cur.fetchone()
            assert row is not None
            return row[0]

    async def upload_part(self, upload_id: UUID | str, content: bytes) -> bool:
        """Persist one upload block, carrying partial provider chunks forward."""
        self._require_initialized()
        upload_uuid = self._coerce_upload_id(upload_id)
        try:
            return await self._stage_upload_part(upload_uuid, content)
        except Exception:
            with contextlib.suppress(Exception):
                await self.abort_upload(upload_uuid)
            raise

    async def _stage_upload_part(self, upload_uuid: UUID, content: bytes) -> bool:
        if not isinstance(content, (bytes, bytearray, memoryview)):
            raise TypeError("upload content must be bytes-like")
        view = memoryview(content)

        async with self._acquire() as conn, self._tx(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT u.*
                      FROM vfs_uploads u
                      JOIN vfs_nodes root ON root.node_id = u.root_id
                     WHERE u.upload_id = %s
                       AND root.filesystem_id = %s
                       AND root.parent_id IS NULL
                       FOR UPDATE OF u
                    """,
                    (upload_uuid, self.filesystem_id),
                )
                upload = await cur.fetchone()
                if upload is None:
                    return False
                if not view:
                    return True

                chunk_size = int(upload["chunk_size"])
                original_size = int(upload["size"])
                chunk_no = original_size // chunk_size
                offset = 0
                partial = original_size % chunk_size

                if partial:
                    await cur.execute(
                        """
                        SELECT data FROM vfs_upload_chunks
                         WHERE upload_id = %s AND chunk_no = %s
                        """,
                        (upload_uuid, chunk_no),
                    )
                    tail = await cur.fetchone()
                    if tail is None or len(tail["data"]) != partial:
                        raise RuntimeError("staged upload is missing its partial chunk")
                    take = min(chunk_size - partial, len(view))
                    merged = bytes(tail["data"]) + bytes(view[:take])
                    await cur.execute(
                        """
                        UPDATE vfs_upload_chunks SET data = %s
                         WHERE upload_id = %s AND chunk_no = %s
                        """,
                        (merged, upload_uuid, chunk_no),
                    )
                    offset = take
                    if len(merged) == chunk_size:
                        chunk_no += 1

                while offset < len(view):
                    end = min(offset + chunk_size, len(view))
                    await cur.execute(
                        """
                        INSERT INTO vfs_upload_chunks (upload_id, chunk_no, data)
                        VALUES (%s, %s, %s)
                        """,
                        (upload_uuid, chunk_no, bytes(view[offset:end])),
                    )
                    chunk_no += 1
                    offset = end

                await cur.execute(
                    """
                    UPDATE vfs_uploads u
                       SET size = %s
                      FROM vfs_nodes root
                     WHERE u.upload_id = %s
                       AND root.node_id = u.root_id
                       AND root.filesystem_id = %s
                       AND root.parent_id IS NULL
                    """,
                    (original_size + len(view), upload_uuid, self.filesystem_id),
                )
            return True

    async def finish_upload(
        self,
        upload_id: UUID | str,
        size: int | None = None,
        sha256: str | None = None,
    ) -> bool:
        """Atomically publish a staged create, overwrite, or append.

        Overwrite/create publication copies the staged rows with a database-
        side ``INSERT ... SELECT``. Append publication locks the target node
        first, so concurrent appenders serialize and each suffix is preserved
        exactly once in row-lock acquisition order.

        A supplied ``size`` must match the staged byte count. For overwrite
        uploads fsspec supplies its incrementally computed SHA-256; direct API
        callers may omit it and the provider scans staged chunks one at a time.
        Append hashes always scan both existing and staged chunks because hash
        states cannot be combined from two final digests.
        """
        self._require_initialized()
        upload_uuid = self._coerce_upload_id(upload_id)
        try:
            if size is not None and (
                not isinstance(size, int) or isinstance(size, bool) or size < 0
            ):
                raise ValueError("upload size must be a non-negative integer")
            if sha256 is not None:
                if not isinstance(sha256, str) or len(sha256) != 64:
                    raise ValueError("sha256 must be a 64-character hexadecimal string")
                try:
                    int(sha256, 16)
                except ValueError as exc:
                    raise ValueError("sha256 must be a 64-character hexadecimal string") from exc
                sha256 = sha256.lower()

            async with self._acquire() as conn, self._tx(conn):
                upload = await self._lock_upload(conn, upload_uuid)
                if upload is None:
                    return False

                async def discard() -> None:
                    await self._delete_upload(conn, upload_uuid)

                staged_size = int(upload["size"])
                if size is not None and size != staged_size:
                    await discard()
                    return False

                path = upload["target_path"]
                row = await self._resolve(conn, path)
                if row is not None:
                    row = await self._lock_node(conn, row["node_id"])
                if row is not None and row["is_dir"]:
                    await discard()
                    return False
                if row is not None and upload["exclusive"]:
                    await discard()
                    return False

                created = False
                if row is None:
                    parent_row, name = await self._resolve_parent(conn, path)
                    if parent_row is None or not parent_row["is_dir"]:
                        await discard()
                        return False
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            INSERT INTO vfs_nodes
                                (filesystem_id, parent_id, name, is_dir)
                            VALUES (%s, %s, %s, false)
                            ON CONFLICT (filesystem_id, parent_id, name)
                                WHERE parent_id IS NOT NULL
                            DO NOTHING
                            RETURNING node_id
                            """,
                            (self.filesystem_id, parent_row["node_id"], name),
                        )
                        inserted = await cur.fetchone()
                    if inserted is not None:
                        row = {"node_id": inserted[0]}
                        created = True
                    elif upload["exclusive"]:
                        await discard()
                        return False
                    else:
                        row = await self._child_for_update(conn, parent_row["node_id"], name)
                        if row is None or row["is_dir"]:
                            await discard()
                            return False

                assert row is not None
                node_id = row["node_id"]
                if upload["append"] and not created:
                    existing_size = int(row["size"])
                    final_hash = await self._append_sha256(
                        conn, node_id, existing_size, upload_uuid, staged_size
                    )
                    await self._append_staged_chunks(conn, row, upload)
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            UPDATE vfs_nodes
                               SET size = %s, sha256 = %s, modified_at = now()
                             WHERE filesystem_id = %s AND node_id = %s
                            """,
                            (
                                existing_size + staged_size,
                                final_hash,
                                self.filesystem_id,
                                node_id,
                            ),
                        )
                else:
                    final_hash = sha256 or await self._staged_sha256(conn, upload_uuid)
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            UPDATE vfs_nodes
                               SET size = %s, sha256 = %s, chunk_size = %s,
                                   modified_at = now()
                             WHERE filesystem_id = %s AND node_id = %s
                            """,
                            (
                                staged_size,
                                final_hash,
                                upload["chunk_size"],
                                self.filesystem_id,
                                node_id,
                            ),
                        )
                        await cur.execute("DELETE FROM vfs_chunks WHERE node_id = %s", (node_id,))
                        await cur.execute(
                            """
                            INSERT INTO vfs_chunks (node_id, chunk_no, data)
                            SELECT %s, chunk_no, data
                              FROM vfs_upload_chunks
                             WHERE upload_id = %s
                             ORDER BY chunk_no
                            """,
                            (node_id, upload_uuid),
                        )

                await discard()
                return True
        except Exception:
            # The publish transaction rolls back before this best-effort
            # cleanup runs, leaving the old target intact. A pool-backed
            # provider gets a fresh transaction; a joined connection is
            # cleaned when its caller transaction is still usable.
            with contextlib.suppress(Exception):
                await self.abort_upload(upload_uuid)
            raise

    async def abort_upload(self, upload_id: UUID | str) -> bool:
        """Immediately discard a staging upload and all of its chunks."""
        self._require_initialized()
        upload_uuid = self._coerce_upload_id(upload_id)
        async with self._acquire() as conn, self._tx(conn):
            return await self._delete_upload(conn, upload_uuid)

    async def read_file(self, path: str) -> bytes | None:
        """Read a file's complete content; None for missing paths/dirs."""
        self._require_initialized()
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
                     WHERE n.filesystem_id = %s AND n.node_id = %s
                     ORDER BY c.chunk_no
                    """,
                    (self.filesystem_id, row["node_id"]),
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
        self._require_initialized()
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
                         WHERE filesystem_id = %s AND node_id = %s
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
                    (
                        self.chunk_size,
                        start,
                        end,
                        self.filesystem_id,
                        row["node_id"],
                    ),
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
        self._require_initialized()
        path = self._normalize(path)
        async with self._acquire() as conn:
            return await self._resolve(conn, path) is not None

    async def get_metadata(self, path: str) -> dict[str, Any]:
        """Return the node's jsonb metadata ({} for missing paths)."""
        self._require_initialized()
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
        self._require_initialized()
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
                         WHERE filesystem_id = %s AND node_id = %s
                        """,
                    (Jsonb(metadata), self.filesystem_id, row["node_id"]),
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
        self._require_initialized()
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
                    (f"{MOVE_TOPOLOGY_LOCK_NAMESPACE}:{self.filesystem_id}",),
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
                                 WHERE filesystem_id = %s AND node_id = %s
                                """,
                            (
                                dest_parent["node_id"],
                                dest_name,
                                self.filesystem_id,
                                src_row["node_id"],
                            ),
                        )
                except psycopg.errors.UniqueViolation:
                    return False
            return True

    async def create_directory(
        self, path: str, mode: int = 0o755, owner_id: int = 1000, group_id: int = 1000
    ) -> bool:
        """Create a directory, creating missing parents (idempotent)."""
        self._require_initialized()
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
                                INSERT INTO vfs_nodes
                                    (filesystem_id, parent_id, name, is_dir, mime_type)
                                VALUES (%s, %s, %s, true, 'inode/directory')
                                ON CONFLICT (filesystem_id, parent_id, name)
                                    WHERE parent_id IS NOT NULL
                                DO NOTHING
                                RETURNING node_id
                                """,
                            (self.filesystem_id, parent_row["node_id"], name),
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
        self._require_initialized()
        async with self._acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE is_dir)      AS dirs,
                        COUNT(*) FILTER (WHERE NOT is_dir)   AS files,
                        COALESCE(SUM(size) FILTER (WHERE NOT is_dir), 0) AS total_bytes
                    FROM vfs_nodes
                    WHERE filesystem_id = %s
                    """,
                    (self.filesystem_id,),
                )
                row = await cur.fetchone()
                assert row is not None  # aggregates always return a row
                await cur.execute(
                    """
                    SELECT COUNT(*)
                      FROM vfs_chunks c
                      JOIN vfs_nodes n ON n.node_id = c.node_id
                     WHERE n.filesystem_id = %s
                    """,
                    (self.filesystem_id,),
                )
                chunk_count = (await cur.fetchone())[0]
            return {
                "total_size_bytes": row[2],
                "file_count": row[1],
                "directory_count": row[0],
                "chunk_count": chunk_count,
            }

    async def cleanup(self) -> dict[str, Any]:
        """Remove staging uploads abandoned for more than 24 hours.

        :meth:`abort_upload` is the immediate cleanup path. This TTL sweep is
        a crash/interruption safety net and never removes visible file nodes.
        """
        self._require_initialized()
        async with self._acquire() as conn, self._tx(conn):
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    WITH expired AS (
                        DELETE FROM vfs_uploads u
                         USING vfs_nodes root
                         WHERE u.root_id = root.node_id
                           AND root.filesystem_id = %s
                           AND root.parent_id IS NULL
                           AND u.created_at < now() - make_interval(secs => %s)
                        RETURNING u.size
                    )
                    SELECT COUNT(*), COALESCE(SUM(size), 0) FROM expired
                    """,
                    (self.filesystem_id, UPLOAD_TTL_SECONDS),
                )
                row = await cur.fetchone()
            assert row is not None
            return {
                "files_removed": 0,
                "bytes_freed": row[1],
                "expired_removed": row[0],
            }
