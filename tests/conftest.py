"""Shared pytest fixtures.

PostgreSQL is provided by `testcontainers` (postgres:16-alpine) — no
docker-compose needed. Set ``VFS_PG_DSN`` to run the suite against an
existing server instead (the container is then skipped).

    uv sync --extra test
    uv run pytest -v
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from testcontainers.community.postgres import PostgresContainer

import chuk_vfs_postgres  # noqa: F401  (registers the "postgres" provider)
from chuk_vfs_postgres import PostgresStorageProvider

IMAGE = os.environ.get("VFS_PG_IMAGE", "postgres:16-alpine")


@pytest.fixture(scope="session", autouse=True)
def test_filesystem_id():
    """Give this test session a private, non-destructive VFS namespace."""
    previous = os.environ.get("VFS_PG_FILESYSTEM_ID")
    filesystem_id = f"pytest-{uuid4()}"
    os.environ["VFS_PG_FILESYSTEM_ID"] = filesystem_id
    yield filesystem_id
    if previous is None:
        os.environ.pop("VFS_PG_FILESYSTEM_ID", None)
    else:
        os.environ["VFS_PG_FILESYSTEM_ID"] = previous


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[PostgresContainer | None]:
    """A throwaway PostgreSQL server (skipped when VFS_PG_DSN is set)."""
    if os.environ.get("VFS_PG_DSN"):
        yield None
        return
    with PostgresContainer(IMAGE) as pg:
        yield pg


@pytest.fixture(scope="session")
def dsn(postgres_container: PostgresContainer | None) -> str:
    """Connection URL for the test database."""
    env_dsn = os.environ.get("VFS_PG_DSN")
    if env_dsn:
        return env_dsn
    assert postgres_container is not None
    url = postgres_container.get_connection_url()
    # testcontainers defaults to the psycopg2 driver string; psycopg3
    # wants the plain scheme
    return url.replace("postgresql+psycopg2://", "postgresql://", 1)


@pytest_asyncio.fixture(scope="session")
async def provider(dsn: str, test_filesystem_id: str):
    """A PostgresStorageProvider on the test database (session-scoped)."""
    p = PostgresStorageProvider(dsn=dsn)
    assert p.filesystem_id == test_filesystem_id
    assert await p.initialize(), "provider initialize failed"
    yield p
    async with p._acquire() as conn, p._tx(conn), conn.cursor() as cur:
        # Deleting only this session's root cascades its nodes, chunks, and
        # root-owned staging uploads. Other namespaces remain untouched.
        await cur.execute(
            """
            DELETE FROM vfs_nodes
             WHERE filesystem_id = %s AND parent_id IS NULL
            """,
            (test_filesystem_id,),
        )
    await p.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_db(provider):
    """Clear only this session's root children and staging uploads."""
    async with provider._acquire() as conn, provider._tx(conn), conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM vfs_uploads u
             USING vfs_nodes root
             WHERE root.node_id = u.root_id
               AND root.filesystem_id = %s
               AND root.parent_id IS NULL
            """,
            (provider.filesystem_id,),
        )
        await cur.execute(
            """
            DELETE FROM vfs_nodes child
             USING vfs_nodes root
             WHERE child.parent_id = root.node_id
               AND root.filesystem_id = %s
               AND root.parent_id IS NULL
            """,
            (provider.filesystem_id,),
        )
    yield


@pytest_asyncio.fixture
async def vfs(dsn: str):
    """A chuk AsyncVirtualFileSystem wired to the postgres provider."""
    from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem

    fs = AsyncVirtualFileSystem("postgres", dsn=dsn)
    await fs.initialize()
    yield fs
    await fs.close()


@pytest_asyncio.fixture
async def external_conn(dsn: str):
    """A standalone connection for transaction-join tests."""
    conn = await psycopg.AsyncConnection.connect(dsn)
    yield conn
    await conn.close()
