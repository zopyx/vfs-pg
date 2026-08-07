"""Shared pytest fixtures.

PostgreSQL is provided by `testcontainers` (postgres:16-alpine) — no
docker-compose needed. Set ``VFS_PG_DSN`` to run the suite against an
existing server instead (the container is then skipped).

    uv sync --extra test
    uv run pytest -v
"""

from __future__ import annotations

import os

import psycopg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

import chuk_vfs_postgres  # noqa: F401  (registers the "postgres" provider)

from chuk_vfs_postgres import PostgresStorageProvider

IMAGE = os.environ.get("VFS_PG_IMAGE", "postgres:16-alpine")


@pytest.fixture(scope="session")
def postgres_container() -> PostgresContainer | None:
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
async def provider(dsn: str):
    """A PostgresStorageProvider on the test database (session-scoped)."""
    p = PostgresStorageProvider(dsn=dsn)
    assert await p.initialize(), "provider initialize failed"
    yield p
    await p.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_db(provider):
    """Reset the VFS tree before every test (keeps the root node)."""
    async with provider._acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE vfs_nodes CASCADE")
            await cur.execute(
                "INSERT INTO vfs_nodes (parent_id, name, is_dir) VALUES (NULL, '', true)"
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
