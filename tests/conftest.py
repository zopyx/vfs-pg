"""Shared fixtures: PostgreSQL via docker compose (postgres:16, db `vfs`).

Start it with:

    docker compose up -d
"""

import os

import pytest_asyncio
import psycopg

import chuk_vfs_postgres  # noqa: F401  (registers the "postgres" provider)

from chuk_vfs_postgres import PostgresStorageProvider

DSN = os.environ.get("VFS_PG_DSN", "postgresql://vfs:vfs@localhost:5432/vfs")


@pytest_asyncio.fixture(scope="session")
async def provider():
    p = PostgresStorageProvider(dsn=DSN)
    assert await p.initialize(), "provider initialize failed — is postgres up? (docker compose up -d)"
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
async def vfs(provider):
    """A chuk AsyncVirtualFileSystem wired to the postgres provider."""
    from chuk_virtual_fs.fs_manager import AsyncVirtualFileSystem

    fs = AsyncVirtualFileSystem("postgres", dsn=DSN)
    await fs.initialize()
    yield fs
    await fs.close()


@pytest_asyncio.fixture
async def external_conn():
    """A standalone connection for transaction-join tests."""
    conn = await psycopg.AsyncConnection.connect(DSN)
    yield conn
    await conn.close()
