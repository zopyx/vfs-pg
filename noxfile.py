"""Nox sessions for vfs-pg — test, lint, docs, build across Python 3.12–3.15.

Usage::

    nox                    # run all sessions
    nox -s tests           # tests on the current Python only
    nox -s tests-3.12      # tests on Python 3.12
    nox -s lint            # ruff only
    nox -s docs            # Sphinx build only

Set ``VFS_PG_DSN`` to run tests against an existing PostgreSQL server instead
of spinning up a testcontainer (needed in CI without Docker).
"""

from __future__ import annotations

import nox

PYTHON_VERSIONS = ["3.12", "3.13", "3.14", "3.15"]
DEFAULT_PYTHON = "3.12"

nox.options.default_venv_backend = "uv"
nox.options.stop_on_first_error = True


# ---------------------------------------------------------------------------
# install helpers
# ---------------------------------------------------------------------------

def _install(session: nox.Session, *extras: str) -> None:
    """Install the project with the given extras in editable mode."""
    extras_str = f"[{','.join(extras)}]" if extras else ""
    session.run_install(
        "uv",
        "sync",
        f"--extra={' --extra '.join(extras)}" if extras else "",
        "--inexact",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
        silent=True,
    )
    session.install("--no-deps", "-e", ".")


# ---------------------------------------------------------------------------
# tests — parametrised by Python version
# ---------------------------------------------------------------------------


@nox.session(python=PYTHON_VERSIONS)
def tests(session: nox.Session) -> None:
    """Run the test suite with coverage (needs PostgreSQL via testcontainers or VFS_PG_DSN)."""
    _install(session, "test")
    session.install("pytest-cov")
    session.run(
        "pytest",
        "--cov=chuk_vfs_postgres",
        "--cov=chuk_fsspec",
        "--cov-report=term-missing",
        "-ra",
        *session.posargs,
    )


# ---------------------------------------------------------------------------
# lint
# ---------------------------------------------------------------------------


@nox.session(python=DEFAULT_PYTHON)
def lint(session: nox.Session) -> None:
    """Lint with ruff (format + check)."""
    _install(session)
    session.install("ruff")
    session.run("ruff", "check", ".")
    session.run("ruff", "format", "--check", ".")


@nox.session(python=DEFAULT_PYTHON)
def format(session: nox.Session) -> None:
    """Auto-format with ruff."""
    _install(session)
    session.install("ruff")
    session.run("ruff", "format", ".")


# ---------------------------------------------------------------------------
# docs
# ---------------------------------------------------------------------------


@nox.session(python=DEFAULT_PYTHON)
def docs(session: nox.Session) -> None:
    """Build Sphinx HTML documentation."""
    _install(session, "docs")
    session.run(
        "sphinx-build",
        "-b",
        "html",
        "-W",  # warnings as errors
        "--keep-going",
        "docs/",
        "docs/_build/html",
    )


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------


@nox.session(python=DEFAULT_PYTHON)
def build(session: nox.Session) -> None:
    """Build a distributable wheel."""
    _install(session)
    session.install("build")
    session.run("python", "-m", "build", "--wheel")
