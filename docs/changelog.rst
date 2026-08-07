Changelog
=========

Unreleased
----------

- **Topology and reads (F-01/F-02)**: serialize topology-changing moves to
  prevent concurrent cross-move cycles, and read range metadata plus chunks
  from one database snapshot so mixed file versions cannot be returned.
- **Transactions and paths (F-03/F-04)**: isolate expected uniqueness
  conflicts with savepoints so joined business transactions remain usable,
  and canonicalize all provider and ``chuk://`` paths consistently while
  rejecting parent traversal and NUL bytes.
- **Lifecycle (F-05)**: synchronize initialize/close races and make the closed
  state consistent for pooled and externally connected providers.
- **Streaming and append (F-06)**: add bounded-memory staged uploads with
  atomic publication, abandoned-upload cleanup, and serialized concurrent
  append without lost suffixes.
- **Isolation (F-07)**: scope roots, sibling uniqueness, content, statistics,
  moves, deletes, and uploads by ``filesystem_id``; test cleanup now deletes
  only its random session namespace.
- **Release hygiene (F-08)**: enforce Ruff check and formatting, 94% coverage,
  strict warning-free Sphinx documentation, package builds, and clean-wheel
  import/entry-point smoke tests on Python 3.12, 3.13, and 3.14. Documentation
  now describes streaming, append ordering, namespaces, and alpha limits.

0.2.0 (2026-08-07)
------------------

Package release with integrity and lifecycle hardening:

- **Data integrity (P0)**: unique sibling index ``(parent_id, name)`` with
  migration guard, atomic ``create_node``, descendant-move rejection,
  idempotent move onto itself, per-file chunk size (self-describing
  layout), database-enforced exclusive creation (``xb``).
- **Lifecycle (P1)**: transaction-scoped advisory lock for schema init
  (deadlock fix), idempotent and failure-safe ``initialize()``, atomic
  create-or-replace writes, atomic JSONB metadata merge.
- **fsspec (P1)**: ``fsspec.specs`` entry point, ``mkdir``
  ``create_parents`` semantics, full paths in non-detailed ``ls``,
  mount-local range reads.
- **Packaging**: full PEP 621 metadata, ``test``/``docs`` extras, Sphinx
  documentation, CI, MIT license.
- **Testing**: testcontainers-based PostgreSQL fixtures (no docker-compose
  needed), 152 tests including concurrency and regression suites.
- **Stress test**: data validation, attempted-vs-ok byte accounting,
  exception reporting, cleanup in ``finally``.

0.1.0 (2026-08-07)
------------------

Prototype: ``PostgresStorageProvider`` (chunked, transaction-join capable)
and the ``chuk_fsspec`` adapter with 256 MiB demo and 60 s stress test.
