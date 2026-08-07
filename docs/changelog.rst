Changelog
=========

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
  needed), 67 tests including concurrency and regression suites.
- **Stress test**: data validation, attempted-vs-ok byte accounting,
  exception reporting, cleanup in ``finally``.

0.1.0 (2026-08-07)
------------------

Prototype: ``PostgresStorageProvider`` (chunked, transaction-join capable)
and the ``chuk_fsspec`` adapter with 256 MiB demo and 60 s stress test.
