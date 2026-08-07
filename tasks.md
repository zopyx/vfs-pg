# vfs-pg improvement tasks

Tasks are ordered by risk and dependency. Complete the data-integrity and lifecycle work before treating the project as production-ready.

## P0 — Data integrity and corruption prevention

### [ ] Prevent duplicate sibling nodes

- Add a database-level unique constraint or unique index on `(parent_id, name)` for non-root nodes.
- Keep the partial root uniqueness constraint.
- Replace check-then-insert logic with an atomic insert and handle `UniqueViolation` as an expected conflict.
- Provide a migration that detects and reports existing duplicate siblings before adding the constraint.
- Update the README so the documented schema matches the implemented schema.

Acceptance criteria:

- Concurrent attempts to create the same path result in exactly one stored node.
- No caller receives success for a node that was not created by that operation.
- Concurrent `mkdir`, `touch`, and first-write tests cover this behavior.

### [ ] Reject directory moves into their own descendants

- In `move_node()`, return success immediately when source and destination are identical.
- Reject moving a directory beneath itself or any descendant.
- Perform validation and the update in the same transaction.
- Consider a recursive CTE or an ancestor walk protected by appropriate row locks.

Acceptance criteria:

- `/a -> /a/child/moved` is rejected without changing the tree.
- Moving a path onto itself is an idempotent success.
- Normal file and directory moves continue to preserve content and metadata.

### [ ] Make chunk layout self-describing

- Validate that configured `chunk_size` is a positive integer.
- Store the chunk size with each file/content version, or enforce one immutable database-wide value.
- Make `read_range()` use the persisted value rather than the reader instance's configuration.
- Define migration behavior for existing files.

Acceptance criteria:

- A file written by one provider instance can be range-read correctly by another instance configured with a different default chunk size.
- Invalid chunk sizes fail during provider construction or initialization.
- Cross-chunk, exact-boundary, empty-file, and EOF-clamping tests pass.

### [ ] Make exclusive creation (`xb`) truly exclusive

- Raise `FileExistsError` when opening or committing an existing path with `xb`.
- Enforce exclusivity with the database uniqueness constraint, not only a preflight existence check.
- Ensure a failed exclusive write does not overwrite or truncate existing content.

Acceptance criteria:

- `xb` creates a missing file.
- `xb` against an existing file raises `FileExistsError` and preserves its bytes and metadata.
- Two concurrent exclusive creates produce one success and one `FileExistsError`.

## P1 — Transactions and provider lifecycle

### [ ] Fix schema-initialization locking

- Replace the session-level `pg_advisory_lock()` with `pg_advisory_xact_lock()` in a controlled transaction.
- Preserve the original schema error instead of masking it with an unlock failure.
- Close and clear a newly created pool when initialization fails.
- Add a timeout test for concurrent initialization.

Acceptance criteria:

- Concurrent provider initialization completes without deadlock.
- A forced schema error releases the advisory lock when the transaction rolls back.
- A provider can initialize successfully after an earlier initialization failure.

### [ ] Make `initialize()` idempotent and concurrency-safe

- Return immediately when an already initialized provider is initialized again.
- Serialize concurrent `initialize()` calls on the same instance.
- Never replace a live pool without closing it.
- Avoid production code that depends on private `psycopg_pool` internals during shutdown.

Acceptance criteria:

- Repeated and concurrent initialization uses one pool.
- `close()` is idempotent.
- Operations after close fail consistently until reinitialization, including providers using an external connection.

### [ ] Make new-file writes atomic

- Address the current high-level `touch` followed by `write_file` sequence, which spans separate pooled transactions.
- Add an atomic provider operation for create-or-replace content, or expose a transaction boundary that covers node creation and chunk insertion.
- Ensure failed writes do not leave empty placeholder files.

Acceptance criteria:

- A failure during first write leaves neither a partial file nor an empty node.
- Overwrites remain atomic: readers observe the complete old or complete new version.
- External-connection transaction joining continues to commit and roll back filesystem and business data together.

### [ ] Make metadata updates concurrency-safe

- Replace read/merge/write metadata updates with an atomic JSONB merge expression.
- Decide and document whether metadata updates change `modified_at`.
- Add concurrent disjoint-key and same-key update tests.

## P1 — fsspec contract compliance

### [ ] Register the `chuk` protocol during package installation

- Add a `fsspec.specs` entry point in `pyproject.toml` for `ChukFileSystem`.
- Keep explicit registration supported for development and testing.
- Add a clean-process test that calls `fsspec.filesystem("chuk", vfs=vfs)` without manual registration.
- Update the README with the exact supported initialization path.

### [ ] Correct `mkdir` semantics

- Honor `create_parents=False`.
- Treat an existing directory as the documented idempotent case.
- Raise an appropriate error when the target or an intermediate component is a file.
- Handle concurrent directory creation without creating duplicates.

### [ ] Use provider-local paths for mounted filesystems

- Preserve both values returned by `_get_provider_for_path()`.
- Pass the translated local path to provider extension methods such as `read_range()`.
- Avoid private `AsyncVirtualFileSystem` APIs if a stable public alternative is available.
- Test range reads through a mounted provider.

### [ ] Align listing and range behavior with fsspec conventions

- Verify whether `ls(..., detail=False)` should return full paths rather than basenames, and make detailed and non-detailed output consistent.
- Define and test negative `start`/`end` range semantics.
- Run fsspec's applicable filesystem contract tests against the adapter.

## P2 — Streaming and scalability

### [ ] Implement bounded-memory, atomic uploads

- Stop retaining the complete file in `ChukBufferedFile._parts`.
- Stop constructing a second complete list of chunks in `PostgresStorageProvider.write_file()`.
- Stream chunks into a temporary content version or upload identifier.
- Atomically switch the node to the completed version and clean abandoned/old versions safely.
- Preserve SHA-256 calculation while streaming.

Acceptance criteria:

- Peak process memory remains bounded when writing files substantially larger than the fsspec block size.
- Readers never see incomplete upload versions.
- Interrupted uploads are recoverable or cleaned by a defined process.

### [ ] Define safe concurrent append semantics

- Replace append read-modify-write with row locking, optimistic version checks, or a dedicated append operation.
- Document whether multiple appenders are serialized and what ordering guarantee applies.
- Add concurrent append tests that prove no data is lost.

### [ ] Evaluate PostgreSQL storage limits operationally

- Benchmark WAL volume, TOAST behavior, vacuum pressure, overwrite churn, backup size, and restore time.
- Document the intended maximum file size and workload profile.
- Decide when content should move to PostgreSQL large objects or external object storage while metadata remains transactional.

## P2 — Isolation, packaging, and test safety

### [ ] Add filesystem/tenant isolation

- Add a `filesystem_id` or namespace key to nodes and chunks.
- Scope root uniqueness, sibling uniqueness, queries, and statistics to that identifier.
- Avoid forcing every provider using the same database to share one global root.

### [ ] Prevent tests from truncating arbitrary databases

- Run integration tests in a disposable database or isolated schema.
- Alternatively, require an explicit test-only opt-in and validate the target database name before destructive setup.
- Remove unconditional `TRUNCATE vfs_nodes CASCADE` against arbitrary `VFS_PG_DSN` values.
- Ensure business-table transaction tests clean up and commit their cleanup.

### [ ] Improve reproducibility and package metadata

- Decide whether this repository is an application or a reusable library.
- Commit `uv.lock` for reproducible application/test environments, or document the library dependency policy.
- Add package metadata, supported Python/PostgreSQL versions, license information, and release automation before publishing.

## P2 — Stress test and observability improvements

### [ ] Make the stress test validate data, not only lengths

- Store enough deterministic generation information to verify random range bytes.
- Do not hold `registry_lock` while sleeping.
- Move large random generation and hashing off the event loop or pre-generate data.
- Count successful and attempted bytes separately.
- Report exception types and representative failures instead of swallowing all exceptions.
- Put cleanup in `finally` so interrupted runs do not leave data behind.
- Make the progress interval respect short stress durations.

Acceptance criteria:

- Range corruption causes the stress run to fail.
- Reported throughput distinguishes attempted, successful, and failed operations.
- A cancelled or failed run cleans its own filesystem namespace.

## Verification gate

Before declaring the prototype production-ready:

- [ ] All existing tests pass.
- [ ] Regression tests cover every P0 and P1 defect above.
- [ ] Concurrency tests run repeatedly without duplicate nodes, deadlocks, lost appends, or partial reads.
- [ ] Tests run only against an isolated disposable database/schema.
- [ ] A large-file benchmark demonstrates bounded memory and documents WAL/storage costs.
- [ ] Documentation accurately describes registration, transaction guarantees, file-size limits, and multi-tenant behavior.
