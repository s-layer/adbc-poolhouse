---
name: verify
description: Prove a change to adbc-poolhouse works end to end. Use before saying a change is done, before committing, and before opening or updating a pull request.
---

# Verify a change

Run from the repository root:

```bash
uv sync --locked --dev --extra duckdb
uv tool install prek  # once; skip if prek is already on PATH
prek run --all-files
CI=true uv run pytest > /tmp/pytest.log 2>&1; echo "pytest rc=$?"
uv run mkdocs build --strict
```

It passes when every command exits 0 and every printed `rc=` is 0. prek runs
basedpyright, ruff and the format check but no tests, so pytest is a separate step. For
a change to the async layer, run the async suite the way CI does: `ADBC_ASYNC_REPEAT=20
uv run pytest tests/async`.

`CI=true` turns on `tests/ci_skips.py`, which fails the run on any skip not allowed
there, as CI does. Without it a skipped test only shows in the summary, and a run that
skipped what you changed is not a pass. It is set on each test line rather than
exported, so it still holds when each line runs in a fresh shell, and other tools that
read `CI` (Playwright, for one) keep their local behaviour.

Take each command's exit status from the command itself. In a pipeline `$?` is the last
command's status, so `cmd | tail` hides a failure. Redirect long output to a file, then
read the file.

## What this covers

- the sync and async pools against real DuckDB and SQLite, and the Snowflake and
  Databricks cassettes in replay
- random orderings of checkout, check-in, cancel and invalidate
  (`tests/async/test_pool_state_machine.py`)

## What it does not cover

- the sync package without anyio installed: CI's `sync-no-anyio` job checks it
- the lowest supported driver release: CI's floor job
- live warehouses: cassettes replay what was recorded, so a warehouse change needs a
  re-record
- Python versions other than the one you ran: CI runs a version matrix

## When it fails

- Read the first failure and fix its cause. Do not rerun hoping for a green, and do not
  skip, loosen or delete the failing test to get one.
- If the failure is real but outside the change, say so in the PR and in your report
  rather than working around it.
