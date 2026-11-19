---
name: verify
description: Prove a change to adbc-poolhouse works end to end. Use before saying a change is done, before committing, and before opening or updating a pull request.
---

# Verify a change

Run from the repository root:

```bash
uv sync --locked --dev --extra duckdb
prek run --all-files
uv run pytest > /tmp/pytest.log 2>&1; echo "pytest rc=$?"
uv run mkdocs build --strict
```

It passes when every command exits 0. prek runs basedpyright, ruff and the format check
but no tests, so pytest is a separate step. For a change to the async layer, run the
async suite the way CI does: `ADBC_ASYNC_REPEAT=20 uv run pytest tests/async`.

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

## When it fails

- Read the first failure and fix its cause. Do not rerun hoping for a green, and do not
  skip, loosen or delete the failing test to get one.
- If the failure is real but outside the change, say so in the PR and in your report
  rather than working around it.
