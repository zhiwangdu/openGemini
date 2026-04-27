# Repository Guidelines

openGemini is a distributed time-series database written in Go (1.24). This document provides conventions for contributors and coding agents working in this repository.

## Project Structure

| Directory | Purpose |
|-----------|---------|
| `app/` | Core component entry points — ts-meta, ts-sql, ts-store, ts-monitor, ts-recover, ts-server |
| `engine/` | Storage engine: shards, partitions, indexes, WAL, compaction, and query execution |
| `lib/` | Shared utilities (config, logger, file ops, protocol buffers, etc.) |
| `services/` | Background services: continuous query, downsample, stream, replication, etc. |
| `coordinator/` | Distributed read/write request coordination across ts-store nodes |
| `config/` | Default configuration files (e.g. `openGemini.conf`) |
| `tests/` | Integration and functional test suite |
| `scripts/ci/` | CI helper scripts (linting, vet, static analysis) |
| `docker/` | Docker deployment files and startup scripts |

## Build, Test, and Development Commands

All commands should be run from the repository root.

| Command | Description |
|---------|-------------|
| `make go-build` | Compile the full project via `build.py --clean` |
| `make gotest` | Run unit tests with coverage (failpoint-enabled, short mode) |
| `make integration-test` | Run integration tests against a local cluster |
| `make style-check` | Enforce import ordering with `goimports-reviser` |
| `make static-check` | Run `staticcheck` on all packages |
| `make go-vet-check` | Run `go vet` excluding `tests/` and `lifted/` |
| `make license-check` | Verify Apache 2.0 headers on all `.go` files |
| `make build-check` | Cross-compile for Windows, macOS, and Linux targets |

## Coding Style & Naming Conventions

- **Indentation**: Tabs (standard Go convention).
- **Imports**: Group standard library first, then third-party, then internal (`github.com/openGemini/...`). Run `make style-check` to enforce ordering via `goimports-reviser`.
- **Naming**: Use `PascalCase` for exported symbols, `camelCase` for unexported. Package names are lowercase, single-word identifiers.
- **License header**: Every `.go` file must begin with the Apache 2.0 license block (run `make license-check` to verify).
- **Line length**: Keep lines under 120 characters (enforced by `linelint` in CI).
- **Formatting tools**: `goimports-reviser`, `staticcheck`, `golangci-lint`, `go vet`. CI runs these before tests.

## Testing Guidelines

- **Framework**: Standard `testing` package. Tests live alongside source as `_test.go` files.
- **Coverage targets**: 70% project-wide, 80% for new/changed code (configured in `codecov.yml`).
- **Running tests**: `make gotest` for unit tests; `make integration-test` for end-to-end tests.
- **Test naming**: Functions should follow `Test<FunctionName>_<Scenario>` (e.g., `TestCreateSerfInstance_NilConfig`).
- **Packages excluded from coverage**: `lib/util/lifted`, `tests/`, `main.go`.

## Commit & Pull Request Guidelines

### Commits

Follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/). Valid types:

`feat` | `fix` | `docs` | `style` | `refactor` | `perf` | `test` | `build` | `ci` | `chore` | `revert`

Always sign-off with `-s` (DCO requirement):

```bash
git commit -s -m "fix: resolve raft logger panic under high concurrency"
```

### Pull Requests

- Link an issue via `close #xxx` or `fix #xxx` in the description.
- Fill out the PR template checklist (style compliance, tests, documentation).
- Add or update tests for new functionality and bug fixes.
- Ensure `make gotest` passes locally before pushing.
- Avoid modifying files under `lib/util/lifted/` (vendored third-party code).
