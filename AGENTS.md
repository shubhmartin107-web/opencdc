# AGENTS.md - OpenCDC Build & Test Guide

## Quick Start

```bash
cargo build --workspace     # Build all crates
cargo test --workspace      # Run all tests
cargo clippy --workspace    # Lint all crates (zero warnings goal)
cargo fmt --all             # Format all code
```

## Workspace Structure

- `crates/opencdc-core` — Core types: ChangeEvent, Operation, DebeziumSchema, SourceInfo, ConnectorOffset, DebeziumArrowMapper
- `crates/opencdc-serde` — Serialization: JSON (Debezium envelope), Avro schema gen, Arrow IPC, Schema Registry wire format
- `crates/opencdc-schema` — Schema management: Builder, SchemaRegistryClient (Confluent API), SchemaBridge (Debezium↔Arrow), SchemaEvolution, Naming conventions
- `crates/opencdc-connector` — Connector trait + Postgres connector (`postgres` feature) + MySQL connector (`mysql` feature) + MongoDB connector (`mongodb` feature)
- `crates/opencdc-mcp` — MCP server binary exposing CDC tools via stdio transport
- `crates/opencdc-pipeline` — Pipeline runtime: Sink/Transform traits, StdoutSink, NullSink, Filter/Rename transforms
- `crates/opencdc-config` — TOML configuration loader for all connector types and pipeline config

> Note: `crates/opencdc-sink-openlake` and `crates/opencdc-demo` exist in the repo but are **not workspace members** (they depend on a private `openlake-core`/`openlake-query` project at `../openlake/`). Build them manually if you have access.

## Common Commands

```bash
# Build individual crate
cargo build -p opencdc-core
cargo build -p opencdc-serde
cargo build -p opencdc-schema
cargo build -p opencdc-connector --features postgres
cargo build -p opencdc-connector --features mysql
cargo build -p opencdc-connector --features mongodb
cargo build -p opencdc-connector --all-features
cargo build -p opencdc-mcp
cargo build -p opencdc-pipeline
cargo build -p opencdc-sink-openlake
cargo build -p opencdc-demo

# Test individual crate
cargo test -p opencdc-core
cargo test -p opencdc-serde
cargo test -p opencdc-schema
cargo test -p opencdc-connector --features postgres,mysql,mongodb
cargo test -p opencdc-connector --features mongodb
cargo test -p opencdc-mcp
cargo test -p opencdc-pipeline
cargo test -p opencdc-sink-openlake
cargo test -p opencdc-demo
cargo test -p opencdc-config

# Run MCP server
cargo run -p opencdc-mcp

# Run end-to-end demo
cargo run -p opencdc-demo

# Run specific test
cargo test -- test_name

# Docker build
docker build -t opencdc-mcp .
```

## Test Counts

```
opencdc-core      93 tests
opencdc-connector  38 tests
opencdc-schema     19 tests
opencdc-serde      19 tests
opencdc-pipeline   18 tests
opencdc-mcp        18 tests
opencdc-config     11 tests
------------------
Total:            216 tests, 0 clippy warnings
```

## Architecture Notes

### Backpressure

All connectors use bounded `tokio::sync::mpsc::channel(1024)` for event delivery. Sender types:
- `Connector::snapshot()` / `Connector::stream()` take `mpsc::Sender<ChangeEvent>` (not `UnboundedSender`)
- Pipeline creates `channel(1024)` pairs and passes the sender to connectors
- `sink.send(event).await` blocks the producer when the channel is full (backpressure)
- `sink.send(event).await.is_err()` checks for receiver drop

### Graceful Shutdown

MCP server (`opencdc-mcp`) handles SIGINT via `tokio::signal::ctrl_c()` in a `tokio::select!` alongside `server.waiting()`. On signal:
1. All connector statuses set to `Stopped`
2. All active sink channels cleared
3. Process exits cleanly

### Health Endpoint

Optional HTTP server toggled via `OPENCDC_HEALTH_PORT` env var:
- `GET /health` — JSON with uptime, connector stats, event counts, aggregate status
- `GET /metrics` — Prometheus-formatted text with counter/gauges for events received/sent/errors, connectors running, uptime

### Postgres Reconnect

The Postgres connector uses `connect_with_retry()` with configurable `max_reconnect_attempts` (default 5) and exponential backoff (500ms × attempt number). The `connection_string()` method builds the `host=... port=...` format for `tokio-postgres`.

### opencdc-config

TOML config loader with:
- `AppConfig::from_file(path)` and `AppConfig::from_toml(str)` for loading
- `SourceConfig` enum with `Postgres`/`MySql`/`MongoDb` variants
- `PipelineConfig` with optional transforms (filter/rename/log) and sinks (stdout/null/openlake)
- `toml` v0.8 dependency

### Rate Limiting

MCP tool handlers acquire a semaphore permit before processing, limiting concurrent tool executions. Configure via `OPENCDC_MAX_CONCURRENT_TOOLS` env var (default 10). Uses `tokio::sync::Semaphore` with `acquire_tool_permit()` on `AppState`.

### REST Catalog Auth

`RestCatalogSinkConfig` now includes `auth_token: Option<String>`. When set, all REST API requests include `Authorization: Bearer <token>`. Configurable via TOML `auth_token = "..."` field in openlake sink config.

### Security

- No hardcoded secrets in production code (verified by audit)
- All passwords come from TOML config files or env vars
- No `openssl`/`native-tls` dependency
- CORS headers on health endpoint with `Access-Control-Allow-Origin: *`
- Standardized JSON errors on non-200 endpoints

### Safety Audit (complete)

All known bugs fixed:
- **mysql/column.rs**: BIT(64) high-bit truncation → uses `val.to_string()` for values > i64::MAX
- **mysql/column.rs**: JSON NULL corruption → `String::from_utf8_lossy` preserves data
- **mysql/column.rs**: DECIMAL UTF-8 data loss → `String::from_utf8_lossy` preserves data
- **mysql/binlog.rs**: Null-byte not found → returns clear error instead of empty names
- **postgres/decoder.rs**: Negative WAL length → explicit range check before usize cast
- **postgres/snapshot.rs**: LSN parse silent 0 → propagates parse errors properly
- **postgres/types.rs**: UTF-8 data loss → `String::from_utf8_lossy` preserves data
- **schema/registry.rs**: Silently substituted defaults → propagates missing field errors
- **sink-openlake/lib.rs**: JSON serde silent corruption → logs warning on failure
- **mcp/state.rs**: Semaphore panic on shutdown → returns `Option` for graceful handling

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENCDC_HEALTH_PORT` | (none) | Port for health/metrics HTTP server |
| `OPENCDC_MAX_CONCURRENT_TOOLS` | `10` | Max concurrent MCP tool executions |
| `OPENCDC_SCHEMA_REGISTRY_URL` | (none) | Confluent Schema Registry base URL |
| `OPENCDC_SCHEMA_REGISTRY_TOKEN` | (none) | Bearer token for schema registry auth |
| `OPENCDC_LOG_FORMAT` | (none) | Set to `json` for structured JSON logging |

### Tools & Workflows

- `.githooks/pre-commit` — Pre-commit hook (install: `git config core.hooksPath .githooks`)
- `.github/workflows/release.yml` — Tag-triggered `v*` release workflow, builds all features + Docker
- `Dockerfile` — Multi-stage build using `cargo-chef`, distroless runtime
- `.dockerignore` — Excludes target, .git, docs from Docker context

## Key Dependencies

- Rust edition 2024, Apache 2.0 license
- Arrow 57.x (not 53)
- reqwest 0.12 (rustls-tls, no native-tls/openssl)
- apache-avro 0.17
- tokio 1.x (features = ["full"])

## Rust Version

MSRV: 1.85+ (edition 2024 requires nightly or Rust 1.85+)
