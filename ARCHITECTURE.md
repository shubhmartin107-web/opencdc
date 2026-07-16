# OpenCDC Architecture

## Overview

OpenCDC is a Change Data Capture system built as a Rust workspace of 7 crates (plus 2 optional crates requiring a private OpenLake dependency). Events flow from a source database through a configurable pipeline of transforms and into one or more sinks. The system is designed for low latency, backpressure-aware data movement, and full Debezium message format compatibility.

## Data Flow

```
Database ──▶ Connector ──▶ channel(1024) ──▶ Pipeline ──▶ transforms ──▶ sinks
                                                   │
                                                   └──▶ MCP Server (tool-based control)
```

1. **Connector** connects to a source database (Postgres/MySQL/MongoDB) and captures change events
2. Events are sent through a bounded `tokio::sync::mpsc::channel(1024)` — this provides natural backpressure
3. **Pipeline** reads from the channel, applies transforms sequentially, and fans out to sinks
4. **MCP Server** provides tool-based control for connector lifecycle and inspection

## Crate Dependency Graph

```
opencdc-core
  ├── opencdc-serde
  ├── opencdc-schema
  ├── opencdc-connector
  │     ├── opencdc-pipeline
  │     │     └── [opencdc-sink-openlake]* + [opencdc-demo]*
  │     └── opencdc-mcp
  └── opencdc-config

* Not workspace members — require private openlake-core/openlake-query.
```

### Layer 1: Core (`opencdc-core`)

Zero-dependency (within workspace) foundational types used by every other crate:

- `ChangeEvent` — The central CDC event with `schema` + `payload` (`before`, `after`, `source`, `op`, `ts_ms`, `transaction`)
- `Operation` — Enum: `Create`, `Update`, `Delete`, `Read`, `Truncate`, `Message` (serialized as single-char strings)
- `ConnectorType` — Enum: `Postgres`, `Mysql`, `Mongodb`, `SqlServer`, `Oracle`, etc.
- `SourceInfo` — Debezium-compatible source metadata (connector, db, table, LSN, GTID, binlog pos, resume token, snapshot phase)
- `ConnectorOffset` — Offset state for resuming from a known position
- `DebeziumSchema` / `DebeziumField` — Schema representation matching Debezium's JSON schema format
- `DebeziumArrowMapper` — Bidirectional type mapping between Debezium types and Apache Arrow types
- `Error` / `Result<T>` — Unified error types

### Layer 2: Serialization & Schema (`opencdc-serde`, `opencdc-schema`)

**opencdc-serde** handles all data format conversions:
- `DebeziumJsonSerde` — JSON serialize/deserialize `ChangeEvent`
- `EnvelopeBuilder` — Build Debezium envelope schemas
- `ArrowIpcSerde` — Arrow IPC file and stream read/write
- `AvroSchemaGenerator` — Convert Debezium schemas to Avro JSON schemas
- `SchemaRegistryWireFormat` — Confluent wire format (magic byte + 4-byte ID + payload)
- `BatchConverter` — Convert between `ChangeEvent` slices and Arrow `RecordBatch`

**opencdc-schema** manages the schema lifecycle:
- `SchemaBuilder` — Builder pattern for `DebeziumSchema`
- `SchemaRegistryClient` — HTTP client for Confluent Schema Registry REST API
- `SchemaBridge` — Bidirectional conversion between Arrow and Debezium schemas
- `SchemaEvolution` — Schema diffing and evolution policy (`AutoAdd`, `Warn`, `Fail`)
- `DebeziumNaming` — Topic, subject, and envelope naming conventions

### Layer 3: Connector Abstraction (`opencdc-connector`)

The `Connector` trait defines the interface for all source connectors:

```rust
#[async_trait]
pub trait Connector: Send {
    fn name(&self) -> &str;
    fn connector_type(&self) -> ConnectorType;
    async fn start(&mut self, config: ConnectorConfig) -> Result<()>;
    async fn stop(&mut self) -> Result<()>;
    async fn snapshot(&mut self, ctx: SnapshotContext, sink: Sender<ChangeEvent>) -> Result<ConnectorOffset>;
    async fn stream(&mut self, ctx: StreamContext, sink: Sender<ChangeEvent>) -> Result<()>;
}
```

Connectors are feature-gated:
- `postgres` — `tokio-postgres` with `pgoutput` logical replication, retry with backoff
- `mysql` — Custom TCP-level binlog client with SHA1 auth, retry with backoff
- `mongodb` — Official `mongodb` crate v3.7 with `rustls-tls` and change streams, retry with backoff

All three connectors support configurable `max_reconnect_attempts` (default 5) with exponential backoff (500ms × attempt number) via `connect_with_retry()`.

Each connector has its own submodule with `config`, `snapshot`, and `stream` components.

### Layer 4: Pipeline (`opencdc-pipeline`)

The `Pipeline` orchestrates the data flow from a connector through transforms and sinks:

- Creates a `channel(1024)` for events
- Manages connector lifecycle (snapshot → stream)
- Applies transforms in order (each receives `ChangeEvent`, returns `Option<ChangeEvent>`)
- Fans out events to all registered sinks
- Supports graceful stop via `stop()` method

Built-in transforms:
- `FilterTransform` — Filter by operation type or snapshot phase
- `RenameTransform` — Remap database and table names
- `LogTransform` — Log events at debug level

Built-in sinks:
- `StdoutSink` — Print events to stdout
- `NullSink` — Count and discard events
- `OpenLakeSink` — Write to OpenLake's in-memory `TableStore` as Arrow RecordBatches
- `RestCatalogSink` — Persistent store via Iceberg REST catalog

### Layer 5: Configuration (`opencdc-config`)

TOML-based declarative configuration:

```rust
AppConfig::from_file("config.toml")?;
// or
AppConfig::from_toml(toml_str)?;
```

`SourceConfig` enum with `Postgres`/`MySql`/`MongoDb` variants, each deserializable from TOML.
`PipelineConfig` with optional `Vec<TransformConfig>` and `Vec<SinkConfig>`.

### Layer 6: Interfaces (`opencdc-mcp`, `opencdc-demo`)

**opencdc-mcp** — MCP server binary using `rmcp` v2 SDK:
- Stdio transport (compatible with MCP hosts like Claude Desktop, VS Code)
- 7 tools: `connector_list`, `connector_register`, `connector_remove`, `connector_status`, `snapshot_start`, `schema_registry_subjects`, `schema_registry_get`
- `AppState` shared state with `RwLock<HashMap<String, ManagedConnector>>`, atomic counters, health status
- Rate limiting via `tokio::sync::Semaphore` (configurable `OPENCDC_MAX_CONCURRENT_TOOLS`, default 10)
- Signal handling via `tokio::signal::ctrl_c()` in `tokio::select!`
- Optional health HTTP server (`/health` JSON, `/metrics` Prometheus text) with CORS headers, OPTIONS preflight, and standardized JSON errors
- Schema registry tools wired to real `SchemaRegistryClient` via `OPENCDC_SCHEMA_REGISTRY_URL` env var
- `snapshot_start` runs a background task and properly tracks state

**opencdc-demo** — End-to-end demonstration binary with a `DemoConnector` (generates sample events) and full pipeline.

## Backpressure Design

All event channels use bounded `tokio::sync::mpsc` with capacity 1024:

- `Connector::snapshot()` and `Connector::stream()` receive `Sender<ChangeEvent>` (not `UnboundedSender`)
- `Pipeline` creates `channel(1024)` pairs
- `sink.send(event).await` blocks the producer when the channel is full
- `sink.send(event).await.is_err()` detects receiver drop

This ensures that slow sinks backpressure all the way to the source connector, preventing unbounded memory growth.

## Graceful Shutdown

1. SIGINT triggers `tokio::signal::ctrl_c()`
2. `AppState::shutdown_all()` sets all connector statuses to `Stopped`
3. Active sink channels are cleared
4. Server waits for clean teardown

## MCP Integration

The MCP server uses `rmcp` v2 with `ServiceExt::serve()` returning a `ServerHandle`. The handle's `waiting()` method is used in `tokio::select!` alongside the signal handler. Tools are implemented as async handlers on `McpService` which holds a reference-counted `AppState`.

## Error Handling

- `opencdc-core::Error` — Unified error enum with `From` impls for `ArrowError`, `serde_json::Error`, `String`, `&str`
- `opencdc-pipeline::PipelineError` — Wraps core errors with pipeline-specific variants (`Sink`, `Transform`, `Source`, `Stopped`)
- Connectors return `opencdc_core::Result<T>` for all public methods
- `tracing` crate used throughout (no `eprintln!`)

## Logging

- `tracing` crate used throughout (no `eprintln!`)
- Structured JSON logging available via `OPENCDC_LOG_FORMAT=json` env var
- JSON format uses `tracing-subscriber` with `JsonStorageLayer`

## Pre-commit Hook

A `.githooks/pre-commit` hook runs `cargo fmt --check` → `cargo clippy --workspace --all-features -- -D warnings` → `cargo test --workspace`. Install via `git config core.hooksPath .githooks`.

## Release Workflow

A GitHub Actions workflow (`.github/workflows/release.yml`) is triggered on `v*` tags:
- Builds all workspace crates with all features
- Runs the full test suite
- Builds a Docker image and pushes to GHCR
- Creates a GitHub release

## Docker Support

Multi-stage `Dockerfile` using `cargo-chef` for layer caching and `gcr.io/distroless/cc` runtime image. Build with `docker build -t opencdc-mcp .`.

## Safety Audit

All known data-corruption and panic bugs have been fixed:

| Bug | File | Fix |
|-----|------|-----|
| BIT(64) high-bit truncation | `mysql/column.rs` | Use `val.to_string()` for values > i64::MAX |
| JSON UTF-8 corruption | `mysql/column.rs` | `from_utf8_lossy` instead of `from_utf8().unwrap()` |
| DECIMAL UTF-8 corruption | `mysql/column.rs` | `from_utf8_lossy` instead of `from_utf8().unwrap()` |
| MySQL binlog null-terminator missing | `mysql/binlog.rs` | Return `Err` instead of empty names |
| Postgres WAL negative length | `postgres/decoder.rs` | Range check before `as usize` cast |
| Postgres LSN hex parse silent zero | `postgres/snapshot.rs` | Propagate parse errors |
| Postgres UTF-8 data loss | `postgres/types.rs` | `from_utf8_lossy` instead of `from_utf8().unwrap()` |
| Schema Registry field validation | `schema/registry.rs` | Replace `unwrap_or(0/""/"AVRO")` with `ok_or_else` |
| OpenLakeSink serde silent corruption | `sink-openlake/lib.rs` | Log `tracing::warn` on serialization failure |
| MCP semaphore panic on shutdown | `mcp/state.rs` | Return `Option` instead of `.expect()` |

## Dependencies & TLS

- No `openssl` or `native-tls` — all TLS via `rustls-tls`
- `reqwest` 0.12 with `rustls-tls` feature
- `mongodb` 3.7 with `rustls-tls` feature
- Arrow 57.x workspace-wide
