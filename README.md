# OpenCDC

[![CI](https://github.com/anomalyco/opencdc/actions/workflows/ci.yml/badge.svg)](https://github.com/anomalyco/opencdc/actions/workflows/ci.yml)
[![Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**OpenCDC** is a low-latency, log-based Change Data Capture (CDC) system written in Rust. It supports Postgres, MySQL, and MongoDB sources and produces events in the Debezium-compatible message format with Arrow-native processing.

## Key Features

- **Pluggable connectors** — Postgres (pgoutput logical replication), MySQL (binlog), MongoDB (change streams)
- **Debezium-compatible** — Message format, envelope schemas, Confluent Schema Registry API, Kafka Connect SMT equivalents
- **Arrow-native** — Events are represented as Apache Arrow RecordBatches for zero-copy columnar processing
- **Pipeline architecture** — Chain transforms and fan-out to multiple sinks per connector
- **MCP server** — Expose CDC operations via the Model Context Protocol (stdio transport)
- **Backpressure** — Bounded channels (capacity 1024) across the entire data path
- **Graceful shutdown** — SIGINT handling with clean connector teardown
- **Health endpoint** — Lightweight HTTP server for `/health` (JSON) and `/metrics` (Prometheus) with CORS
- **Rate limiting** — Semaphore-based concurrent MCP tool execution (configurable via `OPENCDC_MAX_CONCURRENT_TOOLS`)
- **Reconnect with backoff** — All connectors support configurable retry attempts and exponential backoff
- **TOML configuration** — Declarative config for connectors, transforms, and sinks
- **OpenLake integration** — Write CDC events to OpenLake's TableStore via in-memory or Iceberg REST catalog sink (with optional bearer auth)
- **Structured logging** — JSON log output via `OPENCDC_LOG_FORMAT=json`
- **Docker** — Multi-stage Dockerfile with cargo-chef caching and distroless runtime image

## Architecture

```
┌─────────────┐    ┌───────────────┐    ┌─────────────┐
│  Postgres   │───▶│               │    │  StdoutSink │
├─────────────┤    │  Pipeline     │───▶├─────────────┤
│   MySQL     │───▶│  + transforms │    │ NullSink    │
├─────────────┤    │  + sinks      │───▶├─────────────┤
│  MongoDB    │───▶│               │    │ OpenLake    │
└─────────────┘    └───────────────┘    └─────────────┘
                          │
                    ┌─────▼─────┐
                    │  MCP      │
                    │  Server   │
                    └───────────┘
```

### Crates

| Crate | Description |
|-------|-------------|
| `opencdc-core` | Core types: ChangeEvent, Operation, SourceInfo, DebeziumSchema, ConnectorOffset, Arrow mapping |
| `opencdc-serde` | Serialization: Debezium JSON, Avro schema gen, Arrow IPC, Schema Registry wire format |
| `opencdc-schema` | Schema management: builder, Confluent Schema Registry client, Arrow↔Debezium bridge, evolution |
| `opencdc-connector` | Connector trait + Postgres (`postgres`), MySQL (`mysql`), MongoDB (`mongodb`) backends |
| `opencdc-pipeline` | Pipeline runtime: Sink/Transform traits, StdoutSink, NullSink, Filter/Rename transforms |
| `opencdc-mcp` | MCP server exposing CDC tools via stdio transport |
| `opencdc-sink-openlake` | OpenLake sink (in-memory TableStore + persistent Iceberg REST catalog) |
| `opencdc-config` | TOML configuration loader for all connector types and pipeline config |

## Quick Start

### Prerequisites

- Rust 1.85+ (edition 2024)
- Docker & Docker Compose (for database services)
- OpenLake (for OpenLake sink — optional)

### Clone & Build

```bash
git clone https://github.com/anomalyco/opencdc.git
cd opencdc
make build
```

### Start Database Services

```bash
# Start all databases
docker compose --profile all up -d

# Or start individually
docker compose --profile postgres up -d
docker compose --profile mysql up -d
docker compose --profile mongodb up -d
```

### Run Tests

```bash
make test
# All 223 tests pass with zero clippy warnings.
```

### Install Pre-commit Hook

```bash
git config core.hooksPath .githooks
```

### Docker Build

```bash
docker build -t opencdc-mcp .
```

### Run MCP Server

```bash
OPENCDC_HEALTH_PORT=9090 cargo run -p opencdc-mcp
```

### Run Demo

```bash
cargo run -p opencdc-demo
```

## Configuration

OpenCDC supports TOML configuration files:

```toml
[connector]
type = "postgres"
name = "my-pg-connector"
host = "localhost"
port = 5432
database = "mydb"
username = "postgres"
password = "secret"
slot_name = "opencdc_slot"
publication = "opencdc_publication"

[pipeline]
transforms = [
  { type = "rename", table_remap = { "users" = "cdc_users" } },
  { type = "exclude_snapshot" }
]
sinks = [
  { type = "stdout" },
  { type = "openlake", namespace = "cdc", catalog_url = "http://localhost:8181" }
]
```

```rust
use opencdc_config::AppConfig;

let config = AppConfig::from_file("config.toml")?;
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENCDC_HEALTH_PORT` | (none) | Port for health/metrics HTTP server (`/health`, `/metrics`) |
| `OPENCDC_MAX_CONCURRENT_TOOLS` | `10` | Max concurrent MCP tool executions |
| `OPENCDC_SCHEMA_REGISTRY_URL` | (none) | Confluent Schema Registry base URL |
| `OPENCDC_SCHEMA_REGISTRY_TOKEN` | (none) | Bearer token for schema registry auth |
| `OPENCDC_LOG_FORMAT` | (none) | Set to `json` for structured JSON logging |

## Connectors

### Postgres

Uses `pgoutput` logical replication plugin. Requires:
- `wal_level = logical` in `postgresql.conf`
- A replication slot and publication (auto-created)

Configuration: `host`, `port`, `database`, `username`, `password`, `slot_name`, `publication`, `table_include`, `max_reconnect_attempts`.

### MySQL

Custom TCP-level binlog client with SHA1 authentication. Requires:
- `binlog_format = ROW` and `binlog_row_image = FULL` in MySQL config
- A user with `REPLICATION SLAVE`, `REPLICATION CLIENT`, and relevant table privileges

Configuration: `host`, `port`, `user`, `password`, `database`, `server_id`, `tables`, `max_reconnect_attempts`.

### MongoDB

Uses the official `mongodb` crate v3.7 with `rustls-tls` and change streams. Requires a MongoDB replica set.

Configuration: `connection_string`, `database`, `collections`, `max_reconnect_attempts`.

## Pipeline & Transforms

Events flow through the pipeline as: **source → transforms (ordered chain) → sinks (fan-out)**

### Built-in Transforms

| Transform | Description |
|-----------|-------------|
| `FilterTransform` | Filter events by operation type (create, update, delete) or exclude snapshots |
| `RenameTransform` | Remap database and table names |
| `LogTransform` | Log events at `tracing::debug!` level |

### Built-in Sinks

| Sink | Description |
|------|-------------|
| `StdoutSink` | Print events to stdout |
| `NullSink` | Discard events (counting only) |
| `OpenLakeSink` | Write to OpenLake's TableStore |
| `RestCatalogSink` | Persistent sink via Iceberg REST catalog |

## MCP Server

The MCP server exposes CDC operations via stdio transport using the `rmcp` SDK:

| Tool | Description |
|------|-------------|
| `connector_list` | List registered connectors |
| `connector_register` | Register a new connector |
| `connector_remove` | Remove a connector |
| `connector_status` | Get connector status |
| `snapshot_start` | Run a snapshot |
| `schema_registry_subjects` | List Schema Registry subjects |
| `schema_registry_get` | Get schema by ID |

Enable optional health endpoint:

```bash
OPENCDC_HEALTH_PORT=9090 cargo run -p opencdc-mcp
```

Health endpoint serves:
- `GET /health` — JSON with uptime, connector stats, event counts, aggregate status
- `GET /metrics` — Prometheus-formatted counters (events received/sent/errors, connectors, uptime)
- CORS headers (`Access-Control-Allow-Origin: *`) and OPTIONS preflight support
- Standardized JSON error responses (`{"error": "...", "message": "..."}`)

Tool execution is rate-limited via semaphore (default 10 concurrent, configurable via `OPENCDC_MAX_CONCURRENT_TOOLS`).

## Debezium Compatibility

OpenCDC produces events compatible with the Debezium message format:

- **Envelope schema** — `before`, `after`, `source`, `op`, `ts_ms` fields
- **Operation codes** — `c` (create), `u` (update), `d` (delete), `r` (read/snapshot), `t` (truncate), `m` (message)
- **Source info** — Connector metadata matching Debezium's `SourceInfo` structure
- **Schema Registry** — Compatible with Confluent Schema Registry REST API
- **Naming conventions** — Topic names, subject names, and envelope schemas follow Debezium conventions

## License

Apache 2.0
