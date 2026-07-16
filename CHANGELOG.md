# Changelog

## 0.1.0 (Unreleased)

### Phase 1: Foundation

- Workspace with 9 crates, edition 2024, Apache 2.0 license
- `opencdc-core` with core types: ChangeEvent, Operation, SourceInfo, ConnectorOffset, DebeziumSchema, DebeziumArrowMapper, Error
- `opencdc-serde` with JSON/Arrow IPC/Avro/Schema Registry serialization and BatchConverter
- `opencdc-schema` with SchemaBuilder, SchemaRegistryClient, SchemaBridge, SchemaEvolution, DebeziumNaming
- `opencdc-connector` with Connector trait and Postgres (pgoutput), MySQL (binlog), MongoDB (change streams) backends
- `opencdc-pipeline` with Pipeline runtime, Sink/Transform traits, StdoutSink, NullSink, FilterTransform, RenameTransform, LogTransform
- `opencdc-sink-openlake` with OpenLakeSink (in-memory TableStore) and RestCatalogSink (Iceberg REST catalog)
- `opencdc-demo` end-to-end demo binary
- Postgres 16, MySQL 8, MongoDB 7 Docker Compose services with profile-based selection
- Init scripts for database CDC setup

### Phase 1.4: Graceful Shutdown

- `AppState::shutdown_all()` method
- SIGINT handling in MCP `main.rs` via `tokio::signal::ctrl_c()` + `tokio::select!`

### Phase 1.5: Backpressure

- All unbounded channels migrated to bounded `Sender<ChangeEvent>` with capacity 1024
- Connector trait, pipeline, demo, and MCP state updated

### Phase 1 CI

- GitHub Actions CI workflow with cargo check/clippy/fmt/test/doc on push and PR
- `.github/workflows/ci.yml`

### Phase 2: Test Coverage

- 101 new tests added across all crates (212 total, now 223 after safety audit)
- Zero clippy warnings
- Coverage added for: error variants, offset operations, connector type serialization, schema roundtrips, source info builders, transaction metadata, pipeline errors, postgres/mysql/mongodb config, MCP state operations, schema registry edge cases, IPC serialization, Avro schema generation

### Phase 3: Production Hardening

- `opencdc-config` crate with TOML configuration loader (`AppConfig::from_file`, `AppConfig::from_toml`)
- SourceConfig enum with Postgres/MySql/MongoDb variants
- PipelineConfig with transform and sink configuration
- Metrics counters on AppState (`AtomicU64` for events received/sent/errors)
- HealthStatus struct with JSON serialization
- Health endpoint HTTP server (`/health` JSON, `/metrics` Prometheus text) via `OPENCDC_HEALTH_PORT` env var
- Postgres connector reconnect with configurable retry attempts and exponential backoff
- `eprintln!` replaced with `tracing::error!` in Postgres connector

### Phase 4: Documentation

- README.md with project overview, features, architecture diagram, quick start, configuration guide
- ARCHITECTURE.md with layered crate design, data flow, backpressure, graceful shutdown, MCP integration
- CONTRIBUTING.md with setup, build/test/lint commands, code style, PR process
- CHANGELOG.md with phased release history

### Deep Scan: Bug Fixes

- **BIT(64) high-bit truncation** — `mysql/column.rs`: values > i64::MAX now serialized as string to avoid negative JSON numbers
- **JSON UTF-8 corruption** — `mysql/column.rs`: `from_utf8_lossy` preserves data with replacement characters
- **DECIMAL UTF-8 corruption** — `mysql/column.rs`: `from_utf8_lossy` preserves data with replacement characters
- **MySQL binlog null-terminator missing** — `mysql/binlog.rs`: returns clear `Err` instead of silently producing empty table/column names
- **Postgres WAL negative length** — `postgres/decoder.rs`: explicit `>= 0` range check before `as usize` cast prevents OOM on corrupt WAL
- **Postgres LSN hex parse silent zero** — `postgres/snapshot.rs`: propagates parse errors instead of silently returning LSN 0
- **Postgres UTF-8 data loss** — `postgres/types.rs`: `from_utf8_lossy` preserves column data
- **Schema Registry field validation** — `schema/registry.rs`: replaces `unwrap_or(0/""/"AVRO")` with proper `ok_or_else` error propagation
- **OpenLakeSink serde silent corruption** — `sink-openlake/lib.rs`: logs `tracing::warn` on serialization failure
- **MCP semaphore panic on shutdown** — `mcp/state.rs`: returns `Option` instead of `.expect("semaphore closed")` for graceful shutdown
- **Health endpoint dropped errors** — `mcp/health.rs`: logs write/shutdown failures via `tracing::warn`

### Phase 5: Security & Polish

- REST catalog auth: bearer token support in `RestCatalogSinkConfig` with `auth_token` field
- Rate limiting: semaphore-based concurrent tool execution in MCP (max 10 concurrent, configurable via `OPENCDC_MAX_CONCURRENT_TOOLS`)
- CORS headers on health endpoint (`Access-Control-Allow-Origin: *`) with OPTIONS preflight handling
- Standardized JSON error responses on health endpoint (`{"error": "...", "message": "..."}` format)
- Multi-stage Dockerfile for `opencdc-mcp` using `cargo-chef` for layer caching, distroless cc runtime image
- `.dockerignore` for efficient builds
- Audit confirmed no hardcoded secrets in production code
- MySQL connector reconnect with configurable `max_reconnect_attempts` and exponential backoff
- MongoDB connector reconnect with configurable `max_reconnect_attempts` and exponential backoff
- MCP `schema_registry_subjects` and `schema_registry_get` tools now use real `SchemaRegistryClient` (configured via `OPENCDC_SCHEMA_REGISTRY_URL` env var)
- MCP `snapshot_start` now runs a background snapshot task and properly tracks state
- Pre-commit hook (`.githooks/pre-commit`) with fmt, clippy, test checks
- GitHub release workflow (`.github/workflows/release.yml`) triggered on `v*` tags
- JSON structured logging support via `OPENCDC_LOG_FORMAT=json` env var
- Empty `benchmarks/` and `docs/` directories removed
