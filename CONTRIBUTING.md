# Contributing to OpenCDC

## Getting Started

1. Ensure you have Rust 1.85+ installed (edition 2024)
2. Clone the repository
3. Run `make build` to verify the workspace compiles
4. Run `make test` to verify all tests pass
5. Run `make lint` to check for clippy warnings
6. Run `make fmt` to format code

## Development Workflow

### Building

```bash
# Build everything
cargo build --workspace

# Build individual crates
cargo build -p opencdc-core
cargo build -p opencdc-connector --features postgres,mysql,mongodb
cargo build -p opencdc-mcp
```

### Testing

```bash
# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p opencdc-mcp

# Run a specific test
cargo test -- connector_lifecycle

# Run tests with feature flags
cargo test -p opencdc-connector --features postgres,mysql,mongodb
```

### Linting & Formatting

```bash
# Check for clippy warnings (zero warnings goal)
cargo clippy --workspace --all-features -- -D warnings

# Format code
cargo fmt --all
```

Keep the codebase at zero clippy warnings and properly formatted at all times.

### Pre-commit Hook

Install the pre-commit hook to automatically check formatting, linting, and tests before each commit:

```bash
git config core.hooksPath .githooks
```

The hook runs `cargo fmt --check` → `clippy -- -D warnings` → `cargo test --workspace`.

### Docker Build

```bash
docker build -t opencdc-mcp .
```

## Code Style

- Edition 2024 with default formatting (use `cargo fmt`)
- No `unwrap()` or `expect()` in production code — use proper error handling with `?` or match
- No `eprintln!()` — use `tracing::info!`, `tracing::warn!`, `tracing::error!`
- Follow existing patterns: builder pattern for configs, async traits for connectors/transforms/sinks
- Use `thiserror` for error enums, `serde` for serialization, `async_trait` for async traits
- Avoid `openssl`/`native-tls` — use `rustls-tls` for all TLS

## Pull Request Process

1. Create a feature branch from `main`
2. Make your changes with clear commit messages
3. Ensure all tests pass with `cargo test --workspace`
4. Ensure zero clippy warnings with `cargo clippy --workspace --all-features -- -D warnings`
5. Ensure code is formatted with `cargo fmt --all`
6. Open a PR with a description of your changes

## Adding Tests

All new code should include tests. Follow the existing test patterns:

- Unit tests in a `#[cfg(test)] mod tests` block at the bottom of the source file
- Use `#[tokio::test]` for async tests
- Test both success and error paths
- Cover edge cases (empty inputs, invalid data, boundary conditions)
- Use `pretty_assertions` for readable assertions

## Adding a New Connector

1. Add a new module under `crates/opencdc-connector/src/` (e.g., `sqlserver/`)
2. Create `config.rs`, `snapshot.rs`, `stream.rs` submodules
3. Implement the `Connector` trait
4. Gate behind a Cargo feature flag
5. Add connector config variant to `opencdc-config/src/source.rs`
6. Add tests matching the existing connector test patterns

## Adding a New Sink

1. Implement the `Sink` trait from `opencdc-pipeline`
2. Add to `opencdc-config/src/pipeline.rs` if config-driven

## Adding a New Transform

1. Implement the `Transform` trait from `opencdc-pipeline`
2. Add to `opencdc-config/src/pipeline.rs` if config-driven

## Project Structure

```
opencdc/
├── Cargo.toml              # Workspace root
├── Makefile                # Build shortcuts
├── docker-compose.yml      # Database services
├── scripts/                # Init scripts (SQL, JS)
├── crates/
│   ├── opencdc-core/       # Core types
│   ├── opencdc-serde/      # Serialization
│   ├── opencdc-schema/     # Schema management
│   ├── opencdc-connector/  # Connector implementations
│   ├── opencdc-pipeline/   # Pipeline runtime
│   ├── opencdc-mcp/        # MCP server
│   ├── opencdc-sink-openlake/  # OpenLake sink
│   ├── opencdc-demo/       # Demo binary
│   └── opencdc-config/     # TOML configuration
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENCDC_HEALTH_PORT` | (none) | Port for health/metrics HTTP server |
| `OPENCDC_MAX_CONCURRENT_TOOLS` | `10` | Max concurrent MCP tool executions |
| `OPENCDC_SCHEMA_REGISTRY_URL` | (none) | Confluent Schema Registry base URL |
| `OPENCDC_SCHEMA_REGISTRY_TOKEN` | (none) | Bearer token for schema registry auth |
| `OPENCDC_LOG_FORMAT` | (none) | Set to `json` for structured JSON logging |

## Release Process

1. Tag the commit with a version tag (`v0.1.0`, `v0.2.0`, etc.)
2. Push the tag to trigger the release workflow (`.github/workflows/release.yml`)
3. The workflow builds, tests, creates a Docker image, and publishes a GitHub release

## Getting Help

- Open an issue on GitHub for bugs, feature requests, or questions
- For design discussions, open a discussion on GitHub
