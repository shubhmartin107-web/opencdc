# syntax=docker/dockerfile:1
FROM rust:1.85-slim-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/opencdc-core/Cargo.toml crates/opencdc-core/
COPY crates/opencdc-serde/Cargo.toml crates/opencdc-serde/
COPY crates/opencdc-schema/Cargo.toml crates/opencdc-schema/
COPY crates/opencdc-connector/Cargo.toml crates/opencdc-connector/
COPY crates/opencdc-mcp/Cargo.toml crates/opencdc-mcp/
COPY crates/opencdc-pipeline/Cargo.toml crates/opencdc-pipeline/
COPY crates/opencdc-sink-openlake/Cargo.toml crates/opencdc-sink-openlake/
COPY crates/opencdc-config/Cargo.toml crates/opencdc-config/
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --recipe-path recipe.json --release

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/ crates/
RUN cargo build -p opencdc-mcp --release

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
COPY --from=builder /app/target/release/opencdc-mcp /usr/local/bin/opencdc-mcp
EXPOSE 9090
ENV OPENCDC_HEALTH_PORT=9090
ENTRYPOINT ["opencdc-mcp"]
