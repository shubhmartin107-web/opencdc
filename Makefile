.PHONY: all build test lint fmt clean ci

all: build lint test

# Build all crates with all features
build:
	cargo build --workspace --all-features

# Build release
build-release:
	cargo build --workspace --all-features --release

# Run all tests
test:
	cargo test --workspace --all-features

# Lint (clippy)
lint:
	cargo clippy --workspace --all-features -- -D warnings

# Format code
fmt:
	cargo fmt --all

# Check formatting
fmt-check:
	cargo fmt --all --check

# Clean build artifacts
clean:
	cargo clean

# Full CI check (lint + test)
ci: lint test

# Run individual crate tests
test-core:
	cargo test -p opencdc-core

test-serde:
	cargo test -p opencdc-serde

test-schema:
	cargo test -p opencdc-schema

test-connector:
	cargo test -p opencdc-connector --all-features

test-mcp:
	cargo test -p opencdc-mcp

test-pipeline:
	cargo test -p opencdc-pipeline

test-sink-openlake:
	cargo test -p opencdc-sink-openlake

test-demo:
	cargo test -p opencdc-demo

# Run MCP server
run-mcp:
	cargo run -p opencdc-mcp

# Run demo
run-demo:
	cargo run -p opencdc-demo

# Generate rustdoc
doc:
	cargo doc --workspace --all-features --no-deps

# Open rustdoc in browser
doc-open:
	cargo doc --workspace --all-features --no-deps --open
