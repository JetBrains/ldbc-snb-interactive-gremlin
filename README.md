# LDBC SNB Interactive for TinkerPop

A Gremlin-based implementation of the [LDBC Social Network Benchmark (SNB) Interactive v1](https://ldbcouncil.org/benchmarks/snb-interactive/) workload for TinkerPop-compatible graph databases.

## Overview

This project provides:
- **29 Gremlin traversals** implementing all LDBC SNB Interactive queries (14 complex reads, 7 short reads, 8 updates)
- **Pluggable architecture** - add your TinkerPop-compatible database with minimal code
- **LDBC Driver integration** for validation and benchmarking

## Project Status

**Public Preview** - We're sharing this implementation to gather feedback from the TinkerPop community on the Gremlin query patterns and overall approach. Contributions and suggestions welcome!

## Requirements

- Java 21+
- Maven 3.8+
- TinkerPop 3.8.0 compatible database

## Project Structure

```
├── common/     # Shared Gremlin queries and schema (database-agnostic)
├── runner/     # LDBC driver integration
└── ytdb/       # YouTrackDB implementation (reference example)
```

## Quick Start

### 1. Build
```bash
mvn clean package -DskipTests
```

### 2. Load Data
Download an LDBC SNB dataset from [ldbcouncil.org](https://ldbcouncil.org/data-sets-surf-repository/) and configure your loader (vendor-specific).

### 3. Run Validation
Vendor-specific, YTDB example:
```bash
cd ytdb/driver
./validate.sh
```

### 4. Run Benchmark
Vendor-specific, YTDB example:
```bash
cd ytdb/driver
./benchmark.sh
```

## Adding a New Database

1. Create a new module (e.g., `your_db/`)
2. Implement `GraphProvider` interface
3. Create a Guice module binding your provider
4. Optionally override queries in `DefaultQueryModule` for database-specific optimizations

See `ytdb/` module for a complete example.

## Configuration

Key properties in `driver/*.properties`:

| Property | Description |
|----------|-------------|
| `tinkerpop.vendor` | Database identifier (e.g., `ytdb`) |
| `ldbc.snb.interactive.scale_factor` | Dataset size: 0.1, 0.3, 1, 3, 10, 30, 100 |
| `thread_count` | Concurrent benchmark threads |
| `operation_count` | Number of operations to execute |

## Feedback

This is an early release focused on query correctness and Gremlin patterns. We welcome feedback on:
- Query implementations and optimizations
- API design for database plugins
- Missing features or documentation

Please open an issue or discussion on GitHub.

## License

Apache License 2.0 - see [LICENSE](LICENSE)

## Related Projects

- [LDBC SNB Interactive Reference Implementations](https://github.com/ldbc/ldbc_snb_interactive_v1_impls) - Official implementations for Neo4j, PostgreSQL, and other databases
- [LDBC SNB Interactive Driver](https://github.com/ldbc/ldbc_snb_interactive_v1_driver) - The benchmark driver used by this project

## Acknowledgments

- [LDBC Council](https://ldbcouncil.org/) for the SNB benchmark specification and reference implementations
- [Apache TinkerPop](https://tinkerpop.apache.org/) for the graph computing framework
