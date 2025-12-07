# LDBC SNB Interactive for TinkerPop

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)</br>
[![Zulip](https://img.shields.io/badge/Zulip-50ADFF?style=for-the-badge&logo=Zulip&logoColor=white)](https://youtrackdb.zulipchat.com/)

A Gremlin-based implementation of the [LDBC Social Network Benchmark (SNB) Interactive v1](https://ldbcouncil.org/benchmarks/snb-interactive/) workload for TinkerPop-compatible graph databases.

Created and maintained by the [YouTrackDB](https://github.com/JetBrains/youtrackdb) team at JetBrains.

## What This Is

- 29 Gremlin traversals implementing all LDBC SNB Interactive queries (14 complex reads, 7 short reads, 8 updates)
- Pluggable architecture for TinkerPop-compatible databases
- LDBC Driver integration for validation and benchmarking

**Status:** Public Preview - gathering feedback on Gremlin query patterns and approach.

## Requirements

- Java 21+
- Maven 3.8+
- TinkerPop 3.8.0 compatible database

## Project Structure

```
common/     # Database-agnostic Gremlin queries and schema
runner/     # LDBC driver integration
ytdb/       # YouTrackDB reference implementation
```

## Getting Started

### 1. Build

```bash
./mvnw clean package
```

### 2. Download Dataset

```bash
./scripts/fetch-test-data.sh
```

Downloads SF 0.1 dataset (initial snapshot, substitution parameters, update streams) in CSV format.

See [docs/dataset-overview.md](docs/dataset-overview.md) for details on dataset types and sources.

### 3. Load Data

Load the initial snapshot into your database. This is vendor-specific.

Reference: `ytdb/src/main/java/com/youtrackdb/ldbc/ytdb/loader/`

Dataset location: `test-data/runtime/social-network/sf0.1/`

### 4. Configure

Set your database vendor in `runner/ldbc-driver.properties`:

```properties
tinkerpop.vendor=ytdb
```

Default configuration:
- Validation mode
- SF 0.1
- 200 validation queries
- Single thread

See [docs/configuration-and-running.md](docs/configuration-and-running.md) for configuration details.

### 5. Run Validation

```bash
./scripts/ldbc-driver.sh
```

Results written to `runner/results/`.

**Note:** Reload data after each run (updates modify the dataset).

## Adding a Database Vendor

To add support for your TinkerPop-compatible database:

1. Create a vendor module (e.g., `yourdb/`)
2. Implement `GraphProvider` interface
3. Create a Guice module binding your implementation
4. Register vendor in `TinkerPopDb.java`
5. Add vendor properties in `yourdb/ldbc-driver.properties`

Optionally override default queries for vendor-specific optimizations.

Complete guide: [docs/adding-a-vendor.md](docs/adding-a-vendor.md)

Reference implementation: `ytdb/` module

## Documentation

- [Dataset Overview](docs/dataset-overview.md) - Data types, sources, and structure
- [Adding a Vendor](docs/adding-a-vendor.md) - Step-by-step database integration
- [Configuration and Running](docs/configuration-and-running.md) - Configuration parameters and execution modes

## Community

- Questions/feedback: [Zulip](https://youtrackdb.zulipchat.com/)
- Issues/features: [YouTrack](https://youtrack.jetbrains.com/issues/YTDB)

Feedback welcome on:
- Query implementations and optimizations
- API design for database plugins
- Documentation gaps

## Related Projects

- [LDBC SNB Interactive Reference Implementations](https://github.com/ldbc/ldbc_snb_interactive_v1_impls)
- [LDBC SNB Interactive Driver](https://github.com/ldbc/ldbc_snb_interactive_v1_driver)
- [LDBC SNB Data Generator](https://github.com/ldbc/ldbc_snb_datagen_hadoop)

## License

Apache License 2.0 - see [LICENSE](LICENSE)

## Acknowledgments

- [LDBC Council](https://ldbcouncil.org/) for the SNB benchmark specification
- [Apache TinkerPop](https://tinkerpop.apache.org/) for the graph computing framework
