# YouTrackDB LDBC Loader

A standalone bulk loader for importing [LDBC SNB](https://ldbcouncil.org/benchmarks/snb/) (Social Network Benchmark) data into YouTrackDB.

## Why This Exists

YouTrackDB currently does not provide built-in bulk loading functionality. While the Gremlin API works well for transactional workloads, loading millions of vertices and edges one-by-one through standard graph traversals is slow.

This loader addresses that gap by:
- Batching inserts into configurable transaction sizes (default: 50,000 records)
- Processing CSV files in a streaming fashion to handle large datasets
- Creating the full LDBC SNB schema (vertex types, edge types, indexes)
- Supporting both embedded and remote YouTrackDB connections

## Building

### Maven (local)

```bash
cd ytdb-loader
mvn package -DskipTests
java -jar target/ytdb-loader-1.0-SNAPSHOT.jar
```

### Docker

```bash
cd ytdb-loader
docker build -t ytdb-loader .
```

## Usage

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `YTDB_MODE` | `embedded` | `embedded` or `remote` |
| `YTDB_DATA_DIR` | `scratch/data` | Path for embedded database |
| `YTDB_SERVER_HOST` | `localhost` | Remote server host |
| `YTDB_SERVER_PORT` | `8182` | Remote server port |
| `YTDB_SERVER_USER` | `root` | Server authentication user |
| `YTDB_SERVER_PASSWORD` | `root` | Server authentication password |
| `YTDB_DATABASE_NAME` | `ldbc_snb` | Database name to create |
| `YTDB_DATABASE_USER` | `admin` | Database user |
| `YTDB_DATABASE_PASSWORD` | `admin` | Database password |
| `YTDB_TEST_DATA_DIR` | (from properties) | Path to LDBC dataset |

### Running with Docker

```bash
# Load data into a remote YouTrackDB instance
docker run --rm \
  -v /path/to/ldbc-snb-data:/data \
  -e YTDB_MODE=remote \
  -e YTDB_SERVER_HOST=host.docker.internal \
  -e YTDB_TEST_DATA_DIR=/data \
  ytdb-loader
```

### Dataset Structure

The loader expects LDBC SNB Interactive data in the standard format:

```
dataset/
├── static/
│   ├── place_0_0.csv
│   ├── organisation_0_0.csv
│   ├── tagclass_0_0.csv
│   ├── tag_0_0.csv
│   └── ... (relationship files)
└── dynamic/
    ├── person_0_0.csv
    ├── forum_0_0.csv
    ├── post_0_0.csv
    ├── comment_0_0.csv
    └── ... (relationship files)
```

You can generate test data using the [LDBC SNB Data Generator](https://github.com/ldbc/ldbc_snb_datagen_spark).

## Schema

The loader creates the full LDBC SNB schema including:

**Vertex Types:** Person, Place, Organisation, TagClass, Tag, Forum, Post, Comment

**Edge Types:** KNOWS, IS_LOCATED_IN, HAS_INTEREST, STUDY_AT, WORK_AT, HAS_MODERATOR, HAS_MEMBER, CONTAINER_OF, HAS_TAG, HAS_CREATOR, LIKES, REPLY_OF, IS_PART_OF, IS_SUBCLASS_OF, HAS_TYPE

**Indexes:** Unique indexes on all entity IDs, plus query-optimized indexes on frequently filtered properties.

## License

Apache License 2.0
