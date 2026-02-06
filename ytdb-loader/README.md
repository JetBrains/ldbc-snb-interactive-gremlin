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
| `YTDB_DATASET_PATH` | `/data` | Path to LDBC CSV dataset (client-side, in loader container) |
| `YTDB_BACKUP_PATH` | (not set) | Server-side backup path (in DB container, optional) |

### Backup/Restore Behavior

The `YTDB_BACKUP_PATH` variable controls backup/restore functionality:

| `YTDB_BACKUP_PATH` | Behavior |
|--------------------|----------|
| **Not set** | Load from CSVs only, no backup created |
| **Set** | Try restore from backup; on failure, load CSVs and create backup |

**Important:** The backup path is a **server-side path**. In remote mode, the path is interpreted by the database server, not the loader client. Make sure the server has a volume mounted at this path.

### Running with Docker (Remote Mode)

```bash
# Start YouTrackDB server with backup volume
docker run -d --name ytdb-server \
  -p 8182:8182 \
  -v /host/path/to/backup:/backup \
  youtrackdb/youtrackdb-server

# Run loader - first time loads CSVs and creates backup
docker run --rm \
  -v /path/to/ldbc-snb-data:/data:ro \
  -e YTDB_MODE=remote \
  -e YTDB_SERVER_HOST=host.docker.internal \
  -e YTDB_DATASET_PATH=/data \
  -e YTDB_BACKUP_PATH=/backup \
  ytdb-loader

# Subsequent runs: instant restore from backup
docker run --rm \
  -v /path/to/ldbc-snb-data:/data:ro \
  -e YTDB_MODE=remote \
  -e YTDB_SERVER_HOST=host.docker.internal \
  -e YTDB_DATASET_PATH=/data \
  -e YTDB_BACKUP_PATH=/backup \
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

## Backup/Restore Optimization

For large scale factors (SF1+), loading from CSV can take significant time. The loader supports backup/restore to speed up subsequent loads:

1. **First load**: Parses CSVs, creates schema, inserts data, then creates a backup
2. **Subsequent loads**: Attempts restore from backup, instantly recreating the database

This is particularly useful for:
- Development/testing cycles where you need a fresh database state
- Benchmark reruns without re-parsing gigabytes of CSV data
- CI/CD pipelines that need consistent, fast database initialization

## Schema

The loader creates the full LDBC SNB schema including:

**Vertex Types:** Person, Place, Organisation, TagClass, Tag, Forum, Post, Comment

**Edge Types:** KNOWS, IS_LOCATED_IN, HAS_INTEREST, STUDY_AT, WORK_AT, HAS_MODERATOR, HAS_MEMBER, CONTAINER_OF, HAS_TAG, HAS_CREATOR, LIKES, REPLY_OF, IS_PART_OF, IS_SUBCLASS_OF, HAS_TYPE

**Indexes:** Unique indexes on all entity IDs, plus query-optimized indexes on frequently filtered properties.

## License

Apache License 2.0
