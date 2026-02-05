# Dataset Overview

The LDBC SNB Interactive benchmark uses four types of data:

1. **Initial Snapshot** - Static graph representing the starting state
2. **Substitution Parameters** - Input parameters for read queries
3. **Update Streams** - Time-ordered update operations applied during runs
4. **Validation Parameters** - Operation + expected result pairs for correctness checks

## Initial Snapshot

Location: `test-data/runtime/social-network/sf0.1/dynamic/` and `static/`

Contents:
- Person nodes and relationships (knows, likes, hasInterest)
- Forum nodes (hasMember, hasModerator, containerOf)
- Post and Comment nodes (hasCreator, replyOf, hasTag)
- Static reference data (Place, Organization, Tag, TagClass)

Format: CSV with pipe delimiters

**Role in the workflow:** This is the baseline dataset loaded into the database before any validation or benchmark run. 
All subsequent operations (reads and updates) assume this snapshot as the starting state.

## Substitution Parameters

Location: `test-data/runtime/social-network/sf0.1/substitution_parameters/`

One file per query type (e.g., `interactive_1_param.txt`). Each line contains input parameters for one query execution.

**What they are:** Pre-generated query inputs (IDs, time ranges, tag names, etc.) produced by Datagen. 
The driver reads these files to instantiate read operations (Q1..Q14). They do not contain any expected results.

## Update Streams

Location: `test-data/runtime/social-network/sf0.1/update_streams/` (or a directory containing `updateStream_*_{forum,person}.csv`)

Time-ordered insert operations for persons, posts, comments, likes, friendships. Multiple partitions for concurrent execution.

**Important:** Reload initial snapshot after each run (updates modify the dataset).

**What they are:** Sequential write operations that mutate the graph. The driver schedules these alongside reads, 
and for validation it can inject short reads immediately after writes to check correctness of the mutation effects.

## Validation Parameters

Location: `test-data/fixtures/social-network/sf0.1/validation_parameters/`

Files: `validation_params-sf0.1-100.csv`, `validation_params-sf0.1-200.csv`, `validation_params-sf0.1-500.csv`

Small subsets committed to repository for fast onboarding and offline development. Not for official LDBC compliance runs.

**How they are generated (create_validation mode):**
- The driver loads the initial snapshot into the DB.
- It builds a workload stream from:
  - read operations using **substitution parameters**
  - write operations using **update streams**
- It executes operations against the DB and records `(operation, result)` pairs.
- These pairs are written into `validation_params.csv`.

**Key detail:** The operation stream is capped by `operation_count`. The validation filter only **accepts** operations it still needs for coverage:
- enabled **long reads** and **writes** (per-type quotas)
- **short reads** injected after certain writes to validate mutation effects
If the stream ends before those quotas are satisfied, fewer validation parameters are produced. 
Increasing `operation_count` expands the stream and allows the generator to reach the requested validation size.

**Terminology clarification:**
- *Substitution parameters* = inputs for reads (no results).
- *Validation parameters* = inputs + expected results captured from a specific DB state.

## Limiting Validation Size

`validate_database` consumes **all rows** in the validation parameters CSV. There is no built-in setting to limit how 
many operations are validated from a large file. To validate a smaller subset, **slice the file** and point `validate_database` at the sliced file.

Pre-generated validation parameters are available from LDBC. The archive contains large files, so slicing is often necessary for quick local checks:

```
https://datasets.ldbcouncil.org/interactive-v1/validation_params-interactive-v1.0.0-sf0.1-to-sf10.tar.zst
```

## Data Sources

- **SURF Repository:** https://repository.surfsara.nl/datasets/cwi/snb/
- **LDBC Datasets:** https://datasets.ldbcouncil.org/
- **Data Generator:** https://github.com/ldbc/ldbc_snb_datagen_hadoop

## Scale Factors

| SF | Persons | Posts+Comments | Edges | Disk Size |
|----|---------|----------------|-------|-----------|
| 0.1 | ~3K | ~60K | ~200K | ~30 MB |
| 1 | ~11K | ~600K | ~2M | ~300 MB |
| 10 | ~110K | ~6M | ~20M | ~3 GB |
| 100 | ~1.1M | ~60M | ~200M | ~30 GB |

Default: SF 0.1

## Data Format

This implementation uses `csv_composite-longdateformatter`:
- **csv_composite:** Foreign keys merged into parent entities
- **longdateformatter:** Dates as Unix timestamps in milliseconds

## Directory Structure

```
test-data/
    fixtures/                          # Committed to git
        social-network/sf0.1/
            validation_parameters/
    runtime/                           # Gitignored, downloaded
        social-network/sf0.1/
            dynamic/
            static/
            substitution_parameters/
            update_streams/
```

## Getting Started

```bash
./scripts/fetch-test-data.sh     # Download data
# Load initial snapshot (vendor-specific)
./scripts/ldbc-driver.sh         # Run validation
```
