# Dataset Overview

## What Data Does This Benchmark Use?

The LDBC SNB Interactive benchmark uses four types of data:

1. **Initial Snapshot** - Static graph representing the starting state
2. **Substitution Parameters** - Input parameters for read queries
3. **Update Streams** - Dynamic data modifications during benchmark runs
4. **Validation Parameters** - Expected query results for correctness verification

## Data Categories

### Initial Snapshot

The static graph loaded before benchmark execution.

**Location:** `test-data/runtime/social-network/sf0.1/dynamic/` and `test-data/runtime/social-network/sf0.1/static/`

**Contents:**
- Person nodes and relationships (knows, likes, hasInterest)
- Forum nodes (hasMember, hasModerator, containerOf)
- Post and Comment nodes (hasCreator, replyOf, hasTag)
- Static reference data (Place, Organization, Tag, TagClass)

**Format:** CSV files with pipe delimiters

Example (`person_0_0.csv`):
```csv
id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed|language|email
933|Mahinda|Perera|male|628646400000|1266161530447|119.235.7.103|Firefox|si;en|Mahinda933@boarderzone.com
```

**Purpose:** Represents the social network state at time T=0.

### Substitution Parameters

Input parameters for executing read queries during the benchmark.

**Location:** `test-data/runtime/social-network/sf0.1/substitution_parameters/`

**Contents:**
- One file per query type (e.g., `interactive_1_param.txt`, `interactive_short_1_param.txt`)
- Each line contains input parameters for one query execution

Example (`interactive_1_param.txt`):
```
personId|firstName
30786325579101|Ian
24189255811707|Jun
```

**Purpose:** The driver reads these files to generate query operations with realistic input data.

### Update Streams

Dynamic modifications applied during benchmark execution.

**Location:** `test-data/runtime/social-network/sf0.1/update_streams/` and `updateStream_*.csv` files

**Contents:**
- Insert operations for new persons, posts, comments, likes, friendships
- Time-ordered sequence of graph mutations
- Multiple partitions for concurrent execution

Example (`updateStream_0_0_person.csv`):
```csv
1347615339076|0|1|35184372090047|Witold|Ciesla|female|435456000000|1347615339076|31.183.168.247|Firefox|1282|pl;en|...
```

**Purpose:** Simulates real-time social network activity. Updates modify the graph during benchmark runs.

**Important:** The initial snapshot must be reloaded after each benchmark run because updates mutate the dataset.

### Validation Parameters

Pre-computed query results for correctness verification.

**Location (Embedded):** `test-data/fixtures/social-network/sf0.1/validation_parameters/`

**Contents:**
- `validation_params-sf0.1-100.csv` (100 queries)
- `validation_params-sf0.1-200.csv` (200 queries)
- `validation_params-sf0.1-500.csv` (500 queries)

**Format:** JSON lines with query input and expected output

Example:
```json
{"personIdSQ1":26388279068220}|{"firstName":"Jun","lastName":"Wang","birthday":575424000000,...}
```

**Purpose:** Driver executes queries and compares actual results against expected results to verify correctness.

## Why Validation Fixtures Are Embedded

Small validation parameter subsets (100/200/500 queries) for SF0.1 are committed to the repository.

**Rationale:**
- Fast onboarding without large downloads
- Deterministic, reproducible validation
- Suitable for development and debugging
- Works offline

**Not for:**
- Official LDBC compliance runs
- Performance benchmarking

## Why Data Must Be Reloaded

Update operations in the benchmark modify the graph:
- Add new persons, friendships, posts, comments, likes
- Accumulate over multiple benchmark runs
- Change the expected results of read queries

Reloading ensures:
- Consistent starting state
- Reproducible results
- Valid comparisons across runs

## Where Production Data Comes From

All datasets originate from the official LDBC repositories:

**SURF Repository:** https://repository.surfsara.nl/datasets/cwi/snb/

Available data:
- Initial snapshots in various formats (CSV, TTL)
- Multiple scale factors (SF 0.1, 1, 3, 10, 30, 100, 300, 1000)
- Substitution parameters
- Update streams with different partition counts

**LDBC Datasets:** https://datasets.ldbcouncil.org/

Official validation parameters and documentation.

**Data Generator:** https://github.com/ldbc/ldbc_snb_datagen_hadoop

Used to generate custom datasets with specific characteristics.

## Scale Factors

Scale Factor (SF) determines dataset size:

| SF | Persons | Posts+Comments | Edges | Disk Size (approx) |
|----|---------|----------------|-------|-------------------|
| 0.1 | ~3K | ~60K | ~200K | ~30 MB |
| 1 | ~11K | ~600K | ~2M | ~300 MB |
| 10 | ~110K | ~6M | ~20M | ~3 GB |
| 100 | ~1.1M | ~60M | ~200M | ~30 GB |

**Default:** SF 0.1 (fast validation and development)

## Data Format

This implementation uses `csv_composite-longdateformatter`:

**csv_composite:** Foreign keys merged into parent entities (fewer files)
**longdateformatter:** Dates as Unix timestamps in milliseconds

Example:
```csv
id|firstName|birthday|locationIP|...
933|Mahinda|628646400000|119.235.7.103|...
```

## Directory Structure

```
test-data/
    fixtures/                          # Committed to git
        social-network/
            sf0.1/
                validation_parameters/ # Small validation subsets
    runtime/                           # Gitignored, downloaded
        social-network/
            sf0.1/
                dynamic/               # Initial snapshot (dynamic data)
                static/                # Initial snapshot (static reference data)
                substitution_parameters/
                update_streams/
                updateStream_*.csv
```

## Getting Started

1. **Download data:**
   ```bash
   ./scripts/fetch-test-data.sh
   ```

2. **Load initial snapshot** into your database (vendor-specific)

3. **Run validation:**
   ```bash
   ./scripts/ldbc-driver.sh
   ```

The driver reads substitution parameters, executes queries, and validates results against expected output.

