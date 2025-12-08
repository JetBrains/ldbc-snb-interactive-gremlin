# Dataset Overview

The LDBC SNB Interactive benchmark uses four types of data:

1. **Initial Snapshot** - Static graph representing the starting state
2. **Substitution Parameters** - Input parameters for read queries
3. **Update Streams** - Dynamic data modifications during benchmark runs
4. **Validation Parameters** - Expected query results for correctness verification

## Initial Snapshot

Location: `test-data/runtime/social-network/sf0.1/dynamic/` and `static/`

Contents:
- Person nodes and relationships (knows, likes, hasInterest)
- Forum nodes (hasMember, hasModerator, containerOf)
- Post and Comment nodes (hasCreator, replyOf, hasTag)
- Static reference data (Place, Organization, Tag, TagClass)

Format: CSV with pipe delimiters

## Substitution Parameters

Location: `test-data/runtime/social-network/sf0.1/substitution_parameters/`

One file per query type (e.g., `interactive_1_param.txt`). Each line contains input parameters for one query execution.

## Update Streams

Location: `test-data/runtime/social-network/sf0.1/update_streams/`

Time-ordered insert operations for persons, posts, comments, likes, friendships. Multiple partitions for concurrent execution.

**Important:** Reload initial snapshot after each run (updates modify the dataset).

## Validation Parameters

Location: `test-data/fixtures/social-network/sf0.1/validation_parameters/`

Files: `validation_params-sf0.1-100.csv`, `validation_params-sf0.1-200.csv`, `validation_params-sf0.1-500.csv`

Small subsets committed to repository for fast onboarding and offline development. Not for official LDBC compliance runs.

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
