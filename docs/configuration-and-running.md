# Configuration and Running

## Overview

Configuration uses a two-layer property system:
1. **Default properties** in `runner/ldbc-driver.properties`
2. **Vendor overrides** in `{vendor}/ldbc-driver.properties`

The runner script merges these files, with vendor properties taking precedence.

## Key Parameters

### Vendor Selection

```properties
tinkerpop.vendor=ytdb
```

Controls which database implementation to use. The runner:
1. Reads this value from default properties
2. Loads the vendor module (e.g., `ytdb/`)
3. Applies vendor-specific overrides if present

Reference: `scripts/ldbc-driver.sh`

### Execution Mode

```properties
mode=validate_database
```

Three modes available:

**validate_database** - Correctness verification
- Executes queries and compares results against expected output
- Uses validation parameter files
- Reports pass/fail for each query
- Default mode for development

**execute_benchmark** - Performance measurement
- Measures throughput and latency
- Generates detailed metrics
- Requires fully loaded dataset
- Used for official LDBC runs

**create_validation** - Generate validation parameters
- Executes queries and captures results
- Creates validation files for future use
- Rarely needed (pre-generated files exist)

### Validation File

```properties
validate_database=../test-data/fixtures/social-network/sf0.1/validation_parameters/validation_params-sf0.1-200.csv
```

Path to validation parameters when `mode=validate_database`.

Available embedded files:
- `validation_params-sf0.1-100.csv` (100 queries)
- `validation_params-sf0.1-200.csv` (200 queries, default)
- `validation_params-sf0.1-500.csv` (500 queries)

### Scale Factor

```properties
ldbc.snb.interactive.scale_factor=0.1
```
Dataset size indicator. Must match downloaded data.

**Important:** Scale factor in properties must match the dataset in `test-data/runtime/`.

### Thread Count

```properties
thread_count=1
```

Number of concurrent driver threads executing operations (default 1)

### Data Paths

```properties
ldbc.snb.interactive.parameters_dir=../test-data/runtime/social-network/sf0.1/substitution_parameters
ldbc.snb.interactive.updates_dir=../test-data/runtime/social-network/sf0.1/update_streams
```

Paths to runtime data. Must be adjusted when changing scale factors:

```properties
# For SF 1.0
ldbc.snb.interactive.scale_factor=1
ldbc.snb.interactive.parameters_dir=../test-data/runtime/social-network/sf1/substitution_parameters
ldbc.snb.interactive.updates_dir=../test-data/runtime/social-network/sf1/update_streams
validate_database=../test-data/fixtures/social-network/sf1/validation_parameters/validation_params-sf1-200.csv
```

## Property Override System

### How Overrides Work

The runner loads properties in this order:

1. Reads `tinkerpop.vendor` from `runner/ldbc-driver.properties`
2. Checks for `{vendor}/ldbc-driver.properties`
3. Merges: vendor properties override defaults

Reference: `scripts/ldbc-driver.sh`

### Default Properties

Location: `runner/ldbc-driver.properties`

Contains:
- Vendor selection
- Execution mode
- Scale factor
- Data paths
- Thread configuration
- Query enable flags

These apply to all vendors unless overridden.

### Vendor Properties

Location: `{vendor}/ldbc-driver.properties` (optional)

Contains vendor-specific settings:
- Database connection parameters
- Query tuning parameters
- Vendor-specific optimizations

Example (`ytdb/ldbc-driver.properties`):
```properties
# Connection
ytdb.host=localhost
ytdb.port=8182
ytdb.database.name=ldbc_snb
ytdb.username=admin
ytdb.password=admin

# Query tuning
tinkerpop.ic13.maxHops=4
```

Vendor properties are injected into the vendor module via Guice (see `YtdbModule.java`).

### When to Use Each

**Modify default properties when:**
- Switching vendors
- Changing scale factors
- Switching between validation/benchmark modes
- Adjusting thread counts

**Modify vendor properties when:**
- Changing database connection details
- Tuning vendor-specific query optimizations
- Adjusting database-specific timeouts or limits

## Running the Benchmark

### Standard Validation Run

```bash
./scripts/ldbc-driver.sh
```

Uses defaults from `runner/ldbc-driver.properties`:
- Validation mode
- SF 0.1
- 200 validation parameters
- Single thread

### Custom Validation File

Edit `runner/ldbc-driver.properties`:
```properties
validate_database=../test-data/fixtures/social-network/sf0.1/validation_parameters/validation_params-sf0.1-500.csv
```

### Benchmark Mode

Edit `runner/ldbc-driver.properties`:
```properties
mode=execute_benchmark
operation_count=10000
thread_count=1
```

Requires:
- Fully loaded dataset at the configured scale factor
- Substitution parameters and update streams downloaded

### Multi-threaded Execution

1. Download update streams with matching partition count:
   ```bash
   ./scripts/fetch-test-data.sh --update-partitions 4
   ```

2. Configure thread count:
   ```properties
   thread_count=4
   ```
