# Configuration and Running

Configuration uses a two-layer property system:
1. Default properties in `runner/ldbc-driver.properties`
2. Vendor overrides in `{vendor}/ldbc-driver.properties`

The runner merges these, with vendor properties taking precedence.

## Key Parameters

### Vendor Selection

```properties
tinkerpop.vendor=ytdb
```

### Execution Mode

```properties
mode=validate_database
```

- **validate_database** - Correctness verification against expected output (default)
- **execute_benchmark** - Performance measurement with detailed metrics
- **create_validation** - Generate validation parameters (rarely needed)

### Validation File

```properties
validate_database=../test-data/fixtures/social-network/sf0.1/validation_parameters/validation_params-sf0.1-200.csv
```

Available files: `validation_params-sf0.1-100.csv`, `validation_params-sf0.1-200.csv` (default), `validation_params-sf0.1-500.csv`

### Scale Factor

```properties
ldbc.snb.interactive.scale_factor=0.1
```

Must match downloaded dataset in `test-data/runtime/`.

### Thread Count

```properties
thread_count=1
```

### Data Paths

```properties
ldbc.snb.interactive.parameters_dir=../test-data/runtime/social-network/sf0.1/substitution_parameters
ldbc.snb.interactive.updates_dir=../test-data/runtime/social-network/sf0.1/update_streams
```

Adjust when changing scale factors:

```properties
# For SF 1.0
ldbc.snb.interactive.scale_factor=1
ldbc.snb.interactive.parameters_dir=../test-data/runtime/social-network/sf1/substitution_parameters
ldbc.snb.interactive.updates_dir=../test-data/runtime/social-network/sf1/update_streams
validate_database=../test-data/fixtures/social-network/sf1/validation_parameters/validation_params-sf1-200.csv
```

## Property Override System

Load order:
1. Read `tinkerpop.vendor` from `runner/ldbc-driver.properties`
2. Check for `{vendor}/ldbc-driver.properties`
3. Merge: vendor properties override defaults

Reference: `scripts/ldbc-driver.sh`

### Vendor Properties Example

`ytdb/ldbc-driver.properties`:
```properties
ytdb.host=localhost
ytdb.port=8182
ytdb.database.name=ldbc_snb
ytdb.username=admin
ytdb.password=admin

tinkerpop.ic13.maxHops=4
```

Vendor properties are injected via Guice (see `YtdbModule.java`).

## Running

### Standard Validation

```bash
./scripts/ldbc-driver.sh
```

Uses defaults: validation mode, SF 0.1, 200 parameters, single thread.

### Benchmark Mode

```properties
mode=execute_benchmark
operation_count=10000
thread_count=1
```

Requires fully loaded dataset with substitution parameters and update streams.

### Multi-threaded Execution

1. Download update streams with matching partition count:
   ```bash
   ./scripts/fetch-test-data.sh --update-partitions 4
   ```

2. Configure:
   ```properties
   thread_count=4
   ```
