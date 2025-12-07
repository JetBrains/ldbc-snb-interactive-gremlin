## Test data & validation parameters

This repository intentionally distinguishes between runtime benchmark data and small, versioned test fixtures.

### Validation parameters (included)

For quick setup, debugging, and correctness checks, we deliberately include a small subset of pre-generated validation parameters for SF0.1 directly in the repository.

These files live under:
```
test-data/fixtures/social-network/sf0.1/validation_parameters/
```
They contain trimmed validation parameter sets (e.g. 100 / 200 / 500 entries), extracted from the full SF0.1 dataset.

### Full validation parameters (optional)

If you need larger-scale or official validation, you have two options:

1) Download the pre-generated validation parameters from the LDBC repository:
```
https://datasets.ldbcouncil.org/interactive-v1/validation_params-interactive-v1.0.0-sf0.1-to-sf10.tar.zst
```

2) Generate validation parameters yourself using the LDBC SNB Interactive driver tooling, following the standard LDBC workflow.