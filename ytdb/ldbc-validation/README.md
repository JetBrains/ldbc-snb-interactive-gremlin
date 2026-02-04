# LDBC validation (YouTrackDB)

This folder contains a YouTrackDB-specific Docker-based workflow for:
- generating data with LDBC Datagen
- loading/restoring a clean DB state via the YTDB loader (backup/restore)
- running driver modes: create validation params, validate, benchmark

## Quick start

1) Generate data (clears previous data output):
```bash
./datagen.sh
```

2) Create validation params (restores/loads clean DB first):
```bash
./create-params.sh
```

3) Validate (restores/loads clean DB first):
```bash
./validate.sh
```

4) Benchmark (restores/loads clean DB first):
```bash
./benchmark.sh
```

## Notes

- `load.sh` restores from backup if present, otherwise loads from CSV and creates a backup.
- `datagen.sh` removes `work/data/social_network` and `work/data/substitution_parameters` before running.
- Validation parameters are written to `work/results/validation_params.csv`.
- Driver configs are in `driver-*.properties`.
