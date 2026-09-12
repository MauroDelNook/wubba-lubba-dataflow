# Changelog

## [Unreleased]

### Fixed

- Pipeline timestamp parsing: replace `+00:00` with `Z` for Beam's `from_rfc3339`
- DoFn serialization: move imports inside `process()` so they survive pickling to workers
- Beam `Duration` vs `timedelta`: convert window bounds to `datetime` before subtraction
- `setup.sh`: correct Cloud Function source path from `../publisher` to `./publisher`
- `setup.sh`: pass `GCP_PROJECT_ID` and `PUBSUB_TOPIC` env vars to the Cloud Function

### Added

- `run.googleapis.com` API to `setup.sh` (required for gen2 Cloud Functions)
- `publisher/requirements.txt` for Cloud Function dependencies
- Pause/resume instructions in README
- `--requirements_file` flag to README deploy command
- `docs/how-dataflow-works.md` learning guide covering Beam, windows, triggers, and gotchas
- GCS staging bucket (`gs://{PROJECT_ID}-dataflow-temp`) to `infra/setup.sh`
- Bucket cleanup to `infra/teardown.sh`
