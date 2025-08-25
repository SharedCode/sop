# internal/

This directory contains baseline and experimental components used for comparison and integration testing.

- Not part of the public API surface; subject to change without notice.
- Packages here may depend on external services (e.g., Redis, Cassandra).
- Integration tests under `internal/inredcfs/integrationtests` are gated and skipped by default.
  - To run them locally: export SOP_RUN_INREDCFS_IT=1 and ensure dependencies are available.

Use at your own risk; do not vendor or depend on these packages in external projects.
