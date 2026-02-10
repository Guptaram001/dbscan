#!/usr/bin/env bash
# Shared Spark cluster configuration - edit this file only.
# Used by: start_master.sh, start_worker1/2/3.sh, submit_job.sh

# Master
MASTER_HOST=localhost
MASTER_PORT=7077

# Each worker's resources (must match how you start workers)
WORKER_MEMORY=2g
WORKER_CORES=2
NUM_WORKERS=3
SPARK_DEFAULT_PARALLELISM_FACTOR=2


# Driver (only used when submitting Spark jobs).
# UF/GraphX merge step collects merge edges to the driver; use 4g+ for large datasets (e.g. 10M points).
DRIVER_MEMORY=4g
DRIVER_CORES=2

# Serial run (single-machine, no Spark)
SERIAL_CORES=2
SERIAL_MEMORY=2g

# Total executor cores = NUM_WORKERS * WORKER_CORES (use all workers by default)
export TOTAL_EXECUTOR_CORES=$((NUM_WORKERS * WORKER_CORES))
