#!/bin/bash

# One EMR/Spark runner for the Reddit collection project.
#
# Usage:
#   ./scripts/run_emr.sh test4
#   ./scripts/run_emr.sh test8
#   ./scripts/run_emr.sh test12
#   ./scripts/run_emr.sh part1
#   ./scripts/run_emr.sh part2
#
# If test8 is unstable, change the full CORES/PARTITIONS values below to 4.

MODE="$1"
BUCKET="luchen-lab"
CODE_PATH="s3://${BUCKET}/code/spark_fetch_arctic.py"
MANIFEST_PATH="s3://${BUCKET}/manifests/manifest_all_2019_2024.csv"

if [ "${MODE}" = "test4" ]; then
    RUN_GROUP="test_4core"
    RUN_TYPE="test_4core"
    CORES=4
    PARTITIONS=4
elif [ "${MODE}" = "test8" ]; then
    RUN_GROUP="test_8core"
    RUN_TYPE="test_8core"
    CORES=8
    PARTITIONS=8
elif [ "${MODE}" = "test12" ]; then
    RUN_GROUP="test_12core"
    RUN_TYPE="test_12core"
    CORES=12
    PARTITIONS=12
elif [ "${MODE}" = "part1" ]; then
    RUN_GROUP="full_part1"
    RUN_TYPE="full_part1"
    CORES=8
    PARTITIONS=8
elif [ "${MODE}" = "part2" ]; then
    RUN_GROUP="full_part2"
    RUN_TYPE="full_part2"
    CORES=8
    PARTITIONS=8
else
    echo "Usage: ./scripts/run_emr.sh test4|test8|test12|part1|part2"
    exit 1
fi

spark-submit \
    --total-executor-cores "${CORES}" \
    --executor-memory 4G \
    --driver-memory 4G \
    "${CODE_PATH}" \
    --manifest "${MANIFEST_PATH}" \
    --bucket "${BUCKET}" \
    --source archive \
    --run-type "${RUN_TYPE}" \
    --run-group "${RUN_GROUP}" \
    --num-partitions "${PARTITIONS}" \
    --sleep-seconds 2 \
    --max-retries 3
