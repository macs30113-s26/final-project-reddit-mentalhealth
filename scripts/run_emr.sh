#!/bin/bash

# One EMR/Spark runner for the Reddit collection project.
#
# Usage:
#   ./scripts/run_emr.sh test4
#   ./scripts/run_emr.sh test8
#   ./scripts/run_emr.sh full
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
elif [ "${MODE}" = "full" ]; then
    RUN_GROUP="full"
    RUN_TYPE="full"
    CORES=8
    PARTITIONS=8
else
    echo "Usage: ./scripts/run_emr.sh test4|test8|full"
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
