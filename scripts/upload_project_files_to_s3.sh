#!/bin/bash

# Run this from the project root after configuring AWS credentials.
# It uploads the Spark code and manifests to S3 so EMR can read them.

BUCKET="luchen-lab"

aws s3 cp src/spark_fetch_arctic.py "s3://${BUCKET}/code/spark_fetch_arctic.py"
aws s3 cp manifests/manifest_all_2019_2024.csv "s3://${BUCKET}/manifests/manifest_all_2019_2024.csv"
