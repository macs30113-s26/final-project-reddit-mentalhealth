# Reddit Mental Health Data Collection

This project collects historical Reddit data for `r/mentalhealth` using the Arctic Shift API, Spark/EMR, and Amazon S3.

My part of the final project is data collection and raw cloud storage. The output is JSONL data in S3. Later teammates can read these files with Spark for preprocessing, NLP, and modeling.

## Data Scope

- Subreddit: `mentalhealth`
- Time period: January 2019 through December 2024
- Data types: posts and comments
- Source: Arctic Shift historical Reddit API
- Output format: JSONL

The project uses `source=archive` in the S3 path to stay compatible with the sample data path that was already shared with teammates.

## S3 Layout

```text
s3://luchen-lab/
    raw/reddit/posts/source=archive/subreddit=mentalhealth/year=<year>/month=<month>/
    raw/reddit/comments/source=archive/subreddit=mentalhealth/year=<year>/month=<month>/
    manifests/
    logs/collection_logs/
    code/
```

Each output file is named with a deterministic task id, for example:

```text
part-task-000000.jsonl
```

This means rerunning the same manifest overwrites the same S3 keys instead of creating duplicate files.

## Files

```text
src/make_manifest.py
```

Creates manifest CSV files. A manifest is a task list where each row is one monthly collection task. The current project uses one combined manifest:

```text
manifests/manifest_all_2019_2024.csv
```

```text
src/spark_fetch_arctic.py
```

Spark job that reads a manifest, distributes tasks across Spark workers, calls the Arctic Shift API, and writes JSONL files to S3.

```text
scripts/upload_project_files_to_s3.sh
```

Uploads the Spark script and combined manifest CSV file to S3.

```text
scripts/run_emr.sh
```

One Spark submit script for EMR. It supports three modes:

```bash
./scripts/run_emr.sh test4
./scripts/run_emr.sh test8
./scripts/run_emr.sh part1
./scripts/run_emr.sh part2
```

## Manifest Strategy

The full 2019-2024 collection is stored in one manifest. The `run_group` column separates non-overlapping time windows:

```text
test_4core: 2019-01 to 2019-03
test_8core: 2019-04 to 2019-06
test_12core: 2019-07 to 2019-09
full_part1:  2019-10 to 2022-04
full_part2:  2022-05 to 2024-12
```

The test outputs are part of the final dataset because the time windows do not overlap.

## Run Workflow

1. Generate or check the manifest CSV file.
2. Upload `src/spark_fetch_arctic.py` and the manifest to S3:

```bash
./scripts/upload_project_files_to_s3.sh
```

3. Start an EMR cluster with Spark using the course helper script:

```bash
python launch_spark_cluster.py \
    --s3_bucket luchen-lab \
    --primary_count 1 \
    --core_count 2 \
    --instance_type "m5.xlarge"
```

When the helper prints the SSH command, connect to the EMR primary node.

4. Run the 4-core test:

```bash
spark-submit \
    --total-executor-cores 4 \
    --executor-memory 4G \
    --driver-memory 4G \
    s3://luchen-lab/code/spark_fetch_arctic.py \
    --manifest s3://luchen-lab/manifests/manifest_all_2019_2024.csv \
    --bucket luchen-lab \
    --source archive \
    --run-type test_4core \
    --run-group test_4core \
    --num-partitions 4 \
    --sleep-seconds 2 \
    --max-retries 3
```

5. Check S3 outputs and the collection log.
6. Run the 8-core test:

```bash
spark-submit \
    --total-executor-cores 8 \
    --executor-memory 4G \
    --driver-memory 4G \
    s3://luchen-lab/code/spark_fetch_arctic.py \
    --manifest s3://luchen-lab/manifests/manifest_all_2019_2024.csv \
    --bucket luchen-lab \
    --source archive \
    --run-type test_8core \
    --run-group test_8core \
    --num-partitions 8 \
    --sleep-seconds 2 \
    --max-retries 3
```

7. If 8 cores is stable, use 8 cores for the remaining collection. The 12-core test is optional and was used to check whether higher parallelism helped.
8. Run the remaining collection in two parts:

```bash
spark-submit \
    --total-executor-cores 8 \
    --executor-memory 4G \
    --driver-memory 4G \
    s3://luchen-lab/code/spark_fetch_arctic.py \
    --manifest s3://luchen-lab/manifests/manifest_all_2019_2024.csv \
    --bucket luchen-lab \
    --source archive \
    --run-type full_part1 \
    --run-group full_part1 \
    --num-partitions 8 \
    --sleep-seconds 2 \
    --max-retries 3
```

Then run the second part:

```bash
spark-submit \
    --total-executor-cores 8 \
    --executor-memory 4G \
    --driver-memory 4G \
    s3://luchen-lab/code/spark_fetch_arctic.py \
    --manifest s3://luchen-lab/manifests/manifest_all_2019_2024.csv \
    --bucket luchen-lab \
    --source archive \
    --run-type full_part2 \
    --run-group full_part2 \
    --num-partitions 8 \
    --sleep-seconds 2 \
    --max-retries 3
```

9. Terminate the EMR cluster when finished. You can do this in the AWS console, or use the course helper script:

```bash
python terminate_spark_cluster.py --cluster_id j-XXXXXXXXXXXXX
```

Replace `j-XXXXXXXXXXXXX` with the cluster id printed by `launch_spark_cluster.py`.

The `scripts/run_emr.sh` file contains the same commands in a shorter reusable form. It is optional; the explicit `spark-submit` commands above match the style shown in the course EMR cheatsheet.

## Notes On API Safety

External APIs can be rate-limited. This pipeline uses controlled parallelism instead of maximum parallelism:

- Spark cores and partitions are limited.
- Each worker sleeps between API requests.
- Failed requests are retried.
- A run-level JSON log is written to S3.

This keeps the collection process simple enough for a course project while still showing Spark/EMR parallelism.


----------------------------------------------------------
**Individual Contribution to Group Project: Nana Takeshiba**

- Code and Results of My Part: [data-processing.ipynb](data-processing.ipynb)

## Background

Large-scale social media platforms such as Reddit contain valuable information about public discussions surrounding mental health. However, traditional local analysis workflows using pandas or in-memory processing become inefficient when scaling to large datasets.

To address this issue, our group project developed a distributed NLP processing pipeline using PySpark and AWS EMR. The broader goal of the project was to preprocess and organize Reddit discussions from mental health related subreddits so that downstream analyses could be conducted efficiently at scale.

My contribution focused on designing and implementing the scalable preprocessing and exploratory analysis pipeline for Reddit posts and comments data.

## Architecture and Workflow

The analysis was conducted using a Spark-enabled AWS EMR cluster connected through JupyterHub. Reddit posts and comments data stored on Amazon S3 were loaded directly into Spark DataFrames using distributed JSON readers.

After loading the data, I implemented a scalable cleaning pipeline that removed null values, deleted Reddit entries ([deleted], [removed]), and empty text observations. Because repeated transformations on large datasets are computationally expensive, I additionally cached the cleaned Spark DataFrames to improve runtime efficiency.

Next, I constructed a scalable NLP preprocessing workflow using Spark MLlib. This pipeline included:

- lowercasing text,
- punctuation removal,
- tokenization,
- stopword removal,
- and token count generation.

These preprocessing steps transformed raw Reddit text into structured token-based representations suitable for downstream analysis.

I also implemented exploratory analyses on the processed datasets. First, I conducted word frequency analysis using distributed aggregation operations in Spark. Frequently appearing terms such as “feel,” “help,” and “anxiety” provided a descriptive overview of common linguistic patterns in mental health discussions.

Second, I implemented monthly aggregation analyses grouped by subreddit, year, and month to support scalable temporal analysis of Reddit activity.

**Data Storage and Scalability**

To support downstream group analysis, I saved the processed datasets as partitioned parquet files on Amazon S3. I used parquet rather than CSV because parquet files are significantly more efficient for distributed Spark workloads and allow faster querying and reduced storage overhead. Partitioning the datasets by year and month additionally improved scalability for later analyses.

Finally, I conducted scalability benchmarking using multiple sample sizes to evaluate how the Spark workflow scaled with increasing data volume. 

**Here, I will write up the result using full dataset**

This benchmark demonstrated the advantages of distributed processing for large-scale social media text analysis.

Overall, my contribution focused on building a scalable NLP preprocessing infrastructure that enabled the broader group project to conduct efficient large-scale analysis of Reddit mental health discussions.

----------------------------------------------------------
