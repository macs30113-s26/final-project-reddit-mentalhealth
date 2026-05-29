# Reddit Mental Health Data Collection

## Table of Contents
1. Overview
2. Individual Contributions
- Data Collection (Lu)
- Data Cleaning, Preprocessing (Nana)
- (Anyi)


## 1. Overview 
Social media platforms such as Reddit contain large-scale textual discussions related to mental health, including topics such as depression, anxiety, and emotional support. However, analyzing Reddit data at scale is computationally challenging because the datasets are extremely large and consist primarily of unstructured text.

To address this issue, our group project developed a scalable distributed NLP processing pipeline using PySpark and AWS EMR. The project aimed to preprocess, organize, and analyze Reddit posts and comments efficiently in a cloud-based distributed computing environment. Instead of relying on local pandas workflows, we used Spark-based distributed processing to improve scalability, runtime performance, and storage efficiency.

## 2. Individual Contributions

## Individual Contribution - Data Collection (Lu)

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

## Individual Contribution – Data Cleaning, Preprocessing, and Scalable Text Processing　(Nana)

* Code and Results of This Part: [data-processing.ipynb](data-processing.ipynb)

My contribution focused on building a scalable preprocessing and exploratory analysis pipeline for large-scale Reddit mental health discussions using Apache Spark on AWS EMR.

## Architecture and Workflow

The analysis was conducted on a Spark-enabled AWS EMR cluster accessed through JupyterHub. Reddit posts and comments from the r/mentalhealth community were stored on Amazon S3 and loaded directly into Spark DataFrames using distributed JSON readers. This architecture was selected because the dataset is too large for efficient local processing and can be analyzed more effectively through distributed computing.

### Data Cleaning

The first stage of the pipeline focused on data quality control. For both posts and comments, I removed:

* null observations,
* deleted Reddit entries (`[deleted]`),
* removed Reddit entries (`[removed]`),
* and empty text records.

These observations do not contain meaningful textual information and would introduce noise into subsequent analyses. After cleaning, the resulting Spark DataFrames were cached in memory. Because multiple downstream analyses reuse the same cleaned datasets, caching reduces repeated computation and improves overall runtime performance.

### Text Preprocessing

After cleaning, I implemented a scalable text preprocessing workflow designed to convert raw Reddit text into structured representations suitable for large-scale analysis.

The preprocessing pipeline consisted of:

1. Converting all text to lowercase to ensure consistent word matching.
2. Removing punctuation and non-alphabetic characters using regular expressions.
3. Tokenizing text into individual words.
4. Removing common stopwords using Spark MLlib's `StopWordsRemover`.
5. Creating a token-count feature (`num_tokens`) to measure text length.

This workflow was intentionally designed to balance computational efficiency and analytical usefulness. Lowercasing and punctuation removal reduce redundant vocabulary, while tokenization and stopword removal produce cleaner word-level representations for exploratory text analysis. The token count variable additionally provides a simple measure of posting behavior and discussion complexity.

### Exploratory Analysis and Findings

Several scalable exploratory analyses were conducted on the processed datasets.

#### Word Frequency Analysis

First, I performed word frequency analysis using Spark's distributed aggregation operations. By exploding token arrays and aggregating counts across the cluster, I identified the most frequently used terms in mental health discussions.

For posts, the most common terms included *"im"* (857,540 occurrences), *"like"* (622,341), *"feel"* (489,092), *"know"* (372,302), and *"help"* (181,246). These terms suggest that posts primarily consist of personal experiences, emotional reflections, and requests for advice or support.

For comments, the most frequent terms included *"please"* (2,004,055 occurrences), *"thank"* (658,890), *"feel"* (658,400), *"help"* (633,198), and *"therapy"* (390,660). Compared with posts, comments contained more supportive and advice-oriented language, indicating that community members actively respond to and assist one another.

#### Temporal Activity Trends

Second, I conducted yearly aggregation analyses of posts and comments to examine how community activity evolved over time.

The number of posts increased substantially from **37,868 posts in 2019** to **106,339 posts in 2024**, indicating sustained growth in participation within the subreddit.

Similarly, comment activity expanded considerably during the study period. The number of comments increased from **172,849 comments in 2019** to a peak of **387,720 comments in 2021**. Although comment volume declined somewhat afterward, activity remained substantially higher than pre-2020 levels, suggesting continued community engagement.

#### Discussion Length

Third, I calculated average token counts by year to investigate temporal changes in discussion length and user engagement.

Posts remained consistently detailed throughout the observation period, averaging approximately **98–107 tokens per post**. The average post length was relatively stable across years, suggesting that users consistently provided substantial descriptions of their experiences and concerns.

Comments were shorter but exhibited greater variation. Average comment length increased from **28.7 tokens in 2019** to **43.5 tokens in 2021**, before declining to approximately **33.6 tokens in 2024**. This pattern may indicate periods of deeper interaction and more extensive discussion within the community.

All analyses were implemented using Spark transformations and aggregations, allowing them to scale efficiently to large datasets.

### Data Storage and Scalability

To support downstream group analyses, the processed datasets were saved to Amazon S3 as partitioned Parquet files.

I selected the Parquet format because it is substantially more efficient than CSV for Spark workloads, providing faster read performance, columnar storage, and reduced storage requirements. The datasets were additionally partitioned by year and month to improve query efficiency and reduce unnecessary data scanning in later stages of the project.

### Scalability Benchmarking

Finally, I evaluated the scalability of the workflow by running the preprocessing pipeline on multiple data fractions (10%, 50%, and 100% samples).

The workflow completed in **3.60 seconds** for the 10% sample, **2.48 seconds** for the 50% sample, and **2.35 seconds** for the full dataset. The relatively stable runtime suggests that the distributed Spark workflow can process larger datasets without substantial increases in execution time.

## Contribution Summary

Overall, my contribution centered on designing a scalable text-processing infrastructure for Reddit mental health data. This pipeline transformed raw social media text into structured analytical datasets, generated descriptive insights regarding language use and community engagement, and produced reusable Parquet datasets that enabled subsequent group-level analyses. The results suggest that the r/mentalhealth community is characterized by highly personal narratives, strong peer support, and sustained growth in participation over time.

----------------------------------------------------------

(Anyi's part)





----------------------------------------------------------

Author:

Overview: Nana Takeshiba

Individual contribution sections: each project member responsible for the section
