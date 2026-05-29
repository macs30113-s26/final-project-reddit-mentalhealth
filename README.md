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

# Reddit Mental Health Data Collection

## Table of Contents
1. Overview
2. Individual Contributions
- Data Collection and Raw S3 Ingestion (Lu)
- Data Cleaning, Preprocessing(Nana)
- (Anyi)


## 1. Overview 
Social media platforms such as Reddit contain large-scale textual discussions related to mental health, including topics such as depression, anxiety, and emotional support. However, analyzing Reddit data at scale is computationally challenging because the datasets are extremely large and consist primarily of unstructured text.

To address this issue, our group project developed a scalable distributed NLP processing pipeline using PySpark and AWS EMR. The project aimed to preprocess, organize, and analyze Reddit posts and comments efficiently in a cloud-based distributed computing environment. Instead of relying on local pandas workflows, we used Spark-based distributed processing to improve scalability, runtime performance, and storage efficiency.

The project analyzed Reddit discussions from the subreddit:

- mentalhealth

## 2. Individual Contributions

### Individual Contribution - Data Collection and Raw S3 Ingestion (Lu)

My contribution is the first stage of the group pipeline: collecting historical Reddit data and storing the raw data in Amazon S3 so that the rest of the group can process it with Spark. I focused on building a scalable ingestion workflow.

The main challenge is that Reddit data is too large to collect comfortably with a single local Python script. To make the collection process more suitable for a large-scale computing course, I used the Arctic Shift historical Reddit API together with Spark on AWS EMR. Spark divides the collection work into many monthly tasks, runs several API workers in parallel, and writes the output directly to S3.

## Data Scope

- Subreddit: `r/mentalhealth`
- Time period: January 2019 through December 2024
- Data types: Reddit posts and comments
- Source: Arctic Shift historical Reddit API
- Output format: line-delimited JSON written by Spark
- Final raw dataset size: 603,891 posts and 1,818,218 comments, for 2,422,109 total records

The S3 path uses `source=archive` because the earlier sample data shared with teammates already used that convention. In this project, `archive` means historical Reddit data collected from Arctic Shift rather than recent Reddit API/PRAW data.

## Code Files

- [`src/make_manifest.py`](src/make_manifest.py): creates a manifest CSV. A manifest is a task list where each row is one collection job, such as posts from one subreddit in one month.
- [`manifests/manifest_all_2019_2024.csv`](manifests/manifest_all_2019_2024.csv): the final task list for 2019-2024. It contains 144 tasks: 72 months times 2 data types, posts and comments.
- [`src/spark_fetch_arctic.py`](src/spark_fetch_arctic.py): the main Spark/EMR collection script. It reads the manifest, calls the Arctic Shift API, retries failed requests, and writes raw JSON data to S3.
- [`scripts/upload_project_files_to_s3.sh`](scripts/upload_project_files_to_s3.sh): uploads the Spark script and manifest to S3 before running EMR.
- [`scripts/run_emr.sh`](scripts/run_emr.sh): optional helper script containing the `spark-submit` commands used for tests and full runs.

## S3 Data Lake Layout

The raw data is stored in partitioned S3 folders so teammates can read it directly with Spark:

```text
s3://luchen-lab/raw/reddit/posts/source=archive/subreddit=mentalhealth/year=<year>/month=<month>/
s3://luchen-lab/raw/reddit/comments/source=archive/subreddit=mentalhealth/year=<year>/month=<month>/
```

Other supporting files are stored here:

```text
s3://luchen-lab/manifests/manifest_all_2019_2024.csv
s3://luchen-lab/code/spark_fetch_arctic.py
s3://luchen-lab/logs/collection_logs/
```

The output files have Spark-style names such as `part-00000-....json`. Even though the file extension is `.json`, Spark writes one JSON object per line, so the files can be read as JSONL-style data.

## Parallel Collection Strategy

My collection strategy was developed iteratively. Because this pipeline depends on an external API, using more Spark cores does not always mean the job will be faster. If too many workers call the API at the same time, the API may slow down, fail, or hit rate limits. Therefore, I tested several levels of parallelism before running the full dataset.

I first used a conservative 4-core run on three months of data, from 2019-01 to 2019-03. This confirmed that the code, S3 output paths, logs, and retry logic worked correctly. Next, I tested 8 cores on another three-month window, from 2019-04 to 2019-06. The data volume was similar to the 4-core test, but the runtime was much faster, so 8 cores looked like a better setting. I then tested 12 cores on 2019-07 to 2019-09. This run was successful, but it was not faster than 8 cores and was close to the 4-core runtime. This suggested that the bottleneck was probably the Arctic Shift API or network requests, not Spark computation. Based on this result, I chose 8 cores for the full collection.

I also refered to the test runs to decide how to split the full job. AWS Academy sessions can expire after about four hours, so I did not want to run all remaining months in one long job. The 8-core test took about 14.2 minutes for three months of posts and comments. Using that runtime as a rough estimate, I split the remaining data into two larger but still safe run groups. Each full run was designed to finish within the four-hour AWS session limit.

The final run groups were non-overlapping:

```text
test_4core:  2019-01 to 2019-03
test_8core:  2019-04 to 2019-06
test_12core: 2019-07 to 2019-09
full_part1:  2019-10 to 2022-04
full_part2:  2022-05 to 2024-12
```

The test runs are included in the final dataset because their months do not overlap with the full runs. This means the final dataset is the combination of all five successful run groups, without duplicate months.

## EMR Workflow

First, I uploaded the code and manifest to S3:

```bash
./scripts/upload_project_files_to_s3.sh
```

Then I launched an EMR cluster using the course helper script:

```bash
python3 /Users/chenlu/Downloads/launch_spark_cluster.py \
    --s3_bucket luchen-lab \
    --primary_count 1 \
    --core_count 2 \
    --instance_type "m5.xlarge"
```

On the EMR primary node, I used `spark-submit` to run the collection. For example, this command ran the 8-core test:

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

For the longer full runs, I used `nohup` so that the Spark job could continue even if my SSH connection disconnected:

```bash
nohup spark-submit \
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
    --max-retries 3 \
    > full_part1.out 2>&1 &
```

After each run, the script wrote a JSON run log to S3. These logs record the number of tasks, records collected, failed tasks, runtime, and per-task details.

## Run Results

| Run group | Time window | Executor cores | Tasks | Posts | Comments | Failed or partial tasks | Runtime |
|---|---:|---:|---:|---:|---:|---:|---:|
| `test_4core` | 2019-01 to 2019-03 | 4 | 6 | 13,322 | 41,181 | 0 | 24.8 min |
| `test_8core` | 2019-04 to 2019-06 | 8 | 6 | 14,652 | 42,027 | 0 | 14.2 min |
| `test_12core` | 2019-07 to 2019-09 | 12 | 6 | 14,460 | 41,339 | 0 | 24.6 min |
| `full_part1` | 2019-10 to 2022-04 | 8 | 62 | 261,824 | 855,878 | 0 | 3.11 hr |
| `full_part2` | 2022-05 to 2024-12 | 8 | 64 | 299,633 | 837,793 | 0 | 2.72 hr |
| **Total** | **2019-01 to 2024-12** |  | **144** | **603,891** | **1,818,218** | **0** |  |

Based on the test runs, 8 executor cores were a good choice for this API-based workload. The 12-core test did not improve runtime, most likely because the bottleneck was the external API rather than Spark computation.

## Notes on Reliability and Rate Limits

Because this pipeline calls an external API, the goal was not to use as many cores as possible. Too much parallelism could trigger rate limits or unstable requests. I used a conservative setup with limited cores, `sleep_seconds=2`, and `max_retries=3`.

The manifest design also helped with reliability. If a run failed, I could rerun only the affected `run_group` instead of restarting the entire 2019-2024 collection. Since each run group covers a different set of months, the final dataset can combine all successful runs without duplicating months.

For downstream analysis, teammates can read the full raw dataset from these prefixes:

```text
s3://luchen-lab/raw/reddit/posts/source=archive/subreddit=mentalhealth/
s3://luchen-lab/raw/reddit/comments/source=archive/subreddit=mentalhealth/
```



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
