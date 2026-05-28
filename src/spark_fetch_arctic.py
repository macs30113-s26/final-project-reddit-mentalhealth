"""
Collect Reddit data from the Arctic Shift API with Spark and write JSONL to S3.

This script is intentionally simple for a course project:
1. Spark reads a manifest CSV from local disk or S3.
2. Each manifest row is one collection task.
3. Spark distributes tasks across partitions.
4. Each task calls the Arctic Shift API and returns records to Spark.
5. Spark writes JSONL files to partitioned S3 folders.
6. One run-level log is written to S3 at the end.

Example:
    spark-submit \
        --total-executor-cores 4 \
        src/spark_fetch_arctic.py \
        --manifest s3://luchen-lab/manifests/manifest_test_4core_2019_01_03.csv \
        --bucket luchen-lab \
        --num-partitions 4
"""

import argparse
import json
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


POSTS_URL = "https://arctic-shift.photon-reddit.com/api/posts/search"
COMMENTS_URL = "https://arctic-shift.photon-reddit.com/api/comments/search"


def now_utc_string():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_response_data(response_json):
    """
    Arctic Shift responses may be a list, or may wrap records in a data/results key.
    This helper keeps the rest of the code simple.
    """
    if isinstance(response_json, list):
        return response_json
    if isinstance(response_json, dict):
        if "data" in response_json:
            return response_json["data"]
        if "results" in response_json:
            return response_json["results"]
    return []


def utc_to_date_parts(created_utc):
    dt = datetime.fromtimestamp(int(created_utc), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d"), dt.year, dt.month


def is_usable_text(text):
    if text is None:
        return False
    text = str(text).strip()
    return text != "" and text.lower() not in ["[deleted]", "[removed]"]


def normalize_post(record, source, task_id):
    created_date, year, month = utc_to_date_parts(record["created_utc"])
    return {
        "row_type": "record",
        "record_type": "posts",
        "post_id": record.get("id"),
        "subreddit": str(record.get("subreddit", "")).lower(),
        "created_utc": int(record.get("created_utc")),
        "created_date": created_date,
        "year": year,
        "month": f"{month:02d}",
        "title": record.get("title", ""),
        "selftext": record.get("selftext", ""),
        "score": record.get("score"),
        "num_comments": record.get("num_comments"),
        "source": source,
        "task_id": int(task_id),
        "ingested_at": now_utc_string(),
    }


def normalize_comment(record, source, task_id):
    created_date, year, month = utc_to_date_parts(record["created_utc"])
    post_id = str(record.get("link_id", "")).replace("t3_", "")
    return {
        "row_type": "record",
        "record_type": "comments",
        "comment_id": record.get("id"),
        "post_id": post_id,
        "subreddit": str(record.get("subreddit", "")).lower(),
        "created_utc": int(record.get("created_utc")),
        "created_date": created_date,
        "year": year,
        "month": f"{month:02d}",
        "body": record.get("body", ""),
        "score": record.get("score"),
        "source": source,
        "task_id": int(task_id),
        "ingested_at": now_utc_string(),
    }


def get_page_with_retry(url, params, max_retries, sleep_seconds):
    """
    Request one API page. If the API is temporarily slow or rate-limited, wait and
    retry a few times.
    """
    for attempt in range(max_retries):
        try:
            from urllib.parse import urlencode
            from urllib.request import Request, urlopen
            from urllib.error import HTTPError

            full_url = url + "?" + urlencode(params)
            request = Request(full_url, headers={"User-Agent": "macs30113-reddit-project"})

            try:
                with urlopen(request, timeout=60) as response:
                    status_code = response.getcode()
                    response_body = response.read().decode("utf-8")
            except HTTPError as e:
                status_code = e.code
                response_body = e.read().decode("utf-8", errors="replace")

            if status_code == 429:
                wait_seconds = max(30, sleep_seconds * 5 * (attempt + 1))
                time.sleep(wait_seconds)
                continue

            if status_code >= 500:
                wait_seconds = max(10, sleep_seconds * 3 * (attempt + 1))
                time.sleep(wait_seconds)
                continue

            if status_code >= 400:
                raise RuntimeError(f"HTTP {status_code}: {response_body[:200]}")

            return read_response_data(json.loads(response_body)), None

        except Exception as e:
            error_message = str(e)
            wait_seconds = max(5, sleep_seconds * (attempt + 1))
            time.sleep(wait_seconds)

    return [], error_message if "error_message" in locals() else "unknown request error"


def collect_task_records(task, page_size, sleep_seconds, max_retries, source):
    """
    Collect all API pages for one manifest task.

    This uses created_utc paging. For a course project this is simple and readable.
    """
    kind = task["kind"]
    subreddit = task["subreddit"]
    start_date = task["start_date"]
    end_date = task["end_date"]
    task_id = task["task_id"]

    url = POSTS_URL if kind == "posts" else COMMENTS_URL
    after_value = start_date
    normalized_records = []
    failed_requests = []
    page_count = 0

    while True:
        params = {
            "subreddit": subreddit,
            "after": after_value,
            "before": end_date,
            "limit": page_size,
            "sort": "asc",
        }

        page, error = get_page_with_retry(
            url=url,
            params=params,
            max_retries=max_retries,
            sleep_seconds=sleep_seconds,
        )

        if error is not None:
            failed_requests.append(
                {
                    "task_id": int(task_id),
                    "kind": kind,
                    "subreddit": subreddit,
                    "after": str(after_value),
                    "before": end_date,
                    "error": error,
                }
            )
            break

        if len(page) == 0:
            break

        page_count += 1

        for record in page:
            if "created_utc" not in record:
                continue

            if kind == "posts":
                title_ok = is_usable_text(record.get("title"))
                selftext_ok = is_usable_text(record.get("selftext"))
                if title_ok or selftext_ok:
                    normalized_records.append(normalize_post(record, source, task_id))
            else:
                if is_usable_text(record.get("body")):
                    normalized_records.append(normalize_comment(record, source, task_id))

        last_created_utc = max(int(record["created_utc"]) for record in page if "created_utc" in record)
        after_value = last_created_utc + 1

        if len(page) < page_size:
            break

        time.sleep(sleep_seconds)

    return normalized_records, failed_requests, page_count


def process_partition(rows, bucket, source, page_size, sleep_seconds, max_retries, run_id):
    """
    Run inside Spark workers. Each worker handles a group of manifest rows.
    """
    results = []

    for row in rows:
        task = row.asDict()
        task_start = time.time()

        try:
            records, failed_requests, page_count = collect_task_records(
                task=task,
                page_size=page_size,
                sleep_seconds=sleep_seconds,
                max_retries=max_retries,
                source=source,
            )

            status = "success" if len(failed_requests) == 0 else "partial_failure"

            results.append(
                {
                    "row_type": "task_result",
                    "task_id": int(task["task_id"]),
                    "subreddit": task["subreddit"],
                    "kind": task["kind"],
                    "start_date": task["start_date"],
                    "end_date": task["end_date"],
                    "status": status,
                    "n_records": len(records),
                    "page_count": page_count,
                    "failed_requests": failed_requests,
                    "runtime_seconds": round(time.time() - task_start, 2),
                }
            )
            results.extend(records)

        except Exception as e:
            results.append(
                {
                    "row_type": "task_result",
                    "task_id": int(task["task_id"]),
                    "subreddit": task["subreddit"],
                    "kind": task["kind"],
                    "start_date": task["start_date"],
                    "end_date": task["end_date"],
                    "status": "failed",
                    "n_records": 0,
                    "page_count": 0,
                    "failed_requests": [{"error": str(e)}],
                    "runtime_seconds": round(time.time() - task_start, 2),
                }
            )

        time.sleep(sleep_seconds)

    return iter(results)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True, help="Local or S3 manifest CSV path")
    parser.add_argument("--bucket", default="luchen-lab")
    parser.add_argument("--source", default="archive")
    parser.add_argument("--run-type", default="test")
    parser.add_argument(
        "--run-group",
        default="all",
        help="Optional manifest run_group to collect, for example test_4core, test_8core, or full",
    )
    parser.add_argument("--num-partitions", type=int, default=4)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--sleep-seconds", type=float, default=2)
    parser.add_argument("--max-retries", type=int, default=3)
    return parser.parse_args()


def main():
    args = parse_args()
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    start_time = time.time()

    spark = SparkSession.builder.appName("reddit-arctic-shift-ingestion").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    tasks = spark.read.csv(args.manifest, header=True, inferSchema=True)

    if args.run_group != "all" and "run_group" in tasks.columns:
        tasks = tasks.filter(F.col("run_group") == args.run_group)

    tasks = tasks.repartition(args.num_partitions)

    output_rdd = tasks.rdd.mapPartitions(
        lambda rows: process_partition(
            rows=rows,
            bucket=args.bucket,
            source=args.source,
            page_size=args.page_size,
            sleep_seconds=args.sleep_seconds,
            max_retries=args.max_retries,
            run_id=run_id,
        )
    )

    output_rows = spark.read.json(output_rdd.map(lambda item: json.dumps(item, ensure_ascii=False)))
    output_rows.cache()

    task_results = [
        row.asDict(recursive=True)
        for row in output_rows.filter(F.col("row_type") == "task_result").collect()
    ]

    records = output_rows.filter(F.col("row_type") == "record")
    posts = records.filter(F.col("record_type") == "posts").drop("row_type", "record_type")
    comments = records.filter(F.col("record_type") == "comments").drop("row_type", "record_type")

    posts.write.mode("overwrite").partitionBy("source", "subreddit", "year", "month").json(
        f"s3://{args.bucket}/raw/reddit/posts"
    )

    comments.write.mode("overwrite").partitionBy("source", "subreddit", "year", "month").json(
        f"s3://{args.bucket}/raw/reddit/comments"
    )

    number_of_posts = sum(
        item["n_records"] for item in task_results if item["kind"] == "posts"
    )
    number_of_comments = sum(
        item["n_records"] for item in task_results if item["kind"] == "comments"
    )

    failed_tasks = [
        item for item in task_results if item["status"] in ["failed", "partial_failure"]
    ]

    run_log = {
        "run_id": run_id,
        "run_type": args.run_type,
        "run_group": args.run_group,
        "source": args.source,
        "manifest": args.manifest,
        "bucket": args.bucket,
        "num_partitions": args.num_partitions,
        "page_size": args.page_size,
        "sleep_seconds": args.sleep_seconds,
        "max_retries": args.max_retries,
        "number_of_tasks": len(task_results),
        "number_of_posts_collected": number_of_posts,
        "number_of_comments_collected": number_of_comments,
        "number_of_failed_or_partial_tasks": len(failed_tasks),
        "runtime_seconds": round(time.time() - start_time, 2),
        "task_results": task_results,
    }

    log_path = f"s3://{args.bucket}/logs/collection_logs/run_{run_id}_{args.run_type}"
    spark.sparkContext.parallelize([json.dumps(run_log, ensure_ascii=False)], 1).saveAsTextFile(log_path)

    print(json.dumps(run_log, indent=2))
    print(f"Uploaded run log to {log_path}")

    spark.stop()


if __name__ == "__main__":
    main()
