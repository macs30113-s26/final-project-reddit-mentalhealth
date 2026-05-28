"""
Create Reddit API collection manifests for the final project.

A manifest is a CSV task list. Each row tells the Spark ingestion script to
collect one subreddit, one data type, and one monthly time window.

Example:
    python src/make_manifest.py \
        --start-date 2019-01-01 \
        --end-date 2019-04-01 \
        --output manifests/manifest_test_4core_2019_01_03.csv
"""

import argparse
import csv
from datetime import date, datetime
from pathlib import Path


DEFAULT_SUBREDDIT = "mentalhealth"
KINDS = ["posts", "comments"]


def parse_date(value):
    """Convert YYYY-MM-DD text into a date object."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def add_one_month(d):
    """Return the first day of the next month."""
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def make_monthly_tasks(subreddit, start_date, end_date, starting_task_id=0):
    """
    Build monthly collection tasks.

    The date window is left-closed and right-open:
    [start_date, end_date)

    For example, 2019-01-01 to 2019-02-01 means all of January 2019.
    """
    tasks = []
    current = start_date
    task_id = starting_task_id

    while current < end_date:
        next_month = add_one_month(current)
        if next_month > end_date:
            next_month = end_date

        for kind in KINDS:
            tasks.append(
                {
                    "task_id": task_id,
                    "subreddit": subreddit,
                    "kind": kind,
                    "start_date": current.isoformat(),
                    "end_date": next_month.isoformat(),
                    "year": current.year,
                    "month": f"{current.month:02d}",
                }
            )
            task_id += 1

        current = next_month

    return tasks


def write_manifest(tasks, output_path):
    """Write tasks to a CSV file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "task_id",
        "subreddit",
        "kind",
        "start_date",
        "end_date",
        "year",
        "month",
    ]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(tasks)

    print(f"Wrote {len(tasks)} tasks to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Create monthly Reddit collection manifest CSV files."
    )
    parser.add_argument("--subreddit", default=DEFAULT_SUBREDDIT)
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument("--starting-task-id", type=int, default=0)

    args = parser.parse_args()

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)

    if start_date >= end_date:
        raise ValueError("start-date must be before end-date")

    if start_date.day != 1:
        raise ValueError("start-date should be the first day of a month")

    if end_date.day != 1:
        raise ValueError("end-date should be the first day of a month")

    tasks = make_monthly_tasks(
        subreddit=args.subreddit,
        start_date=start_date,
        end_date=end_date,
        starting_task_id=args.starting_task_id,
    )

    write_manifest(tasks, args.output)


if __name__ == "__main__":
    main()
