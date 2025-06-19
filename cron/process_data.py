import csv
import itertools
import os
import re
import subprocess

from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras

from db.database import get_db_connection


def calculate_minimal_distance(changed_file_path, test_file_path):
    """
    Calculates the minimal directory-level distance between a changed file and a test file.
    The distance is defined as the sum of path components minus twice the common prefix length.
    """
    if not changed_file_path or not test_file_path:
        return 1000  # Return a large distance for invalid inputs

    test_file_path_splits = test_file_path.split("/")

    # Handle both single and multiple changed files
    if isinstance(changed_file_path, str):
        changed_files = [{"filename": changed_file_path}]
    else:
        changed_files = changed_file_path

    min_distance = 1000

    for changed_file_entry in changed_files:
        current_changed_file_path = changed_file_entry["filename"]
        changed_file_path_splits = current_changed_file_path.split("/")

        # Calculate distance based on path components
        distance = len(changed_file_path_splits) + len(test_file_path_splits)

        # Subtract common path prefixes to reduce distance
        for i in range(min(len(changed_file_path_splits) - 1, len(test_file_path_splits) - 1)):
            if test_file_path_splits[i] == changed_file_path_splits[i]:
                distance -= 2
            else:
                break

        min_distance = min(min_distance, distance)

    return min_distance


def calculate_common_tokens(changed_file_path, test_file_path):
    """
    Calculates the number of common tokens between a changed file path and a test file path.
    Tokens are extracted by splitting the paths by '/' and '.'.
    """
    if not changed_file_path or not test_file_path:
        return 0

    changed_tokens = set(token for token in re.split(r"[/.]", changed_file_path) if token)
    test_tokens = set(token for token in re.split(r"[/.]", test_file_path) if token)
    return len(changed_tokens.intersection(test_tokens))


def get_test_run_key(test_case_file_path, test_case_name):
    """Generates a unique key for a test run based on its file path and name."""
    return f"{test_case_file_path}::::{test_case_name}"


def process_test_run(test_run, pr_creation_date, failed_run_dates):
    """
    Processes a single test run to extract features related to historical failures.
    """
    data = {
        "test_case_file_path": test_run["test_case_file_path"],
        "test_case_name": test_run["test_case_name"],
        "actual_result": 0 if test_run["actual_result"] == "passed" else 1,
        "failures_7_days": 0,
        "failures_14_days": 0,
        "failures_28_days": 0,
    }

    # Calculate failure counts over different time windows
    for failed_date in failed_run_dates:
        if failed_date > (pr_creation_date - timedelta(days=7)):
            data["failures_7_days"] += 1
        if failed_date > (pr_creation_date - timedelta(days=14)):
            data["failures_14_days"] += 1
        if failed_date > (pr_creation_date - timedelta(days=28)):
            data["failures_28_days"] += 1

    return data


def process_file_change(file_change_path, pr_creation_date, file_change_dates):
    """
    Processes a single file change to extract features related to recent changes.
    """
    file_name, file_extension = os.path.splitext(file_change_path)
    data = {
        "file_name": file_name,
        "file_extension": file_extension[1:] if file_extension else "",
        "changes_3_days": 0,
        "changes_14_days": 0,
    }

    # Calculate change counts over different time windows
    for change_date in file_change_dates:
        if change_date >= (pr_creation_date - timedelta(days=3)):
            data["changes_3_days"] += 1
        if change_date >= (pr_creation_date - timedelta(days=14)):
            data["changes_14_days"] += 1

    return data


def process_data_for_repo(repo_name, checkpoint_date):
    """
    Fetches data from the database, processes it to create features,
    and saves the resulting dataset to a CSV file.
    """
    db_connection = get_db_connection()
    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Fetch pull request details from the database
    if checkpoint_date:
        start_date = checkpoint_date - timedelta(days=28)
        db_cursor.execute(
            """
            SELECT pr_link, repo_name, date_of_pr, list_of_authors
            FROM repo_pr_details
            WHERE repo_name = %s AND date_of_pr >= %s
            """,
            (repo_name, start_date),
        )
    else:
        db_cursor.execute(
            """
            SELECT pr_link, date_of_pr, list_of_authors
            FROM repo_pr_details
            WHERE repo_name = %s
            """,
            (repo_name,),
        )
    pull_request_details = db_cursor.fetchall()

    if not pull_request_details:
        print(f"No PR details found for repository: {repo_name}")
        return None

    pull_request_links = tuple(pr["pr_link"] for pr in pull_request_details)

    # Fetch file change data for all relevant pull requests
    db_cursor.execute(
        """
        SELECT pr_link, files_paths_changed
        FROM pr_to_files_changed_mapping
        WHERE pr_link IN %s
        """,
        (pull_request_links,),
    )
    pr_files_changed_data = db_cursor.fetchall()

    # Group file change dates by file path
    files_changed_history = {}
    for row in pr_files_changed_data:
        pr_link = row["pr_link"]
        files_paths_changed = row["files_paths_changed"]
        if files_paths_changed not in files_changed_history:
            files_changed_history[files_paths_changed] = []
        changed_date = next(
            (pr["date_of_pr"] for pr in pull_request_details if pr["pr_link"] == pr_link),
            None,
        )
        if changed_date:
            files_changed_history[files_paths_changed].append(changed_date)

    # Fetch test run data for all relevant pull requests
    db_cursor.execute(
        """
        SELECT pr_link, test_case_file_path, test_case_name, actual_result
        FROM pr_to_test_runs_mapping
        WHERE pr_link IN %s
        """,
        (pull_request_links,),
    )
    pr_test_runs_data = db_cursor.fetchall()

    # Group failed test run dates by test case
    test_runs_failure_history = {}
    for row in pr_test_runs_data:
        key = get_test_run_key(row["test_case_file_path"], row["test_case_name"])
        if key not in test_runs_failure_history:
            test_runs_failure_history[key] = []

        if row["actual_result"] == "failed":
            changed_date = next(
                (pr["date_of_pr"] for pr in pull_request_details if pr["pr_link"] == row["pr_link"]),
                None,
            )
            if changed_date:
                test_runs_failure_history[key].append(changed_date)

    feature_data = []

    # Process each pull request to generate feature rows
    for pr in pull_request_details:
        pr_link = pr["pr_link"]
        pr_creation_date = pr["date_of_pr"]
        distinct_author_count = len(pr["list_of_authors"]) if pr["list_of_authors"] else 0

        pr_files_changed = [
            row["files_paths_changed"]
            for row in pr_files_changed_data
            if row["pr_link"] == pr_link
        ]
        num_files_changed = len(pr_files_changed)

        pr_test_runs = [
            {
                "test_case_file_path": row["test_case_file_path"],
                "test_case_name": row["test_case_name"],
                "actual_result": row["actual_result"],
            }
            for row in pr_test_runs_data
            if row["pr_link"] == pr_link
        ]

        processed_test_runs = [
            process_test_run(
                test_run,
                pr_creation_date,
                test_runs_failure_history.get(
                    get_test_run_key(
                        test_run["test_case_file_path"], test_run["test_case_name"]
                    ),
                    [],
                ),
            )
            for test_run in pr_test_runs
        ]

        processed_file_changes = [
            process_file_change(
                file_change,
                pr_creation_date,
                files_changed_history.get(file_change, []),
            )
            for file_change in pr_files_changed
        ]

        # Create a feature row for each combination of test run and file change
        for test_run, file_change in itertools.product(
            processed_test_runs, processed_file_changes
        ):
            feature_data.append(
                {
                    **test_run,
                    **file_change,
                    "distinct_author": distinct_author_count,
                    "common_tokens": calculate_common_tokens(
                        test_run["test_case_file_path"], file_change["file_name"]
                    ),
                    "minimal_distance": calculate_minimal_distance(
                        test_run["test_case_file_path"], file_change["file_name"]
                    ),
                    "num_files_changed": num_files_changed,
                    "date_of_pr": pr_creation_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "pr_link": pr_link,
                }
            )

    # Write the processed data to a CSV file
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_file = f"csv/{repo_name}_{timestamp}.csv"
    if feature_data:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        fieldnames = list(feature_data[0].keys())
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(feature_data)

    db_cursor.close()
    db_connection.close()

    return output_file


def train_model(repo_name, processed_data_path):
    """Triggers the model training script for a given repository."""
    print(f"Initiating model training for {repo_name}...")

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    model_path = f"model/{repo_name}_{timestamp}_model.joblib"
    metrics_path = f"metrics/{repo_name}_{timestamp}_report.json"

    command = [
        "python",
        os.path.join(os.path.dirname(__file__), "train_model.py"),
        "--repo_name",
        repo_name,
        "--data_path",
        processed_data_path,
        "--model_path",
        model_path,
        "--metrics_path",
        metrics_path,
    ]

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"Model training for {repo_name} completed successfully.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error during model training for {repo_name}:")
        print(e.stderr)


def main():
    """
    Main function to orchestrate data processing and model training for all repositories.
    """
    db_connection = get_db_connection()
    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Fetch all repositories from the database
    db_cursor.execute("SELECT repo_name FROM repositories")
    repositories = db_cursor.fetchall()

    for repo in repositories:
        repo_name = repo["repo_name"]

        # Get the latest checkpoint date for incremental processing
        db_cursor.execute(
            "SELECT MAX(date_of_checkpoint) as max_checkpoint FROM checkpoint_details WHERE repo_name = %s",
            (repo_name,),
        )
        result = db_cursor.fetchone()
        latest_checkpoint = (
            result["max_checkpoint"] if result and result["max_checkpoint"] else None
        )

        # Process data and train the model for the repository
        processed_data_path = process_data_for_repo(repo_name, latest_checkpoint)
        if processed_data_path:
            train_model(repo_name, processed_data_path)

    db_cursor.close()
    db_connection.close()


if __name__ == "__main__":
    main()
