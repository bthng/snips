"""
Report module to get statistics
"""

import re
from collections import defaultdict
from datetime import datetime
from numpy import nan_to_num
import pandas as pd

log_file_path = "logs/query_log.log"

number_of_logs_pattern = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*Number of (logs|runs) (deleted|to delete) for (\d{4}-\d{2}-\d{2}): (\d+)$"
)

# Data structures to store parsed information
logs_deleted_by_date = defaultdict(int)
time_taken_by_date = defaultdict(float)

# df = pd.DataFrame(columns=["date", "time_taken", "num_logs_deleted", "num_runs"])
rows = []

# Parse the log file
def generate_report():
    with open(log_file_path, "r") as log_file:
        current_date = None
        start_time = None

        for line in log_file:

            # Match "Number of logs" and "Number of runs" entries
            logs_match = number_of_logs_pattern.match(line)
            if logs_match:
                timestamp = datetime.strptime(logs_match.group(1), "%Y-%m-%d %H:%M:%S")
                action = logs_match.group(2)
                date = datetime.strptime(logs_match.group(4), "%Y-%m-%d").date()
                count = int(logs_match.group(5))
                rows.append([date, timestamp, action, count])

    df = pd.DataFrame(columns=["date", "timestamp", "action", "count"], data=rows)
    df_runs = df[df["action"]=="runs"].sort_values("date")
    df_logs = df[df["action"]=="logs"].sort_values("date")

    df_combined = pd.merge(df_runs, df_logs, on=["date"], how="inner", suffixes=("_runs", "_logs"))
    df_combined["time_taken"] = df_combined["timestamp_logs"] - df_combined["timestamp_runs"]
    df_combined["time_taken_in_mins"] = df_combined["time_taken"].apply(lambda x: round(x.total_seconds()/60, 1))
    df_combined = df_combined[["date", "time_taken_in_mins", "count_logs", "count_runs"]]
    df_combined.columns = ["date", "time_taken_in_mins", "deleted_logs", "num_runs"]
    return df_combined

if __name__ == "__main__":
    df_combined = generate_report()
    print("REPORT\n===============")
    print(df_combined)
