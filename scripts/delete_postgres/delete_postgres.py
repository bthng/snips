import os
from dotenv import load_dotenv
import psycopg2
from tqdm import tqdm
from dataclasses import dataclass
import logging
from functools import wraps
import time
from datetime import datetime, timedelta

from report import generate_report


load_dotenv(".env")
logging.basicConfig(
    filename="logs/query_log.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logging.info(
            f"Function {func.__name__} {{**kwargs}} Took {total_time:.4f} seconds"
        )
        return result

    return timeit_wrapper


def connect_to_postgres():
    try:
        database = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        connection = psycopg2.connect(
            database=database, user=user, password=password, host=host, port=port
        )
        logging.info("Connected to PostgreSQL database successfully.")
        return connection
    except Exception as e:
        logging.error(f"Error: Unable to connect to the database - {e}")
        return None


@dataclass
class Query:
    filter_date: str
    limit: int = 100

    def get_run_ids(self):
        return f"SELECT run_id FROM runs WHERE create_timestamp::date ='{self.filter_date}'"

    def get_delete_query(self, offset):
        return f"""
        WITH deleted AS (
            DELETE
            FROM event_logs
            WHERE run_id IN ({self.get_run_ids()} LIMIT {self.limit} OFFSET {offset})
              AND asset_key IS NULL
              AND dagster_event_type IS DISTINCT FROM 'ASSET_MATERIALIZATION' AND dagster_event_type IS DISTINCT FROM 'ASSET_OBSERVATION'
            RETURNING *
        )
        SELECT COUNT(*) FROM deleted;
        """

    def count_rows(self):
        return f"""
            SELECT COUNT(*)
            FROM ({self.get_run_ids()}) x
        """


@timeit
def execute_delete_query(connection, filter_date):
    try:
        cursor = connection.cursor()

        query = Query(filter_date)
        count_rows_query = query.count_rows()

        cursor.execute(count_rows_query)
        num_rows_to_delete = cursor.fetchall()[0][0]
        connection.commit()
        logging.info(
            f"Number of runs to delete for {filter_date}: {num_rows_to_delete}"
        )

        total_affected_rows = 0
        for offset in tqdm(
            range(0, num_rows_to_delete, query.limit),
            desc=f"Deleting offset rows for {filter_date}",
        ):
            delete_query = query.get_delete_query(offset=offset)
            cursor.execute(delete_query)
            affected_rows = cursor.fetchall()[0][0]
            connection.commit()
            logging.info(
                f"DELETE query with offset {offset} executed successfully. Affected log rows: {affected_rows}"
            )
            total_affected_rows += affected_rows

        return total_affected_rows
    except Exception as e:
        logging.error(f"Error: Unable to execute DELETE query - {e}")
        return None
    finally:
        # Close the cursor
        cursor.close()  # type: ignore


@timeit
def execute_delete_queries(connection, filter_dates):
    try:
        for filter_date in tqdm(filter_dates, desc="Deleting rows"):
            affected_rows = execute_delete_query(
                connection=connection, filter_date=filter_date
            )
            logging.info(f"Number of logs deleted for {filter_date}: {affected_rows}")
    except Exception as e:
        logging.error(f"Error: {e}")


if __name__ == "__main__":
    # Connect to the PostgreSQL database
    start_date_params = 2023, 10, 26
    num_days = 6
    filter_dates = [
        (datetime(*start_date_params) + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(0, num_days)
    ]
    connection = connect_to_postgres()

    if connection:
        # Execute the batch of DELETE queries with a progress bar
        execute_delete_queries(
            connection=connection,
            filter_dates=filter_dates,
        )

        # Close the database connection
        connection.close()

    print("PostgreSQL connection is closed, summary:")
    print(generate_report())
    print("Remember to run `VACUUM VERBOSE ANALYZE event_logs;` to free up space!")
