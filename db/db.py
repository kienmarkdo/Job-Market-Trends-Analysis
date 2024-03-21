import csv
import psycopg2
import time
import os

from dotenv import load_dotenv
from psycopg2 import extras


# Load the environment variables from .env file
load_dotenv()

# Define database connection parameters
DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "localhost",  # Or the appropriate host address
    "port": "5432",  # Default PostgreSQL port
}


def populate_job_posting_dimension():
    """
    Populate the job posting dimensional table in the database.
    """
    csv_path = "./data_staging/Staged_data.csv"

    # Define SQL query
    sql_query = """
    INSERT INTO job_posting_dim (
        job_id, job_title, specialization, job_portal, skills, responsibilities, 
        minimum_salary, maximum_salary, minimum_experience, maximum_experience, 
        work_type, gender_preference
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        conn: psycopg2._T_conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Batch data for insertion
        data_batch = []

        with open(csv_path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data_batch.append(
                    (
                        int(row["Job Id"]),
                        row["Job Title"],
                        row["Specialization"],
                        row["Job Portal"],
                        row["Skills"],
                        row["Responsibilities"],
                        int(row["Minimum Salary"]),
                        int(row["Maximum Salary"]),
                        int(row["Minimum Experience (years)"]),
                        int(row["Maximum Experience (years)"]),
                        row["Work Type"],
                        row["Gender Preference"],
                    )
                )

        # Use execute_batch for more efficient batch inserts
        extras.execute_batch(
            cur=cursor, sql=sql_query, argslist=data_batch, page_size=10000
        )  # modify page_size to get different performance / memory usage

        conn.commit()
    except psycopg2.Error as err:
        print(f"Database error: {err}")
    finally:
        if conn:
            cursor.close()
            conn.close()


def populate_company_profile_dimension():
    """
    Populate the company profile dimensional table in the database.
    """
    pass


def populate_job_posting_date_dimension():
    """
    Populate the job posting date dimensional table in the database.
    """
    pass


def populate_benefits_dimension():
    """
    Populate the benefits dimensional table in the database.
    """
    pass


def populate_company_hq_location_dimension():
    """
    Populate the company HQ location dimensional table in the database.
    """
    pass


def populate_job_location_dimension():
    """
    Populate the job location dimensional table in the database.
    """
    pass


def populate_database():
    """
    Populate data from the CSV dataset file into all dimensional tables in the database.

    Read the CSV dataset file and select relevant columns for each dimensional
    table, then insert data from those columns into the corresponding columns
    of each dimensional table in the database.
    """
    stopwatch: float = None  # keep track of start time of each DB operation
    print(f"[+] Populate dimensional tables (in progress)")

    print(f"Populating job posting dimension table")
    stopwatch = time.time()
    populate_job_posting_dimension()
    print(get_elapsed_time_message(stopwatch))

    print(f"Populating company profile dimension table")
    stopwatch = time.time()
    populate_company_profile_dimension()
    print(get_elapsed_time_message(stopwatch))

    print(f"Populating job posting date dimension table")
    stopwatch = time.time()
    populate_job_posting_date_dimension()
    print(get_elapsed_time_message(stopwatch))

    print(f"Populating benefits dimension table")
    stopwatch = time.time()
    populate_benefits_dimension()
    print(get_elapsed_time_message(stopwatch))

    print(f"Populating company HQ location dimension table")
    stopwatch = time.time()
    populate_company_hq_location_dimension()
    print(get_elapsed_time_message(stopwatch))

    print(f"Populating job location dimension table")
    stopwatch = time.time()
    populate_job_location_dimension()
    print(get_elapsed_time_message(stopwatch))


def get_elapsed_time_message(start_time: float) -> str:
    """
    Calculates the program's elapsed time since it was executed to when this
    function is called and returns a message for logging purposes.

    Args:
        start_time: UNIX time of when the program was initially executed

    Returns:
        A log message of the program's elapsed time in seconds.
    """
    end_time = time.time()  # End of program execution to measure elapsed time
    elapsed_time_seconds = "{:.6f}".format(
        end_time - start_time
    )  # Save elapsed time (up to 6 decimal digits)
    return f"Total elapsed time: {elapsed_time_seconds} seconds\n"


if __name__ == "__main__":
    start_time = time.time()  # Start of program execution to measure elapsed time
    try:
        populate_database()
    finally:
        print(f"[+] The database has been populated successfully")
        print(
            get_elapsed_time_message(start_time)
        )  # End of program execution to measure elapsed time
