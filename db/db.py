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

CSV_PATH = "./data_staging/Staged_data.csv"


def populate_job_posting_dimension():
    """
    Populate the job posting dimensional table in the database.
    """

    # Define SQL query
    sql_query = """
    INSERT INTO job_posting_dim (
        job_id, job_title, specialization, job_portal, skills, responsibilities, 
        minimum_salary, maximum_salary, minimum_experience, maximum_experience, 
        work_type, gender_preference
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Batch data for insertion
        data_batch = []

        with open(CSV_PATH, newline="", encoding="utf-8-sig") as csvfile:
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

    # Define SQL query
    sql_query = """
    INSERT INTO company_profile_dim (
        name, sector, industry, size, ticker
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (name, sector, industry, size, ticker) DO NOTHING;
    """

    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Batch data for insertion
        data_batch = []

        with open(CSV_PATH, newline="", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data_batch.append(
                    (
                        row["Company"],
                        row["Company Sector"],
                        row["Company Industry"],
                        int(row["Company Size"]),
                        row["Company Ticker"],
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


def populate_job_posting_date_dimension():
    """
    Populate the job posting date dimensional table in the database.
    """
    # Define SQL query
    sql_query = """
    INSERT INTO job_posting_date_dim (
        day, month, year
    )
    VALUES (%s, %s, %s)
    """

    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Batch data for insertion
        data_batch = []

        with open(CSV_PATH, newline="", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data_batch.append(
                    (
                        int(row["Day"]),
                        int(row["Month"]),
                        int(row["Year"]),
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


def populate_benefits_dimension():
    """
    Populate the benefits dimensional table in the database.
    """
    # Define SQL query
    sql_query = """
    INSERT INTO benefits_dim (
        retirement_plans, stock_options_or_equity_grants, parental_leave, paid_time_off, 
        flexible_work_arrangements, health_insurance, life_and_disability_insurance, employee_assistance_program, 
        health_and_wellness_facilities, employee_referral_program, transportation_benefits, bonuses_and_incentive_programs
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Batch data for insertion
        data_batch = []

        with open(CSV_PATH, newline="", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data_batch.append(
                    (
                        row["Retirement Plans"],
                        row["Stock Options or Equity Grants"],
                        row["Parental Leave"],
                        row["Paid Time Off (PTO)"],
                        row["Flexible Work Arrangements"],
                        row["Health Insurance"],
                        row["Life and Disability Insurance"],
                        row["Employee Assistance Program"],
                        row["Health and Wellness Facilities"],
                        row["Employee Referral Program"],
                        row["Transportation Benefits"],
                        row["Bonuses and Incentive Programs"],
                    )
                )

        # Use execute_batch for more efficient batch inserts
        extras.execute_batch(
            cur=cursor, sql=sql_query, argslist=data_batch, page_size=10000
        )

        conn.commit()
    except psycopg2.Error as err:
        print(f"Database error: {err}")
    finally:
        if conn:
            cursor.close()
            conn.close()


def populate_company_hq_location_dimension():
    """
    Populate the company HQ location dimensional table in the database.
    """
    # Define SQL query
    sql_query = """
    INSERT INTO company_hq_location_dim (
        country, city
    )
    VALUES (%s, %s)
    ON CONFLICT (country, city) DO NOTHING;
    """

    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Batch data for insertion
        data_batch = []

        with open(CSV_PATH, newline="", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # print(f"{row['Company HQ City']} ......... {str(row['Company HQ City'])}")
                data_batch.append(
                    (
                        row["Company HQ Country"],
                        row["Company HQ City"],
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


def populate_job_location_dimension():
    """
    Populate the job location dimensional table in the database.
    """
    # Define SQL query
    sql_query = """
    INSERT INTO job_location_dim (
        country, city, job_city_population
    )
    VALUES (%s, %s, %s)
    ON CONFLICT (country, city) DO NOTHING;
    """

    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Batch data for insertion
        data_batch = []

        with open(CSV_PATH, newline="", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data_batch.append(
                    (
                        row["Country"],
                        row["City"],
                        int(row["Job City Population"]),
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


def get_foreign_key(
    conn,
    primary_key: str,
    dimension_table: str,
    natural_key_columns: list,
    natural_key_values: list,
):
    """
    Fetches a foreign key from a dimension table based on the natural key(s).

    Example:
    ```
    SELECT primary_key FROM dimensional_table WHERE natural_key_columns = natural_key_values;
    ```

    Args:
        conn: Database connection object
        primary_key: Primary key of the table that is being queried
        dimension_table: Name of the table to query
        natural_key_columns: Name of columns in the dimension table that form the natural key
        natural_key_values: Values of the natural key to match

    Returns:
        The foreign key if a matching row is found, None otherwise
    """
    sql_query = f"SELECT {primary_key} FROM {dimension_table} WHERE " + " AND ".join(
        [f"{col} = %s" for col in natural_key_columns]
    )
    cursor = conn.cursor()
    cursor.execute(sql_query, natural_key_values)
    result = cursor.fetchone()
    cursor.close()

    if result:
        return result[0]
    else:
        return None


def populate_fact_table():
    """
    Populate the job posting fact table in the database.

    Match correct dimensions data for each row in the fact table
    then populate the fact table by defining the foreign keys
    associated with the data.

    For example, Job 123 is "Musician" and is located in "Canada".
    Look for primary key of job 123, primary of the job "Musician",
    and primary key of the country "Canada" and link those primary
    keys to a record in the fact table.
    """
    conn = psycopg2.connect(**DB_PARAMS)

    with open(CSV_PATH, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            # Example: SELECT param1 FROM param2 WHERE param3=param4;
            # where param3 and param3 are looped through automatically
            job_posting_key = get_foreign_key(
                conn, "job_id", "job_posting_dim", ["job_id"], [row["Job Id"]]
            )
            company_profile_key = get_foreign_key(
                conn,
                "company_profile_key",
                "company_profile_dim",
                ["name", "sector", "industry", "size", "ticker"],
                [
                    row["Company"],
                    row["Company Sector"],
                    row["Company Industry"],
                    row["Company Size"],
                    row["Company Ticker"],
                ],
            )
            job_posting_date_key = get_foreign_key(
                conn,
                "job_posting_date_key",
                "job_posting_date_dim",
                ["day", "month", "year"],
                [row["Day"], row["Month"], row["Year"]],
            )
            benefits_key = get_foreign_key(
                conn,
                "benefits_key",
                "benefits_dim",
                ["benefits_key"],
                [row["Surrogate Keys"]],
            )
            # print(f"asdasdasdasdasdasd {row['Company HQ City']}")
            # exit()
            company_hq_location_key = get_foreign_key(
                conn,
                "company_hq_location_key",
                "company_hq_location_dim",
                ["country", "city"],
                [row["Company HQ Country"], row["Company HQ City"]],
            )
            job_location_key = get_foreign_key(
                conn,
                "job_location_key",
                "job_location_dim",
                ["country", "city"],
                [row["Country"], row["City"]],
            )

            fact_insert_query = """
            INSERT INTO job_posting_fact (
                job_posting_key, company_profile_key, job_posting_date_key, benefits_key,
                company_hq_location_key, job_location_key
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
            """

            with conn.cursor() as cur:
                cur.execute(
                    fact_insert_query,
                    (
                        job_posting_key,
                        company_profile_key,
                        job_posting_date_key,
                        benefits_key,
                        company_hq_location_key,
                        job_location_key,
                    ),
                )
                conn.commit()


def populate_database():
    """
    Populate data from the CSV dataset file into all dimensional tables in the database.

    Read the CSV dataset file and select relevant columns for each dimensional
    table, then insert data from those columns into the corresponding columns
    of each dimensional table in the database.
    """
    stopwatch: float = None  # keep track of start time of each DB operation
    print(f"[+] Populate dimensional tables...")

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

    # --------------------------------------------------------
    print(f"[+] Populate fact table...")
    stopwatch = time.time()
    populate_fact_table()
    print(get_elapsed_time_message(stopwatch))

    # --------------------------------------------------------
    print("[+] Successfully populated all tables in the database")

    return


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
        print(f"[+] Completed all database operations")
        print(
            get_elapsed_time_message(start_time)
        )  # End of program execution to measure elapsed time
