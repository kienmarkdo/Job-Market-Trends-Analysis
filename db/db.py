import csv
import psycopg2
import time
import os

from dotenv import load_dotenv
from psycopg2 import extras
from measurements import populate_measure_industry_year, populate_measure_company_year

# Load the environment variables from .env file
load_dotenv()

# Define database connection parameters
DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "localhost",
    "port": "5432",
}

CSV_PATH = "./data_staging/Staged_data.csv"


def populate_job_posting_dimension():
    """
    Populate the job posting dimensional table in the database.
    """

    # Define SQL query
    sql_query = """
    INSERT INTO job_posting_dim (
        job_id, job_title, qualifications, specialization, job_portal, skills, responsibilities, 
        minimum_salary, maximum_salary, minimum_experience, maximum_experience, 
        work_type, gender_preference
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (job_id) DO NOTHING;
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
                        row["Qualifications"],
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
    ON CONFLICT (day, month, year) DO NOTHING;
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
    ON CONFLICT (retirement_plans, stock_options_or_equity_grants, parental_leave, paid_time_off, flexible_work_arrangements, health_insurance, life_and_disability_insurance, employee_assistance_program, health_and_wellness_facilities, employee_referral_program, transportation_benefits, bonuses_and_incentive_programs) DO NOTHING;
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


def create_dimension_caches() -> dict[str, dict]:
    """
    Create in-memory caches for all dimension tables.

    Made to optimize the function to populate the fact table.

    Rather than making a query to the database while traversing through
    each row of the dataset to populate the fact table's foreign keys,
    this function fetches the primary key of each row in every dimension
    table and stores them in a dictionary as in-memory caches.

    Return:
        The caches dictionary storing primary keys of all dimension tables.
    """
    conn = psycopg2.connect(**DB_PARAMS)
    caches = {
        "job_posting": {},
        "company_profile": {},
        "job_posting_date": {},
        "benefits": {},
        "company_hq_location": {},
        "job_location": {},
    }

    with conn.cursor() as cur:
        # Cache job_posting_dim keys
        cur.execute("SELECT job_id FROM job_posting_dim;")
        for key in cur.fetchall():  # returns a tuplein the format (id,)
            caches["job_posting"][key[0]] = key[0]

        # Cache company_profile_dim keys
        cur.execute(
            "SELECT name, sector, industry, size, ticker, company_profile_key FROM company_profile_dim;"
        )
        for name, sector, industry, size, ticker, key in cur.fetchall():
            caches["company_profile"][(name, sector, industry, size, ticker)] = key

        # Cache job_posting_date_dim keys
        cur.execute(
            "SELECT day, month, year, job_posting_date_key FROM job_posting_date_dim;"
        )
        for day, month, year, key in cur.fetchall():
            caches["job_posting_date"][(day, month, year)] = key

        # Cache benefits_dim keys
        cur.execute(
            "SELECT retirement_plans, stock_options_or_equity_grants, parental_leave, paid_time_off, flexible_work_arrangements, health_insurance, life_and_disability_insurance, employee_assistance_program, health_and_wellness_facilities, employee_referral_program, transportation_benefits, bonuses_and_incentive_programs, benefits_key FROM benefits_dim;"
        )
        for a, b, c, d, e, f, g, h, i, j, k, l, key in cur.fetchall():
            caches["benefits"][(a, b, c, d, e, f, g, h, i, j, k, l)] = key

        # Cache company_hq_location_dim keys
        cur.execute(
            "SELECT country, city, company_hq_location_key FROM company_hq_location_dim;"
        )
        for country, city, key in cur.fetchall():
            caches["company_hq_location"][(country, city)] = key

        # Cache job_location_dim keys
        cur.execute("SELECT country, city, job_location_key FROM job_location_dim;")
        for country, city, key in cur.fetchall():
            caches["job_location"][(country, city)] = key

    return caches


def prepare_data_for_fact_table_insertion(caches: dict[str, dict]):
    """
    Fetch keys from cache for fact table insertion.

    Made to optimize the function to populate the fact table.

    Iterates through the CSV dataset, finds the primary key of
    every row in each dimension table, then stores the result
    in a list of tuples in the exact format that is required to
    insert the data into the database.

    Args:
        caches: a dictionary of dimension tables and their primary keys

    Returns:
        All rows to be inserted in the fact table.
    """

    data_for_insertion: list[tuple] = []

    with open(CSV_PATH, "r", newline="", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Directly use job_id as a foreign key if it's a primary key in job_posting_dim
            job_posting_key = caches["job_posting"].get((int(row["Job Id"])))

            # Fetch other foreign keys from cache
            company_profile_key = caches["company_profile"].get(
                (
                    row["Company"],
                    row["Company Sector"],
                    row["Company Industry"],
                    int(row["Company Size"]),
                    row["Company Ticker"],
                )
            )

            job_posting_date_key = caches["job_posting_date"].get(
                (int(row["Day"]), int(row["Month"]), int(row["Year"]))
            )

            # Getting bool values this way for data conversion and matching (Python True is not the same as PostgreSQL True)
            benefits_key = caches["benefits"].get(
                (
                    row["Retirement Plans"].lower() == "true",
                    row["Stock Options or Equity Grants"].lower() == "true",
                    row["Parental Leave"].lower() == "true",
                    row["Paid Time Off (PTO)"].lower() == "true",
                    row["Flexible Work Arrangements"].lower() == "true",
                    row["Health Insurance"].lower() == "true",
                    row["Life and Disability Insurance"].lower() == "true",
                    row["Employee Assistance Program"].lower() == "true",
                    row["Health and Wellness Facilities"].lower() == "true",
                    row["Employee Referral Program"].lower() == "true",
                    row["Transportation Benefits"].lower() == "true",
                    row["Bonuses and Incentive Programs"].lower() == "true",
                )
            )

            company_hq_location_key = caches["company_hq_location"].get(
                (row["Company HQ Country"], row["Company HQ City"])
            )

            job_location_key = caches["job_location"].get((row["Country"], row["City"]))

            if all(
                [
                    job_posting_key,
                    company_profile_key,
                    job_posting_date_key,
                    benefits_key,
                    company_hq_location_key,
                    job_location_key,
                ]
            ):
                data_for_insertion.append(
                    (
                        job_posting_key,
                        company_profile_key,
                        job_posting_date_key,
                        benefits_key,
                        company_hq_location_key,
                        job_location_key,
                    )
                )

    return data_for_insertion


def populate_fact_table(data_for_insertion: list[tuple]):
    """
    Populate the job posting fact table in the database using bulk insert.

    Match correct dimensions data for each row in the fact table
    then populate the fact table by defining the foreign keys
    associated with the data.

    For example, Job 123 is "Musician" and is located in "Canada".
    Look for primary key of job 123, primary of the job "Musician",
    and primary key of the country "Canada" and link those primary
    keys to a record in the fact table.

    Args:
        data_for_insertion: data prepared for insertion into the fact table
    """
    conn = psycopg2.connect(**DB_PARAMS)

    insert_query = """
    INSERT INTO job_posting_fact (
        job_posting_key, company_profile_key, job_posting_date_key, benefits_key, 
        company_hq_location_key, job_location_key
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (job_posting_key, company_profile_key, job_posting_date_key, benefits_key, company_hq_location_key, job_location_key) DO NOTHING;
    """

    with conn.cursor() as cur:
        extras.execute_batch(cur, insert_query, data_for_insertion, page_size=10000)
        conn.commit()


def populate_database():
    """
    Populate data from the CSV dataset file into all dimension tables in the database.

    Read the CSV dataset file and select relevant columns for each dimensional
    table, then insert data from those columns into the corresponding columns
    of each dimensional table in the database.
    """
    stopwatch: float = None  # keep track of start time of each DB operation
    print(f"[+] Populate dimension tables...")

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
    caches: dict[str, dict] = create_dimension_caches()
    print(f"Done with caching")
    data_for_insertion: list[tuple] = prepare_data_for_fact_table_insertion(caches)
    print(f"Done preparing data for fact table insertion")
    populate_fact_table(data_for_insertion)
    print(f"Done populating fact table")
    print(get_elapsed_time_message(stopwatch))

    # --------------------------------------------------------
    print(f"Populating jobs per industry and year measure")
    stopwatch = time.time()
    populate_measure_industry_year()
    print(get_elapsed_time_message(stopwatch))

    # --------------------------------------------------------
    print(f"Populating jobs per company and year measure")
    stopwatch = time.time()
    populate_measure_company_year()
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
