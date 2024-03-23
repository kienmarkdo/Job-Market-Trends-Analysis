import psycopg2
import os

from dotenv import load_dotenv

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


def populate_measure_industry_year():
    """
    Adding measures to the fact table 
    
    Populate the jobs_per_industry_and_year column in the fact table
    with the measured value of 
    the number of jobs posted in each industry in each year
    """
    conn = psycopg2.connect(**DB_PARAMS)
    
    create_view = """
        CREATE VIEW jobs_per_industry_and_year AS
        SELECT D.year, P.job_id, C.industry, COUNT(P.job_id) 
        OVER (Partition by (D.year, C.industry)) AS jobs_per_industry_and_year
        FROM job_posting_date_dim D, job_posting_dim P, company_profile_dim C, job_posting_fact F
        WHERE F.job_posting_key = P.job_id AND 
        F.job_posting_date_key = D.job_posting_date_key AND 
        F.company_profile_key = C.company_profile_key;
    """
    update_fact = """
        UPDATE job_posting_fact AS f
        SET jobs_per_industry_and_year = v.jobs_per_industry_and_year
        FROM jobs_per_industry_and_year AS v
        WHERE f.job_posting_key = v.job_id;
    """
    
    with conn.cursor() as cur:
        cur.execute(create_view)
        cur.execute(update_fact)
        conn.commit()


def populate_measure_company_year():
    """
    Adding measures to the fact table 
    
    Populate the jobs_per_company_and_year column in the fact table
    with the measured value of 
    the number of jobs a company posted in each year
    """
    conn = psycopg2.connect(**DB_PARAMS)
    
    create_view = """
        CREATE VIEW jobs_per_company_and_year AS
        SELECT D.year, P.job_id, C.name, COUNT(P.job_id) 
        OVER (Partition by (D.year, C.name)) AS jobs_per_company_and_year
        FROM job_posting_date_dim D, job_posting_dim P, company_profile_dim C, job_posting_fact F
        WHERE F.job_posting_key = P.job_id AND 
        F.job_posting_date_key = D.job_posting_date_key
        AND F.company_profile_key = C.company_profile_key;
    """
    update_fact = """
        UPDATE job_posting_fact AS f
        SET jobs_per_company_and_year = v.jobs_per_company_and_year
        FROM jobs_per_company_and_year AS v
        WHERE f.job_posting_key = v.job_id;
    """
    
    with conn.cursor() as cur:
        cur.execute(create_view)
        cur.execute(update_fact)
        conn.commit()

