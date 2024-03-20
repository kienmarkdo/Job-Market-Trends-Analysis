------------------------------------------------------------------------------------------------------------
-- Development Notes / Design Decisions

-- Naming conventions refresher: 
    -- https://stackoverflow.com/questions/4702728/relational-table-naming-convention/4703155#4703155
    -- https://www.postgresql.org/docs/7.0/syntax525.htm
    -- https://www.geeksforgeeks.org/postgresql-naming-conventions/

-- Store ID as VARCHAR or INT? https://365datascience.com/question/customer-id-data-type/
-- Postgres INT types: https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-INT

-- TEXT versus VARCHAR? None. No performance difference. 
    -- https://stackoverflow.com/questions/4848964/difference-between-text-and-varchar-character-varying

------------------------------------------------------------------------------------------------------------

-- Create Dimension Tables

-- Job Posting Dimension
CREATE TABLE job_posting_dim (
    job_posting_key SERIAL PRIMARY KEY,
    job_id BIGINT,
    job_title TEXT,
    specialization TEXT,
    job_portal TEXT,
    skills TEXT,
    responsibilities TEXT,
    minimum_salary DECIMAL,
    maximum_salary DECIMAL,
    minimum_experience INT,
    maximum_experience INT,
    work_type TEXT,
    gender_preference TEXT
);

-- Company Profile Dimension
CREATE TABLE company_profile_dim (
    company_profile_key SERIAL PRIMARY KEY,
    name TEXT,
    sector TEXT,
    industry TEXT,
    size TEXT,
    ticker TEXT
);

-- Job Posting Date Dimension
CREATE TABLE job_posting_date_dim (
    job_posting_date_key SERIAL PRIMARY KEY,
    day INT,
    month INT,
    year INT
);

-- Benefits Dimension
CREATE TABLE benefits_dim (
    benefits_key SERIAL PRIMARY KEY,
    retirement_plans TEXT,
    stock_options_or_equity_grants TEXT,
    parental_leave TEXT,
    paid_time_off TEXT,
    flexible_work_arrangements TEXT,
    health_insurance TEXT,
    life_and_disability_insurance TEXT,
    employee_assistance_program TEXT,
    health_and_wellness_facilities TEXT,
    employee_referral_program TEXT,
    transportation_benefits TEXT,
    bonuses_and_incentive_programs TEXT
);

-- Company HQ Location Dimension
CREATE TABLE company_hq_location_dim (
    company_hq_location_key SERIAL PRIMARY KEY,
    country TEXT,
    city TEXT
    -- job_city_population NULL ?
);

-- Job Location Dimension
CREATE TABLE job_location_dim (
    job_location_key SERIAL PRIMARY KEY,
    country TEXT,
    city TEXT,
    job_city_population INT
);

-- Create Fact Table

-- Job Posting Fact Table
CREATE TABLE job_posting_fact (
    job_posting_key INT REFERENCES job_posting_dim(job_posting_key),
    company_profile_key INT REFERENCES company_profile_dim(company_profile_key),
    job_posting_date_key INT REFERENCES job_posting_date_dim(job_posting_date_key),
    benefits_key INT REFERENCES benefits_dim(benefits_key),
    company_hq_location_key INT REFERENCES company_hq_location_dim(company_hq_location_key),
    job_location_key INT REFERENCES job_location_dim(job_location_key),
    PRIMARY KEY (job_posting_key, company_profile_key, job_posting_date_key, benefits_key, company_hq_location_key, job_location_key)
);