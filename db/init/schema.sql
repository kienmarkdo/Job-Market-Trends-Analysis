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
    job_id BIGINT PRIMARY KEY,
    job_title TEXT,
    qualifications TEXT,
    specialization TEXT,
    job_portal TEXT,
    skills TEXT,
    responsibilities TEXT,
    minimum_salary DECIMAL,
    maximum_salary DECIMAL,
    minimum_experience INT,
    maximum_experience INT,
    work_type TEXT,
    gender_preference TEXT,
    CONSTRAINT unique_job UNIQUE (job_id)
);

-- Company Profile Dimension
CREATE TABLE company_profile_dim (
    company_profile_key SERIAL PRIMARY KEY,
    name TEXT,
    sector TEXT,
    industry TEXT,
    size BIGINT,
    ticker TEXT, -- company's stock name/symbol, if it exists
    CONSTRAINT unique_company UNIQUE (name, sector, industry, size, ticker)
);

-- Job Posting Date Dimension
CREATE TABLE job_posting_date_dim (
    job_posting_date_key SERIAL PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    CONSTRAINT unique_date UNIQUE (day, month, year)
);

-- Benefits Dimension
CREATE TABLE benefits_dim (
    benefits_key SERIAL PRIMARY KEY,
    retirement_plans BOOLEAN,
    stock_options_or_equity_grants BOOLEAN,
    parental_leave BOOLEAN,
    paid_time_off BOOLEAN,
    flexible_work_arrangements BOOLEAN,
    health_insurance BOOLEAN,
    life_and_disability_insurance BOOLEAN,
    employee_assistance_program BOOLEAN,
    health_and_wellness_facilities BOOLEAN,
    employee_referral_program BOOLEAN,
    transportation_benefits BOOLEAN,
    bonuses_and_incentive_programs BOOLEAN,
    CONSTRAINT unique_benefits UNIQUE (
        retirement_plans, stock_options_or_equity_grants, parental_leave, paid_time_off, 
        flexible_work_arrangements, health_insurance, life_and_disability_insurance, 
        employee_assistance_program, health_and_wellness_facilities, employee_referral_program, 
        transportation_benefits, bonuses_and_incentive_programs
    )
);

-- Company HQ Location Dimension
CREATE TABLE company_hq_location_dim (
    company_hq_location_key SERIAL PRIMARY KEY,
    country TEXT,
    city TEXT,
    CONSTRAINT unique_hq UNIQUE (country, city)
);

-- Job Location Dimension
CREATE TABLE job_location_dim (
    job_location_key SERIAL PRIMARY KEY,
    country TEXT,
    city TEXT,
    job_city_population BIGINT,
    CONSTRAINT unique_job_location UNIQUE (country, city)
);

-- Create Fact Table

-- Job Posting Fact Table
CREATE TABLE job_posting_fact (
    job_posting_key BIGINT REFERENCES job_posting_dim(job_id),
    company_profile_key BIGINT REFERENCES company_profile_dim(company_profile_key),
    job_posting_date_key BIGINT REFERENCES job_posting_date_dim(job_posting_date_key),
    benefits_key BIGINT REFERENCES benefits_dim(benefits_key),
    company_hq_location_key BIGINT REFERENCES company_hq_location_dim(company_hq_location_key),
    job_location_key BIGINT REFERENCES job_location_dim(job_location_key),
    jobs_per_industry_and_year BIGINT,
    jobs_per_company_and_year BIGINT,
    PRIMARY KEY (job_posting_key, company_profile_key, job_posting_date_key, benefits_key, company_hq_location_key, job_location_key)
);
