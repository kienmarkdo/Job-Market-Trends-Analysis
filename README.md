# Job Market Trends Analysis
An enriched data mart to analyze job market trends from 2021 to 2023 in several countries through conceptual design, physical design and data staging, OLAP queries, BI dashboard creation, and data mining.

## Setup Instructions
### Local System
- Install Python 3.x and Docker Engine (Docker Desktop)
    - Open Docker Desktop and leave it open. This keeps Docker Engine running for you to run Docker commands
### Python Environment
- Create virtual environment
    - `python -m venv venv`
- Activate venv
    - `source venv/Scripts/activate` on Windows git bash
    - `source venv/bin/activate` on UNIX
- Install Python dependencies
    - `pip install -r requirements.txt`
### Database Instance
- Make sure port 5432 is available
    - Stop the local Postgres service if it is running on your system
- Pull Docker images and run the containers
    - `docker compose up --build -d` to build the images and run the containers in the background
    - `docker ps` to verify that your containers are started
    - `docker compose down` to stop your running containers
    - `docker system prune -a` to delete all *stopped* images and containers
- Create a file to store sensitive values, such as passwords
    - Create a file named `.env` in the root of the directory
    - Open the file `.env.examples`
    - Copy the contents of `.env.examples` and paste it into `.env`
    - Replace the values with your own values

<!-- ## Docker containers
- Enter `postgres` container
    - `docker exec -it postgres bash` to enter the postgres container
    - `psql -U postgres -d postgres -a -f schema.sql` to manually create tables in the postgres container -->
## Access the Database
Instructions to interact with the Postgres database instance in the Docker container.
- `docker exec -it postgres bash` to enter the postgres container
- `psql -U postgres -d postgres` to interact with the PostgreSQL database in the container
- Refresher on some PSQL commands to get started
```sql
\dt                                    # view all tables
psql -U postgres -d postgres           # open the interactive terminal for the 'postgres' database as the 'postgres' user
SELECT * FROM job_posting_dim;         # view all records in the job_posting_dim table
SELECT COUNT(*) FROM job_posting_dim;  # to count the number of rows
```
<!-- - Note: The database was automatically created in the postgres container when `docker compose up` was executed.
- This is because the `./db/init` directory (which contains the `schema.sql` file) is mounted at `/docker-entrypoint-initdb.d` inside the container to indicate that to PostgreSQL that `schema.sql` (and any other `.sql` or `.sh` scripts present) need to be executed when the container is started up for the first time -->

## Design Process
1. Obtain and load the dataset
    - The original dataset was obtain from Kaggle https://www.kaggle.com/datasets/ravindrasinghrana/job-description-dataset 
2. Conceptual Design
    - Planning and design of Fact table and Dimension tables
3. Data Staging
    - Identify and correct errors or missing values in the data
4. Physical Design
    - Insert the data into a RDBMS (Postgres) and optimize the data for OLAP queries
    - Define aggregations and measurements for analysis
5. Data Visualization (OLAP queries and BI dashboard)
    - Generate standard OLAP operations
    - Generate explorative SQL operations
    - Create a BI dashboard to explore and visualize trends in the data
6. Data Mining
    - Leverage ML techniques to answer relevant questions regarding job market trends

# Data Staging
Our data staging code is in 'CSI4142_DataStaging_Group9.ipynb' in the data_staging folder.
If you want to run and test it:
- Download the notebook from data_staging folder
- Download the first dataset from this link: https://www.kaggle.com/datasets/ravindrasinghrana/job-description-dataset?resource=download
- Download CityPopulation.csv from data_staging folder
- Download CompanyHQLocation.csv from data_staging folder

Please make sure you have python, pandas and jupyter notebook installed on your machine.
