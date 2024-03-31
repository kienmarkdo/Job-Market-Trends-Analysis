# Job Market Trends Analysis
An enriched data mart to analyze job market trends from 2021 to 2023 in several countries through conceptual design, physical design and data staging, OLAP queries, BI dashboard creation, and data mining.

## Setup Instructions
### Local System
- Install Python 3.x and Docker Engine (Docker Desktop)
    - Open Docker Desktop and leave it open. This keeps Docker Engine running for you to run Docker commands
### Python Environment
```console
# Create a virtual environment and install Python dependencies
# A venv is an isolated environment to store yours packages so that you don't have to worry about filling up your computer
python -m venv venv
source venv/Scripts/activate  # Windows (git bash)
source venv/bin/activate      # UNIX

# Install all dependencies
pip install -r requirements.txt

# (OPTIONAL) Install all dependencies individually
# Use this if you do not want to run pip install -r requirements.txt
pip install psycopg2
pip install python-dotenv
pip install pandas
pip install notebook
pip install scikit-learn
pip install matplotlib
pip install numpy
```
### Database Instance
- Make sure port 5432 is available
    - Stop the local Postgres service if it is running on your system
- Create a file to store sensitive values, such as passwords
    - Create a file named `.env` in the root of the directory
    - Open the file `.env.examples`
    - Copy the contents of `.env.examples` and paste it into `.env`
    - Replace the values with your own values
- Pull Docker images and run the containers
    - `docker compose up --build -d` to build the images and run the containers in the background
    - `docker ps` to verify that your containers are started
    - `docker compose down` to stop your running containers
    - `docker system prune -a` to delete all *stopped* images and containers

### Populate Database
Now that the database instance and the schema are created, the db needs to be populated
- `python db/db.py` populates all tables with data, including measurements

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

## Data Staging
Our data staging code is in `CSI4142_DataStaging_Group8.ipynb` in the `data_staging` folder.

To run the notebook:
- Ensure Python, Pandas, and Jupyter Notebook are installed
- Download the first dataset `job_descriptions.csv` from this link: https://www.kaggle.com/datasets/ravindrasinghrana/job-description-dataset?resource=download
- Place the `job_descriptions.csv` into the `data_staging` folder
- Open the terminal in the root of the project. Run the following command to open the notebook in your browser.
```console
jupyter notebook
```
- Click on the `data_staging` to navigate to it
- Click on the `CSI4142_DataStaging_Group8.ipynb` notebook to open it
- In the `Run` tab, click `Run All Cells` to run all cells

## Data Mining
- Data Summarization and Transformation
```console
python data_mining/preprocess.py              # Runs both data summarization and data transformation
python data_mining/preprocess.py --summarize  # Runs data summarization, displaying boxplots, histograms, scatter plots
python data_mining/preprocess.py --transform  # Runs data preprocessing, including data transformation and feature selection

python data_mining/preprocess.py -h           # View help menu
```

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

