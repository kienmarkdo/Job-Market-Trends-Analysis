# Job Market Trends Analysis
An enriched data mart to analyze job market trends from 2021 to 2023 in several countries through conceptual design, physical design and data staging, OLAP queries, BI dashboard creation, and data mining.

# Setup Instructions
- Install Python 3.x and Docker Engine (Docker Desktop)
    - Open Docker Desktop and leave it open. This keeps Docker Engine running for you to run Docker commands
- Create virtual environment
    - `python -m venv venv`
- Activate venv
    - `source venv/Scripts/activate` on Windows git bash
    - `source venv/bin/activate` on UNIX
- Install Docker images and run the containers
    - `docker compose up -d` to start and run the containers in the background
    - `docker ps` to verify that your containers are started
    - `docker compose down` to stop your running containers
    - `docker system prune -a` to delete all *running* images and containers
- Create a file to store sensitive values, such as passwords
    - Create a file named `.env` in the root of the directory
    - Open the file `.env.examples`
    - Copy the contents of `.env.examples` and paste it into `.env`
    - Replace the values with your own values

# Design Process
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