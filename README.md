# Automated Spotify Data Pipeline – ETL with Airflow and PostgreSQL
## Table of Contents
- [Project Overview](#project-overview)
- [Key Features](#key-features)
- [Technologies Used](#technologies-used)
- [ETL Process Overview](#etl-process-overview)
- [Usage](#usage)
- [Airflow DAG Overview](#airflow-dag-overview)

## Project Overview
This project implements an ETL (Extract, Transform, Load) pipeline that pulls data from the Spotify API, processes it using Python, and stores the results in a PostgreSQL database. The pipeline is orchestrated using Apache Airflow to ensure the smooth and reliable scheduling of tasks, allowing for automated data extraction, transformation, and loading into a relational database.

The goal is to collect data about songs and artists from the Spotify API, transform it to derive insights, and load it into PostgreSQL for further analysis.

## Key Features
- **Spotify Data Extraction**: Extract song and artist data from Spotify using their API.
- **Data Transformation**: Process and clean the raw data to make it suitable for storage and analysis.
- **Data Loading**: Store transformed data in PostgreSQL for efficient querying and analytics.
- **Airflow Orchestration**: Schedule and manage the ETL pipeline with DAGs for automation and reliability.
- **Logging and Monitoring**: Keep track of ETL jobs using Airflow's built-in monitoring capabilities.

## Technologies Used
- **Python**: For scripting ETL logic.
- **Apache Airflow**: For orchestration, scheduling, and monitoring the ETL pipeline.
- **PostgreSQL**: For storing processed data.
- **Spotify API**: For extracting song and artist data.

## ETL Process Overview
1. **Extract**: 
   - Connect to Spotify's API and extract data related to songs, albums, artists, and playlists.
2. **Transform**: 
   - Clean and preprocess the extracted data.
   - Normalize and restructure data for storage.
3. **Load**: 
   - Load transformed data into PostgreSQL tables for further querying and analysis.

## Usage
Once the setup is complete, the ETL pipeline will:
- Extract data from the Spotify API on a daily/hourly schedule.
- Process and clean the extracted data.
- Store the data in PostgreSQL for analysis.
You can access the Airflow UI to monitor DAG execution and check logs.

## Airflow Dag Overview
The Airflow DAG for this project orchestrates the entire ETL process. The tasks are organized as follows:
- Task 1: Extract data from Spotify API.
- Task 2: Transform and clean the extracted data.
- Task 3: Load the transformed data into PostgreSQL.
- Task 4: End-of-DAG checks and logging.
The DAG is scheduled to run at a regular interval (e.g., daily) to keep the data up-to-date.
