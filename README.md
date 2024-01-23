# nyc-taxi-trips

## Overview

This project focuses on data engineering and ETL processes using Python, Apache Airflow, and Google Cloud Platform (GCP). The project aims to build a data pipeline that extracts data from NYC Taxi Trips public dataset, transforms it, and loads it into a data warehouse. The project also includes a CI/CD pipeline to automate the process of transferring the code to the Cloud Composer environment.

## Project Setup

- Created a GCP account using personal credentials.
- Activated Google Cloud Storage and BigQuery services on the GCP account.
- If the project wants to be replicated, If you want to replicate the project, you have to enter your own service account as an airflow variable. Because I do not upload my own service account information to github.

## ETL Workflow

### 1. Airflow and Cloud Composer
- Chose Apache Airflow for ETL orchestration.
- Utilized GCP's Cloud Composer, Google's managed Apache Airflow service, for quick deployment and management.
- A custom Airflow instance on a virtual machine could be a simpler alternative, but I chose Cloud Composer for setting up and managing Airflow quickly due to time constraints.
### 2. SQL Query Preparation for GCS
- Crafted an SQL query to extract data from the public dataset `tlc_green_trips_2014`.
- Focused on transferring the first week of May's data to Cloud Storage.
- Ensured the Cloud Storage structure met the desired format (year/month/day).
- The latest version of the SQL script with which I transferred the `tlc_green_trips_2014` table to cloud storage can be found under the 'sqls' folder.

### 3. Airflow DAGS
- In this phase, I set the 'date_to_load' Airflow variable to specify the date for loading data and performing operations. I added a bit of my interpretation here since the scenario involved running ETL processes for a given date. I believe more optimal solutions can be devised for a longer time range.
- In the `load_gcs_to_bigquery` DAG, I implemented the process of writing the data ready in GCS to BigQuery for the given date.
- To create a hexagon ID in the table prepared within the project for the specified date, I utilized the `taxi_zone_geom` table. I prepared an SQL script to obtain pickup and dropoff latitude and longitude values.
- I coded the processes of creating `daypart` from the date, creating a hexagon from the location, and generating popular pickup, dropoff, and routes hexagons in Python. While doing this job, I realized that hexagon ids could not be created for very few locations (because they did not have ids in the `taxi_zone_geom` table) and I removed these records.
- To ensure better coordination between the two existing DAGs and to orchestrate the ETL processes effectively, I created another DAG and linked it to the existing ones.

### 4. CI/CD Pipeline
- Created a service account for GitHub Actions to access the Cloud Storage environment.
- Created a Cloud Storage bucket to store the DAGs and plugins.
- Created a CI/CD pipeline using GitHub Actions to automate the process of transferring the code to the Cloud Composer environment.

### Workflow Costs
- The daily cost of the workflow is approximately ₺350. Cloud Composer is responsible for 95% of the costs. The cost of the workflow can be reduced by using a custom Airflow instance on a virtual machine instead of Cloud Composer. However, I chose Cloud Composer for setting up and managing Airflow quickly due to time constraints. 


## Ideas for High-Level Architectural Design

It is not possible to work with data in a manually determined time period in this way in today's world. The only reason I solved it this way was that I aimed to get maximum work done in a very limited time. Even though I haven't had time to do it, I would like to make some comments about the last part of the case study.

We can consider an event-driven architecture where data triggers events, and the pipeline reacts accordingly and services like Cloud Pub/Sub for handling asynchronous data streams. We can implement a data processing service that can handle and process the incoming data. Apache Beam on Google Cloud Dataflow could be suitable for scalable and parallelized data processing. The trade-off of such a system may be that the cost might increase due to the usage of additional services like Cloud Pub/Sub and Dataflow. Handling data from an unknown source introduces challenges in terms of data quality and consistency. 

To sum up, The chosen approach should align with the specific requirements of the project in terms of cost, performance, and stability. Regular monitoring and optimization will be essential for maintaining a reliable and efficient data pipeline.

