# Databricks Data Pipeline

In this project we create a data pipeline with Databricks. This pipeline integrates data sources and destinations, loading, processing and querying the data in the pipeline effectively.

## Dataset

In this project, we use a dataset included in the Databricks documentation. This dataset is IoT data of various devices and information on their runtime parameters.

## Guide

This project is guided by the official [Databricks Data Pipeline documentation](https://docs.databricks.com/en/getting-started/data-pipeline-get-started.html).

## Project Steps
The steps carried out to execute this project are:

### Cluster Creation
We create a databricks cluster as the basis for our data pipeline.

### Explore Data
We run commands from Databricks CLI and PySpark in a notebook to examine the source data. This is done in data_explore.

### Read And Process Data

In this step, the raw data is loaded into a table, and then processed to keep relevant information. This takes place in the first few cells of the data_processing notebook.

### Querying Data

In this step, we query the data and output the results to show stats on some devices.

### Databricks Job

We then create a Databricks job to automate the execution of both data_explore and data_processing notebooks, creating an automated pipeline.

![job](pics/job.png)