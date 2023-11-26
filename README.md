# IDS 706 - Individual Project 3

In this project, we expand upon the pipeline created in miniproject 11 to create complex data ETL and finally complex visualizations that are able to communicate key data insights in an automated data pipeline with an automatic trigger.

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

### Extract Data

The data_extract notebook contains the script to extract the appropriate dataset from the databricks example datasets.

### Transform And Load Data

This is a simple SQL query that creates a processed table that contains appropriate transformations from the extracted data.

### Querying And Visualizing Data

In this step, we query the data and output the results to show stats on some devices. We then create an appropriate visualization using a complex query.

![query](pics/viz.png)

### Databricks Job

We then create a Databricks job to automate the execution of all three steps - data_extract, data_transformload and data_queryandvisualize, creating an automated pipeline.

![job](pics/job.PNG)
![run](pics/run.PNG)

### Scheduled Run Trigger

We make a specific trigger in databricks so that the data pipeline auto runs at a fixed interval, ensuring that the newest data is used for each pipeline run.

![run](pics/trigger.PNG)