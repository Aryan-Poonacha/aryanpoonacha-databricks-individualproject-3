# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("MyApp").getOrCreate()

# vars used below
file_path = "dbfs:/databricks-datasets/iot/iot_devices.json"
table_name = "raw_iot_data"

# Define the schema
schema = StructType([
    StructField("device_id", IntegerType(), True),
    StructField("device_name", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("cca2", StringType(), True),
    StructField("cca3", StringType(), True),
    StructField("cn", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("scale", StringType(), True),
    StructField("temp", IntegerType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("battery_level", IntegerType(), True),
    StructField("c02_level", IntegerType(), True),
    StructField("lcd", StringType(), True),
    StructField("timestamp", LongType(), True)
])

try:
    # Drop the existing table if it exists
    spark.sql("DROP TABLE IF EXISTS raw_iot_data")

    # Read the JSON file
    df = spark.read.schema(schema).json(file_path)

    # Validate data
    assert df.count() > 0, "Data file is empty"
    assert len(df.columns) == len(schema.fields), "Data schema mismatch"

    # Create a Delta table
    df.write.format("delta").mode('overwrite').saveAsTable("raw_iot_data")

    print("Extraction process completed successfully.")
except AnalysisException as e:
    print(f"An error occurred during the extraction process: {str(e)}")
except AssertionError as e:
    print(f"Data validation failed: {str(e)}")

