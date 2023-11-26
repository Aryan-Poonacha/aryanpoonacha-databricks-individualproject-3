# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()


# vars used below
file_path = "dbfs:/databricks-datasets/iot/iot_devices.json"
table_name = "raw_iot_data"

# COMMAND ----------

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

# COMMAND ----------

# Drop the existing table if it exists
spark.sql("DROP TABLE IF EXISTS raw_iot_data")

# Read the JSON file
df = spark.read.schema(schema).json(file_path)

# Create a Delta table
df.write.format("delta").mode('overwrite').saveAsTable("raw_iot_data")

# Now you can create the view processed_iot_data
spark.sql("""
CREATE OR REPLACE VIEW processed_iot_data AS
SELECT device_id, device_name, ip, cca3, cn, latitude, longitude, temp, humidity, battery_level, c02_level, lcd, timestamp
FROM raw_iot_data
""")

# COMMAND ----------

#verify processed_iot_data: Load the table into a DataFrame
df = spark.table("processed_iot_data")

# Show the first few rows
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     device_name, 
# MAGIC     AVG(temp) as avg_temp, 
# MAGIC     AVG(humidity) as avg_humidity
# MAGIC FROM 
# MAGIC     processed_iot_data
# MAGIC GROUP BY 
# MAGIC     device_name
# MAGIC
