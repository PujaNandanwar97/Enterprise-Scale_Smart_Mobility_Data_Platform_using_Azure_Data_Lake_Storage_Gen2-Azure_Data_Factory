# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer

# COMMAND ----------

storage_account_name = "stnyctaxidata001"
storage_account_key = "<YOUR_STORAGE_ACCOUNT_KEY>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://raw@stnyctaxidata001.dfs.core.windows.net/"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read NYC Taxi Raw Data

# COMMAND ----------

raw_path = "abfss://raw@stnyctaxidata001.dfs.core.windows.net/*.csv"

bronze_df = spark.read \
    .option("header","true") \
    .option("inferSchema","true") \
    .csv(raw_path)       


# COMMAND ----------

# MAGIC %md
# MAGIC ### Record Count

# COMMAND ----------

bronze_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Check

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Preview

# COMMAND ----------

display(bronze_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC # Add Metadata Columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
bronze_df = (
    spark.read.format("delta")
    .load("abfss://bronze@stnyctaxidata001.dfs.core.windows.net/nyc_taxi_bronze")
.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### check New Columns

# COMMAND ----------

bronze_df.columns

# COMMAND ----------

# DBTITLE 1,Cell 14
display(
    bronze_df.select(
        "pickup_year",
        "pickup_month",
        "ingestion_timestamp"
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Partition Columns

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
bronze_df = bronze_df.withColumn(
    "tpep_pickup_datetime",
    to_timestamp("tpep_pickup_datetime")
)

# COMMAND ----------

from pyspark.sql.functions import year, month

bronze_df = bronze_df.withColumn(
    "pickup_year",
    year("tpep_pickup_datetime")
).withColumn(
    "pickup_month", 
    month("tpep_pickup_datetime")
)

# COMMAND ----------

display(bronze_df.select(
    "pickup_year",
    "pickup_month"
).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC # Store Bronze Data into Delta Lake
# MAGIC Store raw NYC Taxi trip data into Delta Lake Bronze storage with partitioning for scalable processing

# COMMAND ----------

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("pickup_year", "pickup_month") \
    .save("abfss://bronze@stnyctaxidata001.dfs.core.windows.net/nyc_taxi_bronze")          

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Bronze Data

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://bronze@stnyctaxidata001.dfs.core.windows.net/nyc_taxi_bronze"
    )
)

# COMMAND ----------

bronze_delta_df = spark.read.format("delta").load(bronze_path)

# COMMAND ----------

bronze_delta_df.count()

# COMMAND ----------

display(
    bronze_delta_df.select(
        "pickup_year",
        "pickup_month"
    ).limit(20)
)