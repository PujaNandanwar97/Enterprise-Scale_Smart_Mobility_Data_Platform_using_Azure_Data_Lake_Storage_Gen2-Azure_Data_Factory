# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer

# COMMAND ----------

storage_account_name = "stnyctaxidata001"
storage_account_key = "<YOUR_STORAGE_ACCOUNT_KEY>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze Data

# COMMAND ----------

silver_df = spark.read.format("delta").load(
    "abfss://bronze@stnyctaxidata001.dfs.core.windows.net/nyc_taxi_bronze"
)

# COMMAND ----------

silver_df.count()

# COMMAND ----------

display(silver_df.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Cleaning

# COMMAND ----------

# MAGIC %md
# MAGIC check Null Values

# COMMAND ----------

from pyspark.sql.functions import col, isnull,sum

silver_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in silver_df.columns
]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Null Records

# COMMAND ----------

silver_df = silver_df.na.drop()

# COMMAND ----------

silver_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Type Conversion

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

silver_df = silver_df.withColumn(
    "tpep_pickup_datetime",
    to_timestamp("tpep_pickup_datetime")
)

silver_df = silver_df.withColumn(
    "tpep_dropoff_datetime",
    to_timestamp("tpep_dropoff_datetime")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Safe Numeric Conversion

# COMMAND ----------

from pyspark.sql.functions import col

silver_df = silver_df \
    .withColumn("passenger_count", col("passenger_count").cast("double")) \
    .withColumn("trip_distance", col("trip_distance").cast("double")) \
    .withColumn("fare_amount", col("fare_amount").cast("double")) \
    .withColumn("extra", col("extra").cast("double")) \
    .withColumn("mta_tax", col("mta_tax").cast("double")) \
    .withColumn("tip_amount", col("tip_amount").cast("double")) \
    .withColumn("tolls_amount", col("tolls_amount").cast("double")) \
    .withColumn("improvement_surcharge", col("improvement_surcharge").cast("double")) \
    .withColumn("total_amount", col("total_amount").cast("double"))

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

display(silver_df.limit(10))

# COMMAND ----------

silver_df.count()

# COMMAND ----------

display(
    dbutils.fs.rm(
        "abfss://silver@stnyctaxidata001.dfs.core.windows.net/nyc_taxi_silver", True
    )
)

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://silver@stnyctaxidata001.dfs.core.windows.net/"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Trip Duration

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp

silver_df = silver_df.withColumn(
    "trip_duration_minutes",
    (
    unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime") 
) / 60
)

# COMMAND ----------

display(
    silver_df.select("trip_duration_minutes").limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Remove Invaild Validation - Trip Distance
# MAGIC
# MAGIC Business Rule Validation - Trip Distance
# MAGIC

# COMMAND ----------

silver_df = silver_df.filter(
    silver_df.trip_distance > 0
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Invaild Passenger Count
# MAGIC Business Rule Validation - Passenger Count

# COMMAND ----------

silver_df = silver_df.filter(
    silver_df.passenger_count > 0
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Invaild Trip Duration

# COMMAND ----------

silver_df = silver_df.filter(
    silver_df.trip_duration_minutes > 0
)

# COMMAND ----------

silver_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Clean Data to Silver Layer

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://silver@stnyctaxidata001.dfs.core.windows.net/nyc_taxi_silver"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://silver@stnyctaxidata001.dfs.core.windows.net/nyc_taxi_silver"
    )
)