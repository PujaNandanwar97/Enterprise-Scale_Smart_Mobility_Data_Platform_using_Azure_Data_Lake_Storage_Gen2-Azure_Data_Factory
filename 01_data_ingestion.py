# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion
# MAGIC
# MAGIC Read raw NYC Taxi data from Azure Data Lake Storage Gen2 into Azure Databricks using PySpark for further processing.  

# COMMAND ----------

storage_account_name = "stnyctaxidata001"
storage_account_key = "<YOUR_STORAGE_ACCOUNT_KEY>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Raw Files in ADLS

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://raw@stnyctaxidata001.dfs.core.windows.net/"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read January 2015 Taxi Dataset

# COMMAND ----------

# DBTITLE 1,Cell 6
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("abfss://raw@stnyctaxidata001.dfs.core.windows.net/yellow_tripdata_2015-01.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Validation

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preview Data

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Count Records

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Profiling
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Columns

# COMMAND ----------

print(f"Total Columns: {len(df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Records

# COMMAND ----------

print(f"Total Records: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Null Values

# COMMAND ----------

from pyspark.sql.functions import col, sum

null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataset Summary Statistics

# COMMAND ----------

display(df.describe())