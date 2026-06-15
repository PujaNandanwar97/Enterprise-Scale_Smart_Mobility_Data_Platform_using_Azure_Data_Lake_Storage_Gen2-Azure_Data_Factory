# Databricks notebook source
storage_account_name = "stnyctaxidata001"
storage_account_key = "<YOUR_STORAGE_ACCOUNT_KEY>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/"
    )
)

# COMMAND ----------

monthly_revenue_df = spark.read.format("delta").load(
    "abfss://gold@stnyctaxidata001.dfs.core.windows.net/monthly_revenue_gold"
)

# COMMAND ----------

vendor_performance_df = spark.read.format("delta").load(
    "abfss://gold@stnyctaxidata001.dfs.core.windows.net/vendor_performance_gold"
)

# COMMAND ----------

payment_analysis_df = spark.read.format("delta").load(
    "abfss://gold@stnyctaxidata001.dfs.core.windows.net/payment_analysis_gold"
)

# COMMAND ----------

peak_hour_analysis_df = spark.read.format("delta").load(
    "abfss://gold@stnyctaxidata001.dfs.core.windows.net/peak_hour_analysis_gold"
)

# COMMAND ----------

trip_distance_analysis_df = spark.read.format("delta").load(
    "abfss://gold@stnyctaxidata001.dfs.core.windows.net/trip_distance_analysis_gold"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Revenue Trend

# COMMAND ----------

display(monthly_revenue_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vendor Performance

# COMMAND ----------

display(vendor_performance_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Payment Analysis

# COMMAND ----------

display(payment_analysis_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Peak Hour Analysis

# COMMAND ----------

display(peak_hour_analysis_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip Distance Analysis

# COMMAND ----------

display(trip_distance_analysis_df)