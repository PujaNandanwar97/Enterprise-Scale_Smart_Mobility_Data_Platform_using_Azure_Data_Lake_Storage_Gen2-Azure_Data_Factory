# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Analytics

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

monthly_revenue_df.createOrReplaceTempView("monthly_revenue")

# COMMAND ----------

vendor_performance_df.createOrReplaceTempView("vendor_performance")

# COMMAND ----------

payment_analysis_df.createOrReplaceTempView("payment_analysis")

# COMMAND ----------

peak_hour_analysis_df.createOrReplaceTempView("peak_hour_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM monthly_revenue
# MAGIC ORDER BY pickup_year, pickup_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Revenue Month

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM monthly_revenue
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Vendor

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vendor_performance
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Peak Business Hour

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM peak_hour_analysis
# MAGIC ORDER BY total_trips DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Payment Type Revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT payment_type,
# MAGIC        total_revenue
# MAGIC FROM payment_analysis
# MAGIC ORDER BY total_revenue DESC;       

# COMMAND ----------

# MAGIC %md
# MAGIC ### Average Fare by Vendor

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT vendorID,
# MAGIC        avg_fare
# MAGIC FROM vendor_performance
# MAGIC ORDER BY avg_fare DESC;       

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Revenue Greater Than 50 Million

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM monthly_revenue
# MAGIC WHERE total_revenue > 50000000;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Peak Hour Revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_hour,
# MAGIC        total_revenue
# MAGIC FROM peak_hour_analysis
# MAGIC ORDER BY total_revenue DESC;       