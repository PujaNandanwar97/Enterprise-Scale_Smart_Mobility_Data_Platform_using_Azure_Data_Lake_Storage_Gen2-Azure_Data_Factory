# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Silver Data

# COMMAND ----------

storage_account_name = "stnyctaxidata001"
storage_account_key = "<YOUR_STORAGE_ACCOUNT_KEY>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# COMMAND ----------

gold_df = spark.read.format("delta").load(
    "abfss://silver@stnyctaxidata001.dfs.core.windows.net/nyc_taxi_silver"
)

# COMMAND ----------

gold_df.count()

# COMMAND ----------

display(gold_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Revenue Analysis

# COMMAND ----------

from pyspark.sql.functions import sum, avg, count

monthly_revenue_df = gold_df.groupBy(
    "pickup_year",
    "pickup_month"
).agg(
    count("*").alias("total_trips"),
    sum("total_amount").alias("total_revenue"),
    avg("fare_amount").alias("avg_fare")
)

# COMMAND ----------

display(monthly_revenue_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Monthly Revenue Gold Table 

# COMMAND ----------

monthly_revenue_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@stnyctaxidata001.dfs.core.windows.net/monthly_revenue_gold")

# COMMAND ----------

display(
    dbutils.fs.ls("abfss://gold@stnyctaxidata001.dfs.core.windows.net/monthly_revenue_gold"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vendor Performance Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Vendor Performance Table 

# COMMAND ----------

from pyspark.sql.functions import sum, avg, count

vendor_performance_df = gold_df.groupBy(
    "VendorID",
).agg(
    count("*").alias("total_trips"),
    sum("total_amount").alias("total_revenue"),
    avg("trip_distance").alias("avg_trip_distance"),
    avg("fare_amount").alias("avg_fare")
)

# COMMAND ----------

display(vendor_performance_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Vendor Performance Gold Table

# COMMAND ----------

vendor_performance_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/vendor_performance_gold"
    )        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification

# COMMAND ----------

display(
    dbutils.fs.ls("abfss://gold@stnyctaxidata001.dfs.core.windows.net/vendor_performance_gold")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passenger Analysis

# COMMAND ----------

from pyspark.sql.functions import sum, avg, count

passenger_analysis_df = gold_df.groupBy(
    "passenger_count"
).agg(
    count("*").alias("total_trips"),
    sum("total_amount").alias("total_revenue"),
    avg("fare_amount").alias("avg_fare")
)

# COMMAND ----------

display(passenger_analysis_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Passenger Analysis Gold Table

# COMMAND ----------

passenger_analysis_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/passenger_analysis_gold"
    )        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(
    dbutils.fs.ls("abfss://gold@stnyctaxidata001.dfs.core.windows.net/passenger_analysis_gold")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Payment Type Analysis

# COMMAND ----------

from pyspark.sql.functions import sum, avg, count

payment_analysis_df = gold_df.groupBy(
    "payment_type"
).agg(
    count("*").alias("total_trips"),
    sum("total_amount").alias("total_revenue"),
    avg("tip_amount").alias("avg_tip")
)

# COMMAND ----------

display(payment_analysis_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Payment Type Analysis Gold Table

# COMMAND ----------

payment_analysis_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/payment_analysis_gold"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(
    dbutils.fs.ls("abfss://gold@stnyctaxidata001.dfs.core.windows.net/payment_analysis_gold")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Peak Hour Analysis

# COMMAND ----------

from pyspark.sql.functions import hour

peak_hour_df = gold_df.withColumn(
    "pickup_hour",
    hour("tpep_pickup_datetime")
)


# COMMAND ----------

from pyspark.sql.functions import count, sum, avg

peak_hour_analysis_df = peak_hour_df.groupBy(
    "pickup_hour"
).agg(
    count("*").alias("total_trips"),
    sum("total_amount").alias("total_revenue"),
    avg("fare_amount").alias("avg_fare")
).orderBy("pickup_hour")

# COMMAND ----------

display(peak_hour_analysis_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Peak Hour Gold Table

# COMMAND ----------

peak_hour_analysis_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/peak_hour_analysis_gold"
    )        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/peak_hour_analysis_gold"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip Distance Analysis

# COMMAND ----------

from pyspark.sql.functions import when,col

trip_distance_df = gold_df.withColumn(
    "trip_category",
    when(col("trip_distance") < 2 , "short Trip")
    .when(
        (col("trip_distance") >= 2) &
        (col("trip_distance") < 10),
        "Medium Trip"
    )
    .otherwise("Long Trip")
)

# COMMAND ----------

from pyspark.sql.functions import count, sum, avg

trip_distance_analysis_df = trip_distance_df.groupBy(
    "trip_category"
).agg(
    count("*").alias("total_Trip"),
    sum("tip_amount").alias("total_revenue"),
    avg("trip_distance").alias("avg_distance"),
    avg("fare_amount").alias("avg_fare")
)

# COMMAND ----------

display(trip_distance_analysis_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Trip Distance Gold Table

# COMMAND ----------

trip_distance_analysis_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/trip_distance_analysis_gold"
    )        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/trip_distance_analysis_gold")
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Revenue Days Analysis Using Window Functions

# COMMAND ----------

from pyspark.sql.functions import to_date, sum

daily_revenue_df = gold_df.groupBy(
    to_date("tpep_pickup_datetime").alias("trip_date")
).agg(
    sum("total_amount").alias("daily_revenue")
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc

window_spec = Window.orderBy(desc("daily_revenue"))
top_revenue_days_df = daily_revenue_df.withColumn(
    "revenue_rank",
    dense_rank().over(window_spec)
)

# COMMAND ----------

display(top_revenue_days_df.orderBy("revenue_rank"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 Revenue Days

# COMMAND ----------

top_10_revenue_days_df = top_revenue_days_df.filter(
    top_revenue_days_df.revenue_rank <= 10
)
display("top_10_revenue_days_df")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Top Revenue Days Gold Tables

# COMMAND ----------

top_revenue_days_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/top_revenue_days_gold"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/top_revenue_days_gold"

    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue Trend Table

# COMMAND ----------

from pyspark.sql.functions import sum
monthly_revenue_trend_df =gold_df.groupBy(
    "pickup_year",
    "pickup_month"
).agg(
    sum("total_amount").alias("monthly_revenue" )
)


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag

window_spec = Window.orderBy(
    "pickup_year",
    "pickup_month"
)

monthly_revenue_trend_df = monthly_revenue_trend_df.withColumn(
    "previous_month_revenue",
    lag("monthly_revenue").over(window_spec)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Revenue Growth %

# COMMAND ----------

from pyspark.sql.functions import round

monthly_revenue_trend_df = monthly_revenue_trend_df.withColumn(
    "revenue_growth_percent",
    round(
        (
            (
                monthly_revenue_trend_df.monthly_revenue
                - monthly_revenue_trend_df.previous_month_revenue
            ) 
            / monthly_revenue_trend_df.previous_month_revenue
        ) * 100,
        2
     )
    )

# COMMAND ----------

display(
    monthly_revenue_trend_df.orderBy(
        "pickup_year",
        "pickup_month"

    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Revenue Trend Analysis Gold Table

# COMMAND ----------

monthly_revenue_trend_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save (
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/revenue_trend_gold"
    )        

# COMMAND ----------

# MAGIC %md
# MAGIC ### verify

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/revenue_trend_gold")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vendor Monthly Performance

# COMMAND ----------

from pyspark.sql.functions import count,sum,avg

vendor_monthly_df = gold_df.groupBy(
    "VendorID",
    "pickup_year",
    "pickup_month"
).agg(
    count("*").alias("total_trip"),
    sum("total_amount").alias("total_revenue"),
    sum("fare_amount").alias("total_fare")
)


# COMMAND ----------

display(vendor_monthly_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Vendor Monthly Performance Gold Table

# COMMAND ----------

vendor_monthly_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save (
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/vendor_monthly_performance_gold"
    )        

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/vendor_monthly_performance_gold"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Revenue Vendor Ranking 

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc

vendor_rank_df = gold_df.groupBy(
    "VendorID"
).agg(
    sum("total_amount").alias("total_revenue")
)
window_spec = Window.orderBy(
    desc("total_revenue")
)
vendor_rank_df = vendor_rank_df.withColumn(
    "Vendor_rank",
    dense_rank().over(window_spec)
)

# COMMAND ----------

display(vendor_rank_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Top Revenue Vendor Ranking Gold Table 

# COMMAND ----------

vendor_rank_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save (
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/vendor_rank_gold"
    )

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/vendor_rank_gold")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Passenger Trend

# COMMAND ----------

from pyspark.sql.functions import count

passenger_trend_df = gold_df.groupBy(
    "pickup_year",
    "pickup_month"
).agg(
    count("*").alias("total_passenger")
)

# COMMAND ----------

display(passenger_trend_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Monthly Passenger trend Analysis Gold Table

# COMMAND ----------

passenger_trend_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save (
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/passenger_trend_gold")

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/passenger_trend_gold")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Trip Trend Analysis

# COMMAND ----------

from pyspark.sql.functions import count, to_date

daily_trip_trend_df = gold_df.groupBy(
    to_date("tpep_pickup_datetime").alias("trip_date")
).agg(
    count("*").alias("total_trips")
)

# COMMAND ----------

display(daily_trip_trend_df.orderBy("trip_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Daily Trip Trend Analysis Gold Table

# COMMAND ----------

daily_trip_trend_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save (
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/daily_trip_trend_gold")

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://gold@stnyctaxidata001.dfs.core.windows.net/daily_trip_trend_gold")
)

# COMMAND ----------

# MAGIC %md
# MAGIC