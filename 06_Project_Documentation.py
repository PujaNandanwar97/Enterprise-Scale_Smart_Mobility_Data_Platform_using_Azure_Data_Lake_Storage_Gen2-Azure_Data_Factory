# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi Data Engineering Project

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technologies Used
# MAGIC
# MAGIC - Azure Data Lake Storage Gen2
# MAGIC - Azure Databricks
# MAGIC - PySpark
# MAGIC - Delta Lake
# MAGIC - SQL
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC Raw Layer → Bronze Layer → Silver Layer → Gold Layer → SQL Analytics
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Problem
# MAGIC  Analyze NYC Taxi trip data to identify revenue trends, vendor performance, passenger behavior, payment patterns, and peak demand hours

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Bronze Layer
# MAGIC
# MAGIC - Ingested raw CSV files
# MAGIC - Added metadata columns
# MAGIC - Stored data in Delta format
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC
# MAGIC - Data cleaning
# MAGIC - Datatype conversion
# MAGIC - Trip duration calculation
# MAGIC - Business validation rules
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer
# MAGIC
# MAGIC Created analytical tables:
# MAGIC
# MAGIC - Monthly Revenue Analysis
# MAGIC - Vendor Performance Analysis
# MAGIC - Passenger Analysis
# MAGIC - Payment Type Analysis
# MAGIC - Peak Hour Analysis
# MAGIC - Trip Distance Analysis
# MAGIC - Top Revenue Days Analysis
# MAGIC - Revenue Trend Analysis
# MAGIC - Vendor Monthly Performance
# MAGIC - Vendor Ranking
# MAGIC - Passenger Trend Analysis
# MAGIC - Daily Trip Trend Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Analytics
# MAGIC
# MAGIC Created business queries using Databricks SQL:
# MAGIC
# MAGIC - Revenue Analysis
# MAGIC - Vendor Analysis
# MAGIC - Payment Analysis
# MAGIC - Peak Hour Analysis
# MAGIC