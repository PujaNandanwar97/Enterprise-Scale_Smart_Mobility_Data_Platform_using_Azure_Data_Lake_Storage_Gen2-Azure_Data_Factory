# NYC Taxi Data Engineering Project

# Project Overview

This project demonstrates the design and implementation of an end-to-end Azure Data Engineering solution using the NYC Taxi dataset.

The solution follows the Medallion Architecture (Bronze → Silver → Gold) to transform raw taxi trip data into business-ready analytical datasets. The project leverages Azure cloud services, Delta Lake, PySpark, and Databricks SQL to build a scalable and production-style data pipeline.

---

# Business Objective

The objective of this project is to transform raw taxi trip data into meaningful business insights that help answer questions such as:

- What are the monthly revenue trends?
- Which vendors generate the highest revenue?
- What are the busiest pickup hours?
- How do customers prefer to pay?
- How does trip distance impact revenue?
- What are the top revenue-generating days?

---

# Solution Architecture

Raw CSV Files

⬇

Azure Data Lake Storage Gen2 (Raw Layer)

⬇

Bronze Layer (Raw Delta Data)

⬇

Silver Layer (Cleaned & Validated Data)

⬇

Gold Layer (Business Aggregations)

⬇

Databricks SQL Analytics

⬇

Dashboard & Business Reporting

---

# Technologies Used

Cloud Services

- Microsoft Azure
- Azure Data Lake Storage Gen2
- Azure Databricks

Data Engineering

- PySpark
- Delta Lake
- Databricks SQL
- Medallion Architecture

Analytics

- Window Functions
- Business KPI Development
- Dashboard Reporting

---

# Bronze Layer

Purpose

Store raw data in Delta format while preserving the original dataset.

Activities Performed

- Ingested NYC Taxi CSV files
- Added ingestion metadata
- Created Delta tables
- Stored partitioned data by year and month
- Implemented raw data persistence

Key Outcome

Created a reliable and auditable raw data layer for downstream processing.

---

# Silver Layer

Purpose

Clean, standardize, and validate data for analytics.

Activities Performed

- Data quality checks
- Null value validation
- Datatype conversions
- Timestamp standardization
- Trip duration calculation
- Business rule validation

Key Outcome

Created trusted and analytics-ready datasets.

---

# Gold Layer

Purpose

Create business-ready datasets optimized for reporting and analytics.

Analytical Data Models Created

Revenue Analysis

- Monthly Revenue
- Revenue Trends
- Top Revenue Days

Vendor Analysis

- Vendor Performance
- Vendor Ranking

Customer Analysis

- Passenger Analysis
- Payment Analysis

Operational Analysis

- Peak Hour Analysis
- Trip Distance Analysis
- Daily Trip Trends

Window Functions Used

- DENSE_RANK()
- LAG()

Key Outcome

Delivered business-ready KPIs and analytical datasets.

---

# SQL Analytics

Databricks SQL was used to generate business insights from Gold Layer datasets.

SQL Use Cases

- Monthly Revenue Analysis
- Top Revenue Months
- Vendor Performance Analysis
- Peak Hour Analysis
- Payment Revenue Analysis
- Revenue Trend Analysis

SQL Concepts Used

- SELECT
- WHERE
- ORDER BY
- LIMIT
- Aggregations
- Window Functions

---

# Dashboard Analytics

Interactive visualizations were created for:

- Revenue Trends
- Vendor Performance
- Payment Analysis
- Peak Hour Demand
- Trip Distance Distribution

These dashboards provide quick insights for business stakeholders and decision-makers.

---

 # Key Skills Demonstrated

- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark Transformations
- Delta Lake
- Data Quality Validation
- SQL Analytics
- Window Functions
- Dashboard Development
- Medallion Architecture
- End-to-End Data Engineering

---

# Project Structure

01_data_ingestion

02_bronze_layer

03_silver_layer

04_gold_layer

05_sql_analytics

06_project_documentation

07_dashboard_queries

---

# Project Highlights

✔ End-to-End Azure Data Engineering Pipeline

✔ Medallion Architecture Implementation

✔ Delta Lake Integration

✔ Business KPI Development

✔ Databricks SQL Analytics

✔ Dashboard Ready Data Models

✔ Production-Style Data Engineering Workflow

---

# Author

Pooja Nandanwar

Aspiring Data Engineer | Azure Data Enthusiast
