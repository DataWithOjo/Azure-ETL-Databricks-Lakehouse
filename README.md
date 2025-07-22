# Azure ETL Lakehouse Project with Medallion Architecture

This project implements a full end-to-end data engineering pipeline using **Azure Databricks**, **Apache Spark**, and **Medallion Architecture (Bronze, Silver, Gold)**. It ingests raw data stored in Azure Data Lake Storage (ADLS Gen2), processes it incrementally using Spark, and delivers cleaned, analytics-ready datasets in the Gold layer.

## 🗂 Project Structure

- **Source Format**: Parquet files
- **Source Location**: Azure Data Lake Storage (ADLS) `source` container
- **Files Used**:
  - `orders.parquet`
  - `products.parquet`
  - `customers.parquet`
  - `region.parquet`

---

## 🏗 Architecture Overview

### 🔸 Bronze Layer – Raw Ingest
- Incrementally reads raw Parquet files from ADLS source container.
- Uses **Databricks Jobs** and **AutoLoader** to ensure efficient incremental loads.
- Writes data as **Parquet** to the `bronze` layer in ADLS.

### ⚙️ Silver Layer – Cleaned and Transformed
- Reads data from the Bronze layer.
- Applies data transformations, column renaming, casting, and filtering.
- Performs schema enforcement and validation.
- Writes outputs as **Delta Tables** to the `silver` layer in ADLS.
- Registers all Delta tables to **Unity Catalog** under the Databricks workspace.

### 🥇 Gold Layer – Business Logic
- Creates **Dimension** and **Fact** tables using the Silver data.
- Performs necessary joins, aggregations, and surrogate key generation.
- Applies **SCD Type 2** logic where applicable.
- Writes to the `gold` layer as **streaming tables** optimized for BI & analytics.

---

## 🔎 Data Quality & Governance

- Applied **Great Expectations** and **DLT expectations** for data validation.
- Rules enforced:
  - Null checks for key fields (e.g., `product_id`, `customer_id`)
  - Positive numeric constraints (e.g., price > 0)
- Ensured referential integrity before loading into Gold Layer.

---

## ⚙️ Databricks Setup

- Workspace, clusters, and storage were named using **Azure naming conventions**.
- Unity Catalog enabled for structured access and lineage tracking.
- Databricks Jobs and DLT Pipelines were used to orchestrate ETL tasks.

---

## 🧠 Technologies Used

- **Azure Databricks (Spark, Delta Live Tables)**
- **Apache Spark (PySpark)**
- **Azure Data Lake Storage Gen2**
- **Delta Lake**
- **Unity Catalog**
- **Great Expectations**
- **Git & GitHub for version control**

---

## 📊 Outcome

By following the Medallion Architecture:
- Enabled **scalable, incremental ingestion** of raw data.
- Delivered **trustworthy and structured data** for analytics.
- Provided a **clear data lineage** from raw source to curated insights.
- Built a reusable, production-grade ETL framework for enterprise data.

---
