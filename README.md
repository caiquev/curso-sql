# 🛒 Customer Analytics Feature Store (ETL)

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![SQL](https://img.shields.io/badge/SQL-SQLite-orange)
![Pandas](https://img.shields.io/badge/Pandas-ETL-green)
![Status](https://img.shields.io/badge/Status-Completed-success)

## 📌 Project Overview

This project implements an **ETL (Extract, Transform, Load)** pipeline designed to build a historical **Feature Store** for customer analytics.

The goal is to transform raw transactional data into structured behavioral features (RFM, Product Preferences, Engagement) that can be used to train Machine Learning models (such as Churn Prediction, LTV forecasting, or Propensity to Purchase).

### 🎯 Key Objectives
* **Prevent Data Leakage:** Implemented a "Time Machine" logic to calculate features based strictly on data available up to a specific reference date (`dtRef`).
* **Feature Engineering:** created complex metrics including Recency, Frequency, Monetary values (RFM), and time-based habits.
* **Backfilling:** Orchestrated a script to generate historical snapshots for multiple months to build a training dataset.

---

## 🛠️ Tech Stack & Skills Demonstrated

* **Python (Pandas, SQLAlchemy):** Orchestration of the ETL pipeline, database connection management, and batch processing of dates.
* **Advanced SQL:**
    * **CTEs (Common Table Expressions):** For modular and readable query design.
    * **Window Functions (`ROW_NUMBER`, `PARTITION BY`):** To identify top-ranking preferences (e.g., favorite product, preferred time of day).
    * **Date Manipulation (`julianday`, `strftime`):** To calculate recency and age metrics.
    * **Conditional Aggregation (`CASE WHEN`):** To pivot transactional data into columnar features (e.g., transactions in the last 7 vs 28 days).
* **Database:** SQLite (scalable logic applicable to BigQuery, Snowflake, or Redshift).

---

## ⚙️ Architecture & Logic

The pipeline follows a "Snapshot" strategy. For every defined reference date, the system looks back in time to calculate metrics.

### 1. The Transformation (`etl_projeto.sql`)
The SQL query is the core engine. It ingests a `{date}` parameter and performs the following:
* **Filtering:** `WHERE DtCriacao < '{date}'` ensures we only see past data.
* **Behavioral Windows:** Calculates metrics for specific lookback windows:
    * Last 7 Days
    * Last 14 Days
    * Last 28 Days
    * Last 56 Days
* **RFM Calculation:** Aggregates Points (Monetary), Counts (Frequency), and days since last interaction (Recency).
* **Preference Tagging:** Uses Window Functions to find the customer's "Favorite Product" and "Preferred Day of Week".

### 2. The Orchestration (`etl.py`)
The Python script acts as the controller:
1.  Connects to the database engine.
2.  Iterates through a list of critical dates (e.g., `2025-01-01`, `2025-02-01`, etc.).
3.  Injects the date into the SQL query dynamically.
4.  Appends the processed dataset to the `feature_store_cliente` table.

---

## 📊 Feature Dictionary

The resulting Feature Store contains a wide-table schema with the following categories:

| Category | Feature Examples | Description |
| :--- | :--- | :--- |
| **Reference** | `dtRef` | The snapshot date used for the calculation. |
| **Recency** | `DiasUltimaInteracao` | Number of days since the last transaction. |
| **Lifecycle** | `IdadeCliente` | Days since the customer joined. |
| **Frequency** | `QtTransacoes28`, `QtTransacoesVida` | Count of transactions in the last 28 days vs Lifetime. |
| **Monetary** | `SaldoPontos`, `QtdePontosPos28` | Current points balance and points earned recently. |
| **Engagement** | `engajamento28Vida` | Ratio of recent activity to lifetime activity. |
| **Habits** | `DtDia`, `PeriodoMaisTransacao28` | Preferred day of week and time of day (Morning, Night, etc). |
| **Preferences**| `ProdutoVida`, `ProdutoD28` | Most purchased product overall and recently. |

---

## 🚀 How to Run

1.  **Prerequisites:**
    * Ensure you have a `database.db` file with raw source tables (`transacoes`, `clientes`, `produtos`).
    * Install dependencies:
        ```bash
        pip install pandas sqlalchemy
        ```

2.  **Execute the ETL:**
    Run the python script to process the historical dates:
    ```bash
    python etl.py
    ```

3.  **Check Results:**
    Access the database and query the new table:
    ```sql
    SELECT * FROM feature_store_cliente LIMIT 10;
    ```

---

## 👤 Author

**Caíque Veras** *Data Scientist | SQL & Python*
