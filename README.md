# 🎬 Movies Dataset — Exploratory Data Analysis (EDA)

> Built on **Databricks** using **Apache Spark (PySpark)** and **Spark SQL**

---

## 📌 Overview

This project performs an end-to-end Exploratory Data Analysis on a Movies dataset stored in a Databricks Unity Catalog table (`workspace.default.movies`). The notebook explores movie performance across industries, languages, studios, ratings, revenue, profit, and ROI — entirely within the Databricks environment using both PySpark DataFrame API and `%sql` magic commands.

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| **Databricks** | Cloud-based notebook environment |
| **Apache Spark (PySpark)** | Distributed data processing |
| **Spark SQL** | SQL queries on Delta tables |
| **Unity Catalog** | Data storage (`workspace.default.movies`) |

---

## 📂 Dataset

- **Source Table:** `workspace.default.movies`
- **Cleaned Table:** `workspace.default.movies_clean` (created during the notebook)
- **Key Columns:** `title`, `industry`, `studio`, `language`, `release_year`, `imdb_rating`, `budget`, `revenue`, `currency`

---

## 📋 What's Covered

### 1. 🔍 Data Loading & Inspection
- Loaded dataset using `spark.table()`
- Displayed first rows with `df.show()` and `df.display()`
- Inspected schema using `df.printSchema()`
- Counted total rows and listed all column names
- Generated descriptive statistics with `df.describe()` and `df.summary()` (includes percentiles)

### 2. 🧹 Data Cleaning & Transformation
- Identified that `imdb_rating` was stored as a `String` type
- Cast `imdb_rating` to `Double` using `try_cast` via `expr()` to safely handle invalid values
- Created a cleaned Delta table `workspace.default.movies_clean` with corrected data types using `CREATE OR REPLACE TABLE`

### 3. 🔎 Column Filtering
- Selected specific columns (`title`, `imdb_rating`, `industry`) using `df.select()`
- Used `truncate=False` in `show()` to display full movie titles without truncation

### 4. 📊 Exploratory Data Analysis (SQL)

#### Industry & Language
- Count of movies per industry
- Movies count per language
- Total revenue generated per industry
- Industry with the highest total revenue

#### Studio Analysis
- Listed all movies by **Marvel Studios**
- Ranked studios by total revenue

#### Revenue & Financial Analysis
- Revenue and movie count grouped by currency
- Calculated **profit** (`revenue - budget`) for each movie
- Top 5 most profitable movies
- Movies with a **loss** (`profit ≤ 0`)
- Average profit per industry
- Total profit by each currency
- Checked for movies with missing `budget` or `revenue` values

#### IMDb Rating Analysis
- Top 5 movies with the highest IMDb rating
- Average IMDb rating per industry
- Movies with IMDb rating greater than 8
- Year-wise average IMDb rating trend

#### Rating Classification (CASE WHEN)
Classified movies into performance tiers:

| Rating Score | Category |
|---|---|
| ≥ 8 | **HIT** |
| 5 – 8 | **AVERAGE** |
| < 5 | **FLOP** |

- Counted the number of movies in each category

#### Window Functions
- Used `DENSE_RANK()` to rank movies by **revenue** (overall)
- Used `DENSE_RANK()` partitioned by **industry** to rank movies by `imdb_rating` within each industry

#### ROI (Return on Investment)
- Calculated ROI as `(revenue / budget) * 100`
- Identified the movie with the **highest ROI**

---

## 🚀 How to Run

1. Import the `.ipynb` notebook into your **Databricks workspace**
2. Attach it to a running cluster
3. Ensure the table `workspace.default.movies` exists in your Unity Catalog
4. Run all cells sequentially — the notebook will auto-create `workspace.default.movies_clean`

---

## 📁 File Structure

```
├── movies_dataset_eda.ipynb   # Main Databricks notebook
└── README.md                  # Project documentation
```
