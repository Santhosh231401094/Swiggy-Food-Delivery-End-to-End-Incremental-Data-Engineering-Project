# Swiggy Food Delivery: End-to-End Incremental Data Engineering Project

## 📌 Project Overview

This project builds a robust end-to-end data pipeline in Microsoft Fabric that processes Swiggy delivery data from raw ingestion to an interactive Power BI dashboard.

Instead of a simple one-time data load, this solution implements a Medallion Architecture (Bronze → Silver → Gold) along with an Incremental Loading pattern to ensure only new records are processed, improving performance, scalability, and cost efficiency.

## 🏗️ Data Architecture

The project follows the Medallion Architecture.
<img width="1291" height="641" alt="swiggy1 drawio (1)" src="https://github.com/user-attachments/assets/e59e74e5-7672-40ee-89cb-089c40783b83" />

## 🥉 Bronze Layer – Raw Data (Lakehouse)

Ingested raw CSV data from GitHub into a Lakehouse staging table (swiggy).

Data is stored in Delta format for efficient processing.

The table acts as the historical raw data source.

## 🥈 Silver Layer – Data Cleaning & Transformation

Data cleaning and transformations were implemented using PySpark notebooks.

Key Transformations

**1. Missing Value Handling**

Filled categorical columns with "NA"
```
df=df.fillna({
    "restaurant":"NA",
    "cuisine":"NA",
    "location":"NA",
    "city":"NA",
    "book_table":"NA",
    "online_order":"NA"
})
```

Filled numeric columns with 0
```
df=df.fillna({
    "ordered_qty":0,
    "distance_km":0,
    "total_amount":0
})
```

**2. String Standardization**

Fixed inconsistent city names such as "Bnegaluru" → "Bengaluru"

Applied initcap(), trim(), and regexp_replace()
```
df = df.withColumn("city", trim(col("city")))
df = df.withColumn("city", initcap(col("city")))
df = df.withColumn("city", regexp_replace(col("city"), "Bnegaluru", "Bengaluru"))
```

**3. Conditional Logic**

If online_order = 'No'

Set:

```
delivery_time = NULL
distance_km = NULL
```
```
df = df.withColumn("delivery_time",when(col("online_order") == "No", None)\
.otherwise(col("delivery_time"))
)

df = df.withColumn("distance_km",when(col("online_order") == "No", None)\
.otherwise(col("distance_km"))
)
```

**4. Rating Transformation**

Converted rating strings like "4.5/5" to numeric floats.
```
df = df.withColumn("rating",when(col("rating") == "NEW", None).otherwise(col("rating")))
df=df.withColumn("rating_numeric",split(col("rating"),"/").getItem(0).cast("float"))
df=df.drop("rating")
```


## 🚀 Incremental Data Pipeline (Fabric Data Factory)

The pipeline was designed to load only new records using incremental logic.

**Step 1 — Lookup Activity**

Fetch the latest order number already processed.
<img width="1909" height="914" alt="image" src="https://github.com/user-attachments/assets/d0c84253-c887-47ab-ac22-d0f30cbaffe7" />


```
SELECT MAX(CAST(SUBSTRING(order_id,5,10) AS INT)) AS last_order_num
FROM Bronze.dbo.swiggy
```

This value is stored in a pipeline variable.

**Step 2 — Incremental Copy**

Only new rows greater than the last processed order ID are loaded.
<img width="1903" height="915" alt="image" src="https://github.com/user-attachments/assets/aec706e5-5613-49b2-b8a5-b615d0730850" />

Example logic:

```
SELECT *
FROM dbo.swiggy
WHERE CAST(SUBSTRING(order_id,5,10) AS INT) > @{activity('last_order_num').output.firstRow.max_num}
```
## ⏱️ Wait Activity (Important)

A 30-second Wait Activity was introduced before the second Copy Data activity.

Why this was necessary

Microsoft Fabric sometimes **delays metadata updates** after a table load.

Without the wait:

The pipeline fetches stale metadata

Newly inserted rows are not detected

Adding the wait ensures:

✔ Latest rows are visible                                                                                                                                                                                                                                                                                          
✔ Incremental copy works reliably                                                                                                                                                                                                                                                                                        
✔ Pipeline failures are prevented

## 🧹 Truncation Strategy

After each successful run:

The swiggy_incremental table is truncated

The main staging table keeps full history

Benefits:

✔ Prevents duplicate loads

✔ Keeps incremental table lightweight

✔ Preserves historical data

## 🧠 Dimensional Modeling (Gold Layer)

A Star Schema was implemented in the Fabric Data Warehouse using T-SQL Stored Procedures.
<img width="1911" height="927" alt="Screenshot 2026-03-17 232408" src="https://github.com/user-attachments/assets/aa2e48a8-2df2-43ce-bd9d-0cbce1da51f4" />

## 📊 Business Insights & SQL Analysis

More than 20 analytical queries were created to generate business insights.
check queries in /SQL_warehouse

## 📈 Power BI Dashboard


A Semantic Model was created to connect:
<img width="1911" height="916" alt="Screenshot 2026-03-17 232013" src="https://github.com/user-attachments/assets/1d4e9305-30e2-49ad-97e1-72d5eacc61bd" />
<img width="1911" height="924" alt="Screenshot 2026-03-17 235223" src="https://github.com/user-attachments/assets/b70bbc84-1f6a-4f89-8a30-6a0860f5e962" />

Fact tables

Dimension tables

This enabled high-performance reporting in Power BI.

Dashboard Highlights

Revenue by cuisine

Order trends by time

Online vs offline order distribution

Restaurant performance metrics

Delivery efficiency analysis
