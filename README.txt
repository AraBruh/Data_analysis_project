README


🏥 Big Data Pipeline for Healthcare Analytics


📌 Overview

This project implements a Big Data analytics pipeline for healthcare data using Pandas, MongoDB, and Apache Spark.

It processes a dataset of 100,000 patient records to uncover:

Disease patterns
Age-related health risks
Population-level trends

The project demonstrates how NoSQL databases and distributed computing can be combined for scalable healthcare analytics.

📊 Dataset
📁 Source: Kaggle healthcare dataset
📦 Size: ~100,000 records
📄 Format: CSV (patients.csv)
Features:
Age
Hypertension
Type 2 Diabetes
Obesity
Heart Failure
⚙️ Tech Stack
Tool	Purpose
🐍 Python	Core programming
📊 Pandas	Data cleaning & preprocessing
🍃 MongoDB	NoSQL storage & aggregation
⚡ PySpark	Distributed data processing
📈 Matplotlib / Seaborn	Visualization
🏗️ Architecture / Pipeline
Raw CSV
   ↓
Pandas (Cleaning & Preprocessing)
   ↓
MongoDB (Storage + Indexing)
   ↓
Clean CSV Export
   ↓
Apache Spark (Processing + SQL)
   ↓
Visualization (Pandas / Seaborn)

⚠️ Note: Spark processes data from the cleaned CSV file, not directly from MongoDB.

🚀 Features
🔹 Data Preprocessing
Removed duplicates
Handled missing values
Converted data types for consistency
🔹 MongoDB Integration
Stored data as JSON-like documents
Batch insertion (500 records per batch)
Indexing on:
age
dx_hypertension
🔹 Apache Spark Processing
DataFrame transformations:
Filtering (e.g., age > 50)
Aggregations & grouping
Spark SQL queries
Execution plan analysis
🔹 Performance Comparison
MongoDB vs Spark SQL aggregation
Query execution time measurement
🔹 Visualization
Disease distributions
Age distribution
Correlation heatmaps
Boxplots & histograms
🔹 Storage Optimization
Exported dataset to Parquet format
📈 Example Queries
Spark SQL
SELECT dx_hypertension, COUNT(*) 
FROM patients 
GROUP BY dx_hypertension;
MongoDB Aggregation
collection.aggregate([
    {"$group": {"_id": "$dx_hypertension", "count": {"$sum": 1}}}
])
📊 Key Insights
Disease prevalence increases with age
Hypertension and obesity show strong correlation
Spark significantly improves performance for large-scale queries
MongoDB indexing reduces query time
▶️ How to Run
1️⃣ Install Dependencies
pip install pandas matplotlib seaborn pymongo pyspark
2️⃣ Start MongoDB
mongod

Default URI:

mongodb://localhost:27017/
3️⃣ Run the Project
python main.py
📁 Project Structure
├── data/
│   └── patients.csv
├── patients_clean.csv
├── patients.parquet
├── main.py
├── README.md
👥 Team Members
Ali Al-Mashhadani – 2305616
Anshelika Anttila – 2305051
Rashinthi Jayalath – 2301285
⚠️ Challenges
Handling large datasets efficiently
Ensuring consistency between Pandas and Spark
Understanding Spark execution plans
🔮 Future Improvements
🔗 Integrate Spark ↔ MongoDB Connector
📡 Add real-time streaming (Kafka + Spark Streaming)
📊 Build an interactive dashboard (Streamlit / Power BI)
🧠 Apply machine learning for disease prediction
⭐ Acknowledgements
Kaggle for the dataset
Open-source tools: PySpark, MongoDB, Pandas