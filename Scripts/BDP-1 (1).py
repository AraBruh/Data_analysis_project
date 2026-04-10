
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Loading dataset
df = pd.read_csv("C:/Users/anjel/Documents/HAMK/YEAR 3/Big Data/patients.csv")

# Show first rows
print("Preview of dataset:")
print(df.head())

# Cleaning data
df = df.drop_duplicates()
df = df.dropna()

print("Data cleaned!")

from pymongo import MongoClient

# Connecting to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["health_db"]
collection = db["patients"]

# Convert DataFrame to a dictionary
data = df.to_dict(orient="records")

# Insert data in batches (to avoid timeout)
batch_size = 500
success_count = 0

for i in range(0, len(data), batch_size):
    batch = data[i:i+batch_size]
    
    try:
        collection.insert_many(batch, ordered=False)
        success_count += len(batch)
        print(f"Uploaded batch {i//batch_size + 1}")
    
    except errors.BulkWriteError as e:
        print("Duplicate detected, skipping some records.")
        success_count += len(batch) - len(e.details['writeErrors'])

print(f"Upload complete. Inserted {success_count} records.")

# Create indexes for faster queries
collection.create_index("patient_id", unique=True)
collection.create_index("age")
collection.create_index("sex")

print("Indexes created!")

# Processing and Transforming data using Apache Spark

from pyspark.sql import SparkSession

# Starting Spark session
spark = SparkSession.builder.appName("BigDataHealthcare").getOrCreate()

# Loading dataset into Spark
spark_df = spark.read.csv(
    "C:/Users/anjel/Documents/HAMK/YEAR 3/Big Data/patients.csv",
    header=True,
    inferSchema=True
)

print("Spark DataFrame preview:")
spark_df.show()

# Count hypertension cases
print("Hypertension count:")
spark_df.groupBy("dx_hypertension").count().show()

# Filter patients older than 50
print("Patients older than 50:")
spark_df.filter(spark_df.age > 50).show()

# Using Spark SQL

# Create SQL table
spark_df.createOrReplaceTempView("patients")

# Run SQL query
print("Spark SQL result:")
spark.sql("""
SELECT dx_hypertension, COUNT(*) 
FROM patients 
GROUP BY dx_hypertension
""").show()

# To explain..

print("Spark SQL result:")
spark.sql("""
SELECT dx_hypertension, COUNT(*) 
FROM patients 
GROUP BY dx_hypertension
""").explain(True)

# Aggregation

from pyspark.sql.functions import sum

print("Disease totals:")
spark_df.select(
    sum("dx_hypertension").alias("hypertension"),
    sum("dx_type2_diabetes").alias("type2_diabetes"),
    sum("dx_obesity").alias("obesity")
).show()

print("Heart failure by age:")
spark_df.groupBy("age").avg("dx_heart_failure").show()

# 1 
spark_result = spark_df.groupBy("age").avg("dx_hypertension")

pdf = spark_result.toPandas()

pdf.plot(x='age', y='avg(dx_hypertension)', kind='line')
plt.title("Hypertension Rate by Age (Spark)")
plt.show()

# 2
spark_result2 = spark_df.groupBy("dx_type2_diabetes").count()
spark_result2.toPandas().plot(kind='bar')
plt.title("Diabetes Count (Spark)")
plt.show()

# Visualizing


# Bar chart (Matplotlib)
df['dx_hypertension'].value_counts().plot(kind='bar')
plt.title("Hypertension Distribution")
plt.show()

# Seaborn plot
sns.countplot(x="dx_hypertension", data=df)
plt.title("Hypertension Count (Seaborn)")
plt.show()

print("Visualization completed!")

# Extra visualizations

# To get an idea of the age distribution of the data set for context

plt.figure()
sns.histplot(df['age'], bins=20)
plt.title("Age Distribution")
plt.xlabel("Age")
plt.ylabel("Count")
plt.show()

# Disease Comparison

disease_counts = df[['dx_hypertension','dx_type2_diabetes','dx_obesity']].sum()

plt.figure()
disease_counts.plot(kind='bar')
plt.title("Disease Comparison")
plt.ylabel("Number of Patients")
plt.show()

# Correlation between diseases

plt.figure()
sns.heatmap(df[['age','dx_hypertension','dx_type2_diabetes','dx_obesity','dx_heart_failure']].corr(), annot=True)
plt.title("Correlation Heatmap")
plt.show()

# Age vs diseases 

plt.figure()
sns.boxplot(x=df['dx_hypertension'], y=df['age'])
plt.title("Age vs Hypertension")
plt.show()

# Top diseases

plt.figure()
disease_counts.plot(kind='pie', autopct='%1.1f%%')
plt.title("Disease Distribution")
plt.ylabel("")
plt.show()

# Age group analysis

df['age_group'] = pd.cut(df['age'], bins=[0,20,40,60,80,100])

age_group_data = df.groupby('age_group')[['dx_hypertension','dx_obesity']].sum()

age_group_data.plot(kind='bar')
plt.title("Disease by Age Group")
plt.show()

# Spark visualization

spark_df.groupBy("dx_hypertension").count().toPandas().plot(kind='bar')
plt.title("Spark Hypertension Count")
plt.show()

# Bonus: CSV vs Parquet comparison

import time

# Save as Parquet
spark_df.write.mode("overwrite").parquet("patients_parquet")

# CSV load time
start = time.time()
df_csv = spark.read.csv(
    "C:/Users/anjel/Documents/HAMK/YEAR 3/Big Data/patients.csv",
    header=True,
    inferSchema=True
)
df_csv.count()
print("CSV Load Time:", time.time() - start)

# Parquet load time
start = time.time()
df_parquet = spark.read.parquet("patients_parquet")
df_parquet.count()
print("Parquet Load Time:", time.time() - start)