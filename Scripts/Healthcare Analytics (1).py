# Importing libraries

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time

from pymongo import MongoClient, errors
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum


# Loading dataset

df = pd.read_csv("data/patients.csv")

print("Preview of dataset:")
print(df.head())
print("Dataset size:", df.shape)


# Cleaning data with pandas

df = df.drop_duplicates()
df = df.dropna()

# Ensure numeric consistency for analysis
df['age'] = df['age'].astype(int)
df['dx_hypertension'] = df['dx_hypertension'].astype(int)
df['dx_type2_diabetes'] = df['dx_type2_diabetes'].astype(int)
df['dx_obesity'] = df['dx_obesity'].astype(int)
df['dx_heart_failure'] = df['dx_heart_failure'].astype(int)

print("Data cleaned successfully!")


# Storing data in MolngoDB

client = MongoClient("mongodb://localhost:27017/")
db = client["health_db"]
collection = db["patients"]

# Convert DataFrame to dictionary
data = df.to_dict(orient="records")

# Insert in batches
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

# Create indexes
collection.create_index("patient_id", unique=True)
collection.create_index("age")
collection.create_index("sex")

print("Indexes created!")

# Pipeline:
# CSV → Pandas Cleaning → MongoDB Storage → Spark Processing → Visualization

# Spark

spark = SparkSession.builder.appName("BigDataHealthcare").getOrCreate()
print("Spark Session created successfully!")


# Loading into spark

# Save cleaned data for Spark consistency
df.to_csv("patients_clean.csv", index=False)

spark_df = spark.read.csv(
    "patients_clean.csv",
    header=True,
    inferSchema=True
)

spark_df = spark_df.na.fill(0)

print("Spark DataFrame Preview:")
spark_df.show()


# Transformations

print("Hypertension count:")
spark_df.groupBy("dx_hypertension").count().show()

print("Patients older than 50:")
spark_df.filter(spark_df.age > 50).show()


# Spark SQL

spark_df.createOrReplaceTempView("patients")

sql_query = """
SELECT dx_hypertension, COUNT(*) 
FROM patients 
GROUP BY dx_hypertension
"""

print("Spark SQL result:")
spark.sql(sql_query).show()

print("Execution Plan:")
spark.sql(sql_query).explain(True)


# MongoDB vs spark

print("MongoDB Aggregation Result:")

start = time.time()

mongo_result = list(collection.aggregate([
    {"$group": {"_id": "$dx_hypertension", "count": {"$sum": 1}}}
]))

end = time.time()

for doc in mongo_result:
    print(doc)

print("MongoDB Query Time:", end - start)


print("\nSpark SQL Aggregation Result:")

start = time.time()

spark_result = spark.sql("""
SELECT dx_hypertension, COUNT(*) as count
FROM patients
GROUP BY dx_hypertension
""").collect()

end = time.time()

print("Spark SQL Query Result:")
for row in spark_result:
    print(row)

print("Spark SQL Query Time:", end - start)


# Aggregations

print("Disease totals:")

spark_df.select(
    spark_sum("dx_hypertension").alias("hypertension"),
    spark_sum("dx_type2_diabetes").alias("type2_diabetes"),
    spark_sum("dx_obesity").alias("obesity")
).show()


print("Heart failure by age:")
spark_df.groupBy("age").avg("dx_heart_failure").show()


# Spark to pandas visualizations

age_group = spark_df.groupBy("age").avg("dx_hypertension")
pdf = age_group.toPandas()

pdf.plot(x='age', y='avg(dx_hypertension)', kind='line')
plt.title("Hypertension Rate by Age (Spark)")
plt.show()

spark_df.groupBy("dx_type2_diabetes").count().toPandas().plot(kind='bar')
plt.title("Diabetes Count (Spark)")
plt.show()


# Parquet storage test for bonus points

spark_df.write.mode("overwrite").parquet("patients.parquet")

parquet_df = spark.read.parquet("patients.parquet")

print("Parquet Data Preview:")
parquet_df.show()


# Pandas and seaborn visualizations

df['dx_hypertension'].value_counts().plot(kind='bar')
plt.title("Hypertension Distribution")
plt.show()

sns.countplot(x="dx_hypertension", data=df)
plt.title("Hypertension Count (Seaborn)")
plt.show()


# More visualizations

plt.figure()
sns.histplot(df['age'], bins=20)
plt.title("Age Distribution")
plt.show()


disease_counts = df[['dx_hypertension','dx_type2_diabetes','dx_obesity']].sum()

plt.figure()
disease_counts.plot(kind='bar')
plt.title("Disease Comparison")
plt.show()


plt.figure()
sns.heatmap(
    df[['age','dx_hypertension','dx_type2_diabetes','dx_obesity','dx_heart_failure']].corr(),
    annot=True
)
plt.title("Correlation Heatmap")
plt.show()


plt.figure()
sns.boxplot(x=df['dx_hypertension'], y=df['age'])
plt.title("Age vs Hypertension")
plt.show()


plt.figure()
disease_counts.plot(kind='pie', autopct='%1.1f%%')
plt.title("Disease Distribution")
plt.ylabel("")
plt.show()


# Age group analysis

df['age_group'] = pd.cut(df['age'], bins=[0,20,40,60,80,101])

age_group_data = df.groupby('age_group')[['dx_hypertension','dx_obesity']].sum()

age_group_data.plot(kind='bar')
plt.title("Disease by Age Group")
plt.show()


# Final spark visualization

spark_df.groupBy("dx_hypertension").count().toPandas().plot(kind='bar')
plt.title("Spark Hypertension Count")
plt.show()

print("Visualization completed!")

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