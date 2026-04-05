from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PatientsAnalysis").getOrCreate()

# Load CSV again (simpler than MongoDB integration for now)
df = spark.read.csv("patients.csv", header=True, inferSchema=True)

df.show(5)

# 🔹 Count patients
print("Total:", df.count())

# 🔹 Average age
df.selectExpr("avg(age)").show()

# 🔹 Group by sex
df.groupBy("sex").count().show()

import matplotlib.pyplot as plt

pdf = df.groupBy("sex").count().toPandas()

plt.figure(figsize=(6,4))
bars = plt.bar(pdf["sex"], pdf["count"])

# Add numbers on top of bars
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval, int(yval), 
             ha='center', va='bottom')

plt.title("Patient Distribution by Sex")
plt.xlabel("Sex")
plt.ylabel("Number of Patients")

plt.tight_layout()
plt.show()

from pyspark.sql.functions import sum as spark_sum

disease_cols = [col for col in df.columns if col.startswith("dx_")]

# Sum each disease column
disease_counts = df.select([
    spark_sum(col).alias(col) for col in disease_cols
])

disease_counts.show()
pdf = disease_counts.toPandas().T.reset_index()
pdf.columns = ["disease", "count"]

pdf = pdf.sort_values(by="count", ascending=False).head(10)


# Clean disease names
pdf["disease"] = pdf["disease"].str.replace("dx_", "") \
                               .str.replace("_", " ") \
                               .str.title()

plt.figure(figsize=(10,5))
bars = plt.bar(pdf["disease"], pdf["count"])

plt.xticks(rotation=45, ha="right")

# Add values
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval, int(yval),
             ha='center', va='bottom')

plt.title("Top 10 Most Common Diseases")
plt.xlabel("Disease")
plt.ylabel("Number of Patients")

plt.tight_layout()
plt.show()



from pyspark.sql.functions import avg, when

# Create age groups
df_grouped = df.withColumn(
    "age_group",
    when(df.age < 30, "Under 30")
    .when(df.age < 50, "30-50")
    .when(df.age < 70, "50-70")
    .otherwise("70+")
)

# Calculate hypertension rate per group
group_analysis = df_grouped.groupBy("age_group").agg(
    avg("dx_hypertension").alias("hypertension_rate")
)

pdf = group_analysis.toPandas()
import matplotlib.pyplot as plt

plt.figure(figsize=(6,4))

bars = plt.bar(pdf["age_group"], pdf["hypertension_rate"])

# Add percentage labels
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval, 
             f"{yval*100:.1f}%", ha='center', va='bottom')

plt.title("Hypertension Rate by Age Group")
plt.xlabel("Age Group")
plt.ylabel("Percentage")

plt.ylim(0,1)

plt.tight_layout()
plt.show()



disease_cols = [col for col in df.columns if col.startswith("dx_")]
corr_dict = {}
for i in range(len(disease_cols)):
    for j in range(i+1, len(disease_cols)):
        disease1 = disease_cols[i]
        disease2 = disease_cols[j]
        corr = df.stat.corr(disease1, disease2)  # Spark correlation
        corr_dict[(disease1, disease2)] = corr


import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Create empty matrix
matrix = pd.DataFrame(np.zeros((len(disease_cols), len(disease_cols))),
                      index=disease_cols, columns=disease_cols)

# Fill upper triangle with correlations
for (d1, d2), val in corr_dict.items():
    matrix.loc[d1, d2] = val
    matrix.loc[d2, d1] = val  # mirror

# Fill diagonal safely
for i in range(len(matrix)):
    matrix.iloc[i, i] = 1

# Clean labels
matrix.index = [x.replace("dx_", "").replace("_", " ").title() for x in matrix.index]
matrix.columns = matrix.index

# Plot
plt.figure(figsize=(10,8))
sns.heatmap(matrix, annot=True, fmt=".2f", cmap="Reds", cbar_kws={'label': 'Correlation'})
plt.title("Co-occurring Diseases Heatmap")
plt.tight_layout()
plt.show()