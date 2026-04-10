import pandas as pd
from pymongo import MongoClient, errors

# 1. Load CSV
df = pd.read_csv("patients.csv")
print("CSV loaded. Shape:", df.shape)

# 2. Convert to dict
data = df.to_dict(orient="records")

# 3. Connect to MongoDB
client = MongoClient("mongodb+srv://Ali_Al-Mashhadani:Ali20012001@rawrxd.xnap8xc.mongodb.net/?appName=RawrXD")

db = client["healthcare_db"]
collection = db["patients"]

# 4. Drop old collection because previously the uploaded items doubled
collection.drop()
print("Old collection deleted.")

# 5. Create indexes
collection.create_index("patient_id", unique=True)
collection.create_index("age")
collection.create_index("sex")
print("Indexes created.")

# 6. Upload in smaller batches because the upload kept timing out
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