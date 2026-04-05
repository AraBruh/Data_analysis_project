from pymongo import MongoClient

client = MongoClient("mongodb+srv://Ali_Al-Mashhadani:Ali20012001@rawrxd.xnap8xc.mongodb.net/?appName=RawrXD")
db = client["healthcare_db"]
collection = db["patients"]

# 1. Count total patients
total = collection.count_documents({})
print("Total patients:", total)

# 2. Average age
pipeline = [
    {"$group": {"_id": None, "avg_age": {"$avg": "$age"}}}
]
print("Average age:", list(collection.aggregate(pipeline)))

# 3. Count by sex
pipeline = [
    {"$group": {"_id": "$sex", "count": {"$sum": 1}}}
]
print("Patients by sex:", list(collection.aggregate(pipeline)))
