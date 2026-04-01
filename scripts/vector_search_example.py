
from pymongo import MongoClient

# --- Connection ---
# Connect to the MoG instance, which is listening on the default MongoDB port.
client = MongoClient('mongodb://user:password@localhost:27018/admin')
db = client['testdb']
collection = db['articles']

# --- Data Setup ---
# Clear existing data and insert some sample documents with vector embeddings.
collection.delete_many({})
collection.insert_many([
    {
        "_id": 1,
        "title": "An article about cats",
        "embedding": [0.1, 0.8, 0.2]
    },
    {
        "_id": 2,
        "title": "An article about dogs",
        "embedding": [0.7, 0.2, 0.1]
    },
    {
        "_id": 3,
        "title": "Another article about cats",
        "embedding": [0.2, 0.7, 0.3]
    }
])

print("--- Initial Data in Collection ---")
for doc in collection.find():
    print(doc)

# --- Vector Search ---
# Define the vector to search for and the aggregation pipeline.
query_vector = [0.15, 0.75, 0.25]
pipeline = [
    {
        "$vectorSearch": {
            "path": "embedding",
            "queryVector": query_vector,
            "limit": 2
        }
    }
]

print(f"\n--- Performing vector search with query: {query_vector} ---")
results = list(collection.aggregate(pipeline))

# --- Results ---
print("\n--- Vector Search Results ---")
for doc in results:
    print(f"Score: {doc.get('__mog_vectorSearchScore', 'N/A')}, Document: {doc}")

client.close()
