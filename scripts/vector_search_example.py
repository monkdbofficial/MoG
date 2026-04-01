from pymongo import MongoClient
from pymongo.errors import PyMongoError


# MONGO_URI = "mongodb://user:password@localhost:27018/admin"
MONGO_URI = "mongodb://localhost:27017/admin"
DB_NAME = "testdb"
COLLECTION_NAME = "articles"
# The `MONGO_URI` variable is storing the connection string for connecting to a MongoDB database. In
# this case, the URI is `"mongodb://user:password@localhost:27018/admin"`.


def main() -> None:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        collection = client[DB_NAME][COLLECTION_NAME]

        collection.drop()
        collection.insert_many(
            [
                {
                    "_id": "doc_1",
                    "title": "MonkDB is great for time-series and vector workloads.",
                    "embedding": [0.91, 0.07, 0.01, 0.22],
                },
                {
                    "_id": "doc_2",
                    "title": "Vector search in databases is important for AI applications.",
                    "embedding": [0.11, 0.80, 0.05, 0.33],
                },
                {
                    "_id": "doc_3",
                    "title": "MonkDB provides scalable distributed storage.",
                    "embedding": [0.84, 0.10, 0.02, 0.18],
                },
            ],
            ordered=True,
        )

        print("--- Seeded Documents ---")
        for doc in collection.find({}, {"_id": 1, "title": 1, "embedding": 1}):
            print(doc)

        pipeline = [
            {
                "$vectorSearch": {
                    "path": "embedding",
                    "queryVector": [0.10, 0.79, 0.06, 0.30],
                    "limit": 2,
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "title": 1,
                    "score": {"$meta": "vectorSearchScore"},
                }
            },
        ]

        print("\n--- Running Vector Search ---")
        print(pipeline)
        results = list(collection.aggregate(pipeline, maxTimeMS=10000))

        print("\n--- Vector Search Results ---")
        for doc in results:
            print(doc)
        if not results:
            print("No results returned.")

    except PyMongoError as err:
        print(f"Vector search failed: {err}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
