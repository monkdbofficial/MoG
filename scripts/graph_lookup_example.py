from pymongo import MongoClient
from pymongo.errors import PyMongoError


# MONGO_URI = "mongodb://user:password@localhost:27018/admin"
MONGO_URI = "mongodb://localhost:27017/admin"
DB_NAME = "social"
USERS = "users"


def main() -> None:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        db = client[DB_NAME]
        users = db[USERS]

        users.drop()
        users.insert_many(
            [
                {"_id": "u1", "user_id": 1, "name": "Alice", "manager_id": None, "active": True},
                {"_id": "u2", "user_id": 2, "name": "Bob", "manager_id": 1, "active": True},
                {"_id": "u3", "user_id": 3, "name": "Cara", "manager_id": 2, "active": True},
                {"_id": "u4", "user_id": 4, "name": "Dan", "manager_id": 2, "active": False},
            ],
            ordered=True,
        )

        print("--- Seeded Users ---")
        for doc in users.find({}, {"_id": 1, "user_id": 1, "name": 1, "manager_id": 1, "active": 1}).sort("user_id", 1):
            print(doc)

        pipeline = [
            {"$match": {"user_id": 3}},
            {
                "$graphLookup": {
                    "from": "users",
                    "startWith": "$manager_id",
                    "connectFromField": "manager_id",
                    "connectToField": "user_id",
                    "maxDepth": 3,
                    "depthField": "depth",
                    "restrictSearchWithMatch": {"active": True},
                    "as": "management_chain",
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "user_id": 1,
                    "name": 1,
                    "management_chain.user_id": 1,
                    "management_chain.name": 1,
                    "management_chain.depth": 1,
                }
            },
        ]

        print("\n--- Running $graphLookup ---")
        print(pipeline)
        results = list(users.aggregate(pipeline, maxTimeMS=10000))

        print("\n--- Graph Lookup Results ---")
        for doc in results:
            print(doc)
        if not results:
            print("No results returned.")

    except PyMongoError as err:
        print(f"Graph lookup failed: {err}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
