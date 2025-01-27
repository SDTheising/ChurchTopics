from pymongo import MongoClient
import os
from dotenv import load_dotenv
import argparse

# Load environment variables from a .env file
load_dotenv()

# Function to connect to the database
def connect_to_db():
    client = MongoClient(os.getenv("MONGO_DB"))
    db = client["Sermons"]  # Replace with your database name
    return db["Churches"]  # Replace with your collection name

# Range value for filtering (example: 400)
range_value = 100

collection = connect_to_db()
# Define the pipeline with debugging steps
pipeline = [
    # Step 1: Clean up the 'Size' field by removing spaces and commas
    {
        "$addFields": {
            "clean_size": {
                "$replaceAll": {
                    "input": {"$replaceAll": {"input": "$Size", "find": ",", "replacement": ""}},
                    "find": " ",
                    "replacement": ""
                }
            }
        }
    },
    # Step 2: Add a debug projection stage to inspect intermediate values
    {
        "$project": {
            "_id": 1,
            "Size": 1,  # Original Size field
            "clean_size": 1,  # Cleaned version of Size
            "split_size": {"$split": ["$clean_size", "-"]},  # Split the cleaned size into min and max
        }
    },
    # Step 3: Filter documents with invalid split results
    {
        "$match": {
            "$expr": {
                "$eq": [{"$size": "$split_size"}, 2]
            }
        }
    },
    # Step 4: Add fields for min_size and max_size, with error handling
    {
        "$addFields": {
            "min_size": {
                "$convert": {
                    "input": {"$arrayElemAt": ["$split_size", 0]},
                    "to": "int",
                    "onError": None
                }
            },
            "max_size": {
                "$convert": {
                    "input": {"$arrayElemAt": ["$split_size", 1]},
                    "to": "int",
                    "onError": None
                }
            }
        }
    },
    # Step 5: Filter out documents where min_size or max_size is null
    {
        "$match": {
            "$and": [
                {"min_size": {"$ne": None}},
                {"max_size": {"$ne": None}}
            ]
        }
    },
    # Step 6: Final range filtering
    {
        "$match": {
            "$expr": {
                "$and": [
                    {"$lte": ["$min_size", range_value]},
                    {"$gte": ["$max_size", range_value]}
                ]
            }
        }
    }
]

# Execute the pipeline
results = collection.aggregate(pipeline)

# Debug output: Print intermediate results
print("Debugging Results:")
print("-" * 40)

for doc in results:
    print(f"Document ID: {doc['_id']}")
    print(f"Original Size: {doc.get('Size')}")
    print(f"Cleaned Size: {doc.get('clean_size')}")
    print(f"Split Size: {doc.get('split_size')}")
    print(f"Min Size: {doc.get('min_size')}")
    print(f"Max Size: {doc.get('max_size')}")
    print("-" * 40)

print("Pipeline completed. Check above for any problematic entries.")
