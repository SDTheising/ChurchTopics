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

# Function to query by language, denomination, or size range
def query_churches(collection, language=None, denomination=None, range_value=None):
    query = {}

    # Add language filter if provided
    if language:
        query["Language"] = {"$regex": language, "$options": "i"}

    # Add denomination filter if provided
    if denomination:
        query["Denomination"] = {"$regex": denomination, "$options": "i"}

    # Add range filter if provided
    if range_value is not None:
        try:
            range_value = int(range_value)
            # Use MongoDB aggregation to parse size ranges and filter
            pipeline = [
                {
                    "$addFields": {
                        "min_size": {"$toInt": {"$arrayElemAt": [{"$split": ["$Size", "-"]}, 0]}},
                        "max_size": {"$toInt": {"$arrayElemAt": [{"$split": ["$Size", "-"]}, 1]}}
                    }
                },
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
            results = collection.aggregate(pipeline)
            return [doc for doc in results]
        except ValueError:
            raise ValueError("Range value must be an integer.")

    # Execute the query for language/denomination only
    results = collection.find(query)
    return [doc for doc in results]

# Main function
def main():
    
    field_map = {
        "name": "ChurchName",
        "language": "Language",
        "denomination": "Denomination",
        "size": "Size",
        "url": "Website"
    }
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Query MongoDB collection for churches.")
    parser.add_argument(
        "--language",
        type=str,
        help="Search for churches with a specific language (regex supported)."
    )
    parser.add_argument(
        "--denomination",
        type=str,
        help="Search for churches with a specific denomination (regex supported)."
    )
    parser.add_argument(
        "--size",
        type=str,
        help="Search for churches where the size range contains the given number."
    )
    parser.add_argument(
        "--returns",
        type=str,
        default="ChurchName",
        help=f"Define what sections to return. Valid options are: {', '.join(field_map.keys())}"
    )
    
    args = parser.parse_args()
    
 

    # Connect to the database
    collection = connect_to_db()

    # Query the database
    try:
        churches = query_churches(
            collection,
            language=args.language,
            denomination=args.denomination,
            range_value=args.size
        )
        returns = field_map.get(args.returns, None)
        if returns is None:
            print(f"Error: Invalid --returns value '{args.returns}'. Valid options are: {', '.join(field_map.keys())}")
            return
        # Print the results
        if churches:
            print("Matching churches:")
            for church in churches:
                print(church.get(returns))
        else:
            print("No matching churches found.")
    except ValueError as e:
        print(f"Error: {e}")

# Run the main function only if executed directly
if __name__ == "__main__":
    main()
