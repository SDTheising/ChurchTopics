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
                            "ChurchName": 1,
                            "Language": 1,
                            "Denomination": 1,
                            "Website": 1,
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
                },
                # Step 7: Include only required fields in the final output
                {
                    "$project": {
                        "ChurchName": 1,
                        "Language": 1,
                        "Denomination": 1,
                        "Size": 1,
                        "Website": 1  # Ensure all potential fields from 'field_map' are included
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
        default=["name"],
        nargs="+",
        help=f"Define what sections to return. Valid options are: {', '.join(field_map.keys())}. Note, can have multiple in the format \"--returns <arg> <arg>\""
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
        # Handle the case where `args.returns` is a list of fields
        returns = [field_map.get(field, None) for field in args.returns]

        # Check for invalid fields
        if None in returns:
            invalid_fields = [field for field, mapped in zip(args.returns, returns) if mapped is None]
            print(f"Error: Invalid fields in --returns: {', '.join(invalid_fields)}")
            print(f"Valid options are: {', '.join(field_map.keys())}")
            return

        if returns is None:
            print(f"Error: Invalid --returns value '{args.returns}'. Valid options are: {', '.join(field_map.keys())}")
            return
        # Print the results
        if churches:
            print("Matching churches:")
            for church in churches:
                print({field: church.get(field_map.get(field, "Field not found")) for field in args.returns})
        else:
            print("No matching churches found.")
    except ValueError as e:
        print(f"Error: {e}")

# Run the main function only if executed directly
if __name__ == "__main__":
    main()
