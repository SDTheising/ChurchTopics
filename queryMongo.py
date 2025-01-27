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

def query_churches(collection, language=None, denomination=None, range_value=None):
    pipeline = []

    # Stage 1: Match on language and denomination if they're provided
    match_stage = {}
    if language:
        match_stage["Language"] = {"$regex": language, "$options": "i"}
    if denomination:
        match_stage["Denomination"] = {"$regex": denomination, "$options": "i"}

    # Only add a $match if there's something to match on
    if match_stage:
        pipeline.append({"$match": match_stage})

    # Stage 2: If range_value is provided, add all the "size" parsing steps
    if range_value is not None:
        try:
            range_int = int(range_value)
        except ValueError:
            raise ValueError("Range value must be an integer.")

        pipeline.extend([
            # Clean up the 'Size' field
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
            # Project intermediate steps for debug or clarity
            {
                "$project": {
                    "_id": 1,
                    "ChurchName": 1,
                    "Language": 1,
                    "Denomination": 1,
                    "Website": 1,
                    "Size": 1,
                    "clean_size": 1,
                    "split_size": {"$split": ["$clean_size", "-"]}
                }
            },
            # Keep docs that have exactly two elements in split_size
            {
                "$match": {
                    "$expr": {"$eq": [{"$size": "$split_size"}, 2]}
                }
            },
            # Convert the parts into min_size, max_size (integers)
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
            # Filter out where conversion failed
            {
                "$match": {
                    "$and": [
                        {"min_size": {"$ne": None}},
                        {"max_size": {"$ne": None}}
                    ]
                }
            },
            # Keep only those where min <= range_int <= max
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {"$lte": ["$min_size", range_int]},
                            {"$gte": ["$max_size", range_int]}
                        ]
                    }
                }
            },
            # Final project
            {
                "$project": {
                    "_id": 1,
                    "ChurchName": 1,
                    "Language": 1,
                    "Denomination": 1,
                    "Size": 1,
                    "Website": 1
                }
            }
        ])

    # Finally, run the pipeline if range is provided, or do a simpler find if not
    if range_value is not None:
        results = collection.aggregate(pipeline)
    else:

        if pipeline:
            results = collection.aggregate(pipeline)
        else:
            # If no language/denomination or range is provided, there's no pipeline, so just return everything
            results = collection.find()

    return list(results)


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
