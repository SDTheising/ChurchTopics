query = {"Language": {"$regex": "Spanish", "$options": "i"}}
projection = {"ChurchName": 1, "_id": 0}  # Include ChurchName, exclude _id

results = collection.find(query, projection)

# Print just the names
for document in results:
    print(document.get("ChurchName"))
