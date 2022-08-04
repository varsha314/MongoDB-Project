import json
import pymongo
from datetime import datetime


def create_db(doc):
    # Open JSON file
    with open(doc) as file:
        # Load JSON file
        obj = json.load(file)
    # Iterate through all the products in the document
    for i in range(len(obj["products"])):
        # Get current time
        dt = datetime.now()
        # Convert to timestamp
        ts = int(datetime.timestamp(dt))
        date = str(dt.date())
        data = {"query": obj["debug"]["query_without_qrm"], "final_query": obj["final_query"],
                "num_found": obj["numFound"], "category": obj["products"][i]["category"],
                "vendor": obj["products"][i]["store"],
                "attributes": [
                    {"color": obj["products"][i]["color"], "category": obj["products"][i]["category"],
                     "discount": obj["products"][i]["discounted_price"]}], "date": date, "timestamp": ts}
        # Insert data into database
        mycol.insert_one(data)


try:
    # Establish a connection btw the Python application and MongoDB
    client = pymongo.MongoClient()
    print("Connected successfully!!!")
except:
    print("Could not connect to MongoDB")

# Accessing database
mydb = client["new"]
# Accessing collection
mycol = mydb["add"]
# Call the function by passing json document as argument
create_db("response_99.json")
