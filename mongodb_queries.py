from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

load_dotenv()  # loads variables from .env
mongo_uri = os.getenv("MONGO_URI")

db = client.mydatabasedbName = 'vehicle_data'

def execute_queries(start_time_str, end_time_str):
    # Connect to MongoDB
    client = MongoClient(mongo_uri, server_api=ServerApi('1'))

    if dbName not in client.list_database_names():
        raise Exception(f"Database doesn't exist.")
    db = client[dbName]

    # Collection for processed data
    processed_collection = db.processed_data
    raw_collection = db.raw_data

    # Convert time strings to datetime objects
    start_time = datetime.strptime(start_time_str, "%d/%m/%Y %H:%M:%S")
    end_time = datetime.strptime(end_time_str, "%d/%m/%Y %H:%M:%S")

    # Fetch data from the collection within the specified time range

    # First Query, link with the lowest vehicle count
    query1_result = list(processed_collection.find({
        "time": {
            "$gte": start_time.strftime("%d/%m/%Y %H:%M:%S"),
            "$lte": end_time.strftime("%d/%m/%Y %H:%M:%S"),

        },}
    ).sort({
        "vcount":1})
    )


    if not query1_result:
        raise ValueError("No link found for the specified time range.")

    # Convert the data into a DataFrame
    min_vcount = pd.DataFrame(query1_result)
    print("First Query Results")
    print(f"WIthin the specified time range, the link {min_vcount.iloc[0]['link']} had the lowest vehicle count, {min_vcount.iloc[0]['vcount']}")

    # Second Query, link with the highest mean vehicle speed
    query2_result = list(processed_collection.find({
        "time": {
            "$gte": start_time.strftime("%d/%m/%Y %H:%M:%S"),
            "$lte": end_time.strftime("%d/%m/%Y %H:%M:%S"),

        },
    }).sort({
        "vspeed": -1
    }))

    if not query2_result:
        raise ValueError("No link found for the specified time range.")

    # Convert the data into a DataFrame
    max_vspeed = pd.DataFrame(query2_result)
    print(f"\nSecond Query Results")
    print(f"Within the specified time range, the link {max_vspeed.iloc[0]['link']} had the highest mean speed, {max_vspeed.iloc[0]['vspeed']}")

    # Third query, vehicle that travelled the longest distance
    query3_result = list(raw_collection.aggregate([
        {  # Match documents that fall within the date range
            "$match": {
                "time": {
                    "$gte": start_time.strftime("%d/%m/%Y %H:%M:%S"),
                    "$lte": end_time.strftime("%d/%m/%Y %H:%M:%S"),
                }
            }
        },
        {   # For each vehicle on each link, find the maximum position (distance)
            "$group": {
                "_id": { "name": "$name", "link": "$link" },
                "maxPosition": { "$max": "$position"}
            }
        },
        {   # Sum the distances for each vehicle
            "$group": {
                "_id": "$_id.name",
                "totalDistance": {"$sum": "$maxPosition"}
            }
        },
        {   # Sort by vehicle name and total distance
            "$sort": { "_id.name": 1, "totalDistance": -1 }
        }
    ]))

    if not query3_result:
        raise ValueError("No vehicle found for the specified time range.")

    max_distance = pd.DataFrame(query3_result)
    print(f"\nThird Query Results")
    print(
        f"Within the specified time range, the vehicle team {max_distance.iloc[0]['_id']} travelled the longest distance, {max_distance.iloc[0]['totalDistance']+1.0}")
    # Adding 1 to the max_distance, cause trip_end position equals -1.0

if __name__ == '__main__':
    start_time_str = "01/09/2024 00:00:00"
    end_time_str = "21/09/2024 23:08:55"
    execute_queries(start_time_str, end_time_str)
