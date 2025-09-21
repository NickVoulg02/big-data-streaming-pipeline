from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

load_dotenv()  # loads variables from .env
mongo_uri = os.getenv("MONGO_URI")"
dbName = 'vehicle_data'

def create_database():
    try:
        # Create a new client and connect to the server
        client = MongoClient(mongo_uri, server_api=ServerApi('1'))

        # Check if the database exists
        if dbName in client.list_database_names():
            print(f"Database already exists.")
        else:
            # MongoDB waits until you have created a collection (table),
            # with at least one document (record) before it actually creates the database (and collection).
            # So we're going to insert a dummy document

            new_db = client[dbName]
            processed = new_db["processed_data"]
            raw_data = new_db["raw_data"]
            print(f"Database and collections created successfully.")

    except Exception as exception:
        print(exception)
    finally:
        client.close()

def processed_df(batch):
    batch.show()

    # Each link at specific time
    process = batch.groupBy("time", "link").agg(
        expr("count(*) as vcount"),     # Aggregate using two operations, the count of vehicles
        expr("avg(speed) as vspeed")    # And their average speed
    )
    process.show()

    # Saving the unprocessed data in the "raw_data" collection using the save() mode
    process.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", uri) \
        .option("database", dbName) \
        .option("collection", "processed_data") \
        .save()

def raw_mongo_pipeline(batch_df):
    # Saving the unprocessed data in the "raw_data" collection using the save() mode
    batch_df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", uri) \
        .option("database", dbName) \
        .option("collection", "raw_data") \
        .save()

def send_batch(batch_df,batch_id):
    # Raw data to MongoDB
    raw_mongo_pipeline(batch_df)
    # Processed data to MongoDB
    processed_df(batch_df)


if __name__ == "__main__":
    create_database()
    # Initializing Spark session
    try:
        spark_session = SparkSession\
            .builder\
            .appName("MySparkSession") \
            .config('spark.jars.packages',
                    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .config("spark.mongodb.output.uri", uri+"/"+dbName) \
            .getOrCreate()

        # Reduce screen clutter
        spark_session.sparkContext.setLogLevel("ERROR")
    except Exception as e:
        print(e)

    # Setting parameters for the Spark session to read from Kafka
    bootstrapServers = "localhost:9092"
    topics = "vehicle_positions"

    # Streaming data from Kafka topic as a dataframe
    spark_df = None
    try:
        spark_df = spark_session.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', bootstrapServers) \
            .option('subscribe', topics) \
            .option('startingOffsets', 'earliest') \
            .load()
    except Exception as e:
        print('Error connecting to Kafka')
        print(e)

    # Defining a schema that identifies the relevant columns for the JSON data
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("destination", StringType(), True),
        StructField("time", StringType(), True),
        StructField("link", StringType(), True),
        StructField("position", FloatType(), True),
        StructField("spacing", FloatType(), True),
        StructField("speed", FloatType(), True)
    ])

    # Expression that reads in raw data from dataframe as a string
    raw_df = spark_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("raw_data")) \
        .select("raw_data.*")


    # Writing stream query and processing each batch dataframe in append mode
    query = raw_df\
        .writeStream\
        .outputMode("append")\
        .foreachBatch(send_batch) \
        .start()

    # Terminates the stream on abort
    query.awaitTermination()






