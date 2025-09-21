import json
import os
import time
from datetime import datetime
import pandas as pd
from kafka import KafkaProducer

# Specify Kafka cluster parameters
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TEST = os.environ.get("KAFKA_TOPIC_TEST", "vehicle_positions")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "7.3.1")

if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        api_version=KAFKA_API_VERSION,
        # Each line is a JSON string
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Read uxsim data
    df = pd.read_csv('output.csv')
    print(df.info())
    df.to_json("temp.json")

    df = df.rename(columns={
        'name': 'name',
        'orig': 'origin',
        'dest': 'destination',
        't': 'time',
        'link': 'link',
        'x': 'position',
        's': 'spacing',
        'v': 'speed'
    })

    # Start time
    start = datetime.now()

    # Add the t variable to the start time in 'time' column (convert seconds to timedelta)
    df['time'] = start + pd.to_timedelta(df['time'], unit='s')

    # Drop unnecessary columns
    df = df.drop(columns=['dn', 'Unnamed: 0'])

    # Format Date and Time values
    df['time'] = df['time'].dt.strftime("%d/%m/%Y %H:%M:%S")

    pd.set_option('display.max_columns', None)
    print(df.info())

    print(f'Producer started: {start}')

    N = 5  # Set your desired step size (N) here
    for secs in range(0, 3600, N):
        # Start time plus seconds passed
        temp = start + pd.to_timedelta(secs, unit='s')
        temp = temp.strftime("%d/%m/%Y %H:%M:%S")
        grouped_df = df[(df['time'] == temp) & (df['link'] != 'waiting_at_origin_node')]

        grouped_df.apply(lambda row: producer.send(KAFKA_TOPIC_TEST, json.loads(row.to_json())), axis=1)
        # Every 5 seconds
        time.sleep(N)



    producer.flush()
