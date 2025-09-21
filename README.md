# Big Data Streaming Pipeline 

This repository contains the implementation of a **real-time big data pipeline** for vehicle traffic simulation and analysis.  
It integrates **Kafka**, **Spark**, and **MongoDB** to demonstrate the end-to-end workflow of producing, streaming, processing, and storing high-volume data.

## 📌 Features
- **Data Simulation**: Generate synthetic vehicle movement data using `simulation.py`.
- **Streaming with Kafka**: 
  - `kafka_producer.py` streams vehicle position data into a Kafka topic.
  - `kafka_consumer.py` consumes and displays messages in real time.
- **Real-Time Processing with Spark**: 
  - `spark_mongo_pipeline.py` processes data from Kafka:
    - Aggregates vehicle counts and average speeds per road link.
    - Writes both raw and processed data to MongoDB collections.
- **MongoDB Storage & Queries**:
  - Stores datasets in two collections: `raw_data` and `processed_data`.
  - `mongodb_queries.py` executes advanced analytics, such as:
    - Finding the road link with the fewest vehicles.
    - Finding the road link with the highest average speed.
    - Identifying the vehicle that traveled the longest distance in a given timeframe.

## 🛠️ Tech Stack
- **Python 3.12.3**
- **Apache Kafka** (with Zookeeper)
- **Apache Spark 3.5.2** + Hadoop3
- **MongoDB Atlas** (M0 Sandbox cluster)
- **Libraries**: `pandas`, `json`, `datetime`, `pymongo`, `kafka-python-ng`, `pyspark`

## 📂 Project Structure
- simulation.py # Generate vehicle movement simulation data
- kafka_producer.py # Kafka producer sending data to topic
- kafka_consumer.py # Kafka consumer displaying real-time messages
- spark_mongo_pipeline.py # Spark pipeline: stream processing + MongoDB storage
- mongodb_queries.py # MongoDB queries for analytics


## 🚀 Setup Instructions

### 1️⃣ Prerequisites
- Install **Python 3.12.3** and required packages:
```bash
  pip install pandas kafka-python-ng pyspark pymongo
```

### 2️⃣ Run the Simulation
Generate synthetic traffic data:
```bash
python simulation.py
```

### 3️⃣ Start Kafka
Start Zookeeper and Kafka server, then create a topic:
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --topic vehicle_positions --bootstrap-server localhost:9092
```

### 4️⃣ Run Producer & Consumer
In separate terminals:
```bash
python kafka_producer.py
python kafka_consumer.py
```

### 5️⃣ Run Spark Streaming & MongoDB Pipeline
Process data in real-time and store results in MongoDB:
```bash
python spark_mongo_pipeline.py
```

### 6️⃣ Run MongoDB Queries
Execute predefined analytics:
```bash
python mongodb_queries.py
```

## 📊 Example Queries
- Find road link with least vehicles in a timeframe
- Find road link with highest average speed in a timeframe
- Find vehicle that traveled the longest distance

## 📖 References
- [Apache Kafka Quickstart](https://kafka.apache.org/quickstart)
- [Apache Kafka with Python](https://dev.to/hesbon/apache-kafka-with-python-laa)
- [Working with JSON in Apache Spark](https://medium.com/expedia-group-tech/working-with-json-in-apache-spark-1ecf553c2a8c)
- [Getting started with MongoDB, PySpark, and Jupyter Notebook](https://www.mongodb.com/company/blog/getting-started-with-mongodb-pyspark-and-jupyter-notebook)

## ✨ Authors
- Βασίλειος Αλεξόπουλος (ΑΜ: 1084625)
- Νικόλαος Βούλγαρης (ΑΜ: 1084626)
