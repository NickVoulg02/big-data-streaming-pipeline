# Big Data Streaming Pipeline

This repository contains the implementation of a **real-time big data pipeline** for vehicle traffic simulation and analysis. 

It integrates **Apache Kafka**, **Apache Spark**, and **MongoDB** to demonstrate an end-to-end workflow of producing, streaming, processing, and storing high-volume data in a fully containerized environment.

## Architecture

The entire pipeline has been containerized using **Docker Compose**. It consists of 5 isolated microservices:
1. **Zookeeper:** Manages the Kafka cluster.
2. **Kafka Broker:** Handles the real-time data streaming (`vehicle_positions` topic).
3. **MongoDB:** A local NoSQL database with persistent volume storage to hold the raw and processed data.
4. **Kafka Producer Worker:** A Python container that runs `simulation.py` to generate traffic data, then executes `kafka_producer.py` to stream it to Kafka.
5. **Spark Processor Worker:** A Python/Java container that runs `spark_mongo_pipeline.py`. It consumes the stream, aggregates vehicle counts and speeds, and writes the results to MongoDB.

## Prerequisites
* **Docker Desktop** (with WSL 2 enabled if running on Windows).

## Quickstart Guide

### 1. Clone the repository
```bash
git clone https://github.com/NickVoulg02/big-data-streaming-pipeline
cd big-data-streaming-pipeline
```

### 2. Set up the Environment Variables

Create your local environment file by copying the provided example:
```bash
cp .env.example .env
```

### 3. Launch the Pipeline

Start the entire automated pipeline with a single command:
```bash
docker-compose up -d --build
```
Docker will download the necessary images, build the Python/Spark environments, and orchestrate the containers. The Producer will wait 15 seconds for Kafka to initialize before it automatically starts simulating and streaming data.

## Viewing the Data

Because the local MongoDB container maps to port 27017, you can view the live data streaming into your database using any local GUI:

1. Open MongoDB Compass or any Database Tool of your preference.
2. Connect using the URI: mongodb://localhost:27017/
3. Open the vehicle_data database to see the live raw_data and processed_data collections updating in real-time.

## Running Analytics Queries

Once the pipeline has processed some data, you can run the analytical queries to find:
1. The road link with the least vehicles.
2. The road link with the highest average speed.
3. The vehicle that traveled the longest distance.

To execute the query script inside the running Spark container, use your local terminal:
```bash
docker-compose exec spark-processor python mongodb_queries.py
```

## Project Structure
```plaintext
.
├── docs/                     # Assignment descriptions and reports
├── docker-compose.yaml       # Container orchestration
├── Dockerfile                # Multi-stage build for Python & Spark environments
├── .env.example              # Template for environment variables
├── simulation.py             # Generates vehicle movement data (UXSIM)
├── kafka_producer.py         # Streams data to Kafka
├── spark_mongo_pipeline.py   # Processes the stream and writes to MongoDB
├── mongodb_queries.py        # Analytics queries
└── requirements.txt          # Python dependencies
```

## Authors
- Βασίλειος Αλεξόπουλος (ΑΜ: 1084625)
- Νικόλαος Βούλγαρης (ΑΜ: 1084626)