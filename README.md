# Big Data Streaming Pipeline

This repository contains the implementation of a **real-time big data pipeline** for vehicle traffic simulation and analysis. 

It integrates **Apache Kafka**, **Apache Spark**, and **MongoDB** to demonstrate an end-to-end workflow of producing, streaming, processing, and storing high-volume data in a fully containerized environment.

This repository builds upon the foundational requirements of a [Big Data Management Systems academic assignment](./docs/project_description.docx), assigned during the Spring 2023-24 semester. Since then, the repository has been significantly refactored with DevOps practices, including Docker containers, automated GitHub Actions CI testing, and structural modularity.



## Architecture

The entire pipeline has been containerized using **Docker Compose** with a focus on modularity and resilience. It consists of 5 isolated microservices:
1. **Zookeeper:** Manages the Kafka cluster.
2. **Kafka Broker:** Handles the real-time data streaming (`vehicle_positions` topic).
3. **MongoDB:** A local NoSQL database with persistent volume storage to hold the raw and processed data.
4. **Kafka Producer Worker:** A lightweight Python container (built via `Dockerfile.producer`) that runs `simulation.py` to generate traffic data, then executes `kafka_producer.py` to stream it to Kafka.
5. **Spark Processor Worker:** A Python/Java container (built via `Dockerfile.spark`) that runs `spark_mongo_pipeline.py`. It consumes the stream, aggregates vehicle counts and speeds, and writes the results to MongoDB.

*Note: The deployment utilizes native Docker health checks to ensure the database and message brokers are fully operational before the producer and processor workers begin execution.*

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
Docker will download the necessary images, build the specific environments for both the Producer and Processor, and orchestrate the containers. The workers will automatically wait for Kafka and MongoDB to be fully initialized before streaming begins.

## Viewing the Data

Because the local MongoDB container maps to port 27017, you can view the live data streaming into your database using any local GUI:

1. Open MongoDB Compass or any Database Tool of your preference.
2. Connect using the URI: `mongodb://localhost:27017/`
3. Open the `vehicle_data` database to see the live `raw_data` and `processed_data` collections updating in real-time.

## Running Analytics Queries

Once the pipeline has processed some data, you can run the analytical queries to find:
1. The road link with the least vehicles.
2. The road link with the highest average speed.
3. The vehicle that traveled the longest distance.

To execute the query script inside the running Spark container, use your local terminal:
```bash
docker-compose exec spark-processor python mongodb_queries.py
```

## Continuous Integration

This repository utilizes **GitHub Actions** for automated testing. On every push or pull request to the `main` branch, a CI workflow automatically spins up the entire multi-container infrastructure using Docker Compose. It uses native Docker health checks to verify that Kafka and MongoDB are fully initialized and healthy. Finally, it ensures the Python and Spark environments boot successfully and connect to the broker and database without crashing.

## Authors
- Βασίλειος Αλεξόπουλος (ΑΜ: 1084625)
- Νικόλαος Βούλγαρης (ΑΜ: 1084626)