# Realtime Data Streaming With TCP Socket, Apache Spark, OpenAI LLM, Kafka and Elasticsearch | End-to-End Data Engineering Project

## Table of Contents
- [Introduction](#introduction)
- [System Architecture](#system-architecture)
- [What I worked on](#what-i-worked-on)
- [Technologies](#technologies)
- [Architectural Decisions & Trade-offs](#architectural-decisions-&-Trade-offs)
- [Getting Started](#getting-started)


## Introduction

This project is a detailed guide for building an end-to-end data engineering pipeline using TCP/IP Socket, Apache Spark, OpenAI LLM, Kafka, and Elasticsearch. It explains every stage from data acquisition, processing, sentiment analysis with ChatGPT, production to Kafka topic, and connection to elasticsearch.

## System Architecture
![System_architecture.png](assets%2FSystem_architecture.png)

The project is designed with the following components:

- **Data Source**: I have used `yelp.com` dataset for the pipeline.
- **TCP/IP Socket**: Used for streaming data over the network in chunks
- **Apache Spark**: To process data with its master and worker nodes.
- **Confluent Kafka**: Cluster on the cloud
- **Control Center and Schema Registry**: Helps in monitoring and schema management of the Kafka streams.
- **Kafka Connect**: For elasticsearch connection
- **Elasticsearch**: For indexing and querying

## What I worked on

- Setting up data pipeline with TCP/IP 
- Real-time data streaming with Apache Kafka
- Data processing techniques with Apache Spark
- Realtime sentiment analysis with OpenAI ChatGPT
- Synchronising data from kafka to elasticsearch
- Indexing and Querying data on elasticsearch

## Technologies

- Apache Spark
- Confluent Kafka
- Docker
- Elasticsearch
- Python
- TCP/IP

## Architectural Decisions & Trade-offs

This pipeline was designed to simulate a real-world streaming environment while addressing specific engineering challenges regarding latency and data integrity. Below are the key design choices:

### 1. Ingestion Strategy (TCP Socket vs. Kafka Source)
* **Current Implementation:** The pipeline currently utilizes a TCP/IP socket connection to stream Yelp data into Apache Spark. This approach was chosen to simplify the simulation of a live data feed without the overhead of maintaining an external producer service.
* **Production Consideration:** In a production environment, I would decouple the ingestion layer by placing a **Kafka Producer** at the source (before Spark). This would ensure data durability and replayability in the event of a Spark cluster failure, preventing data loss during downtime.

### 2. Handling API Latency (OpenAI Integration)
* **Challenge:** Integrating an external API (OpenAI GPT) within a high-throughput Spark streaming job introduces significant latency risks due to network I/O and rate limits.
* **Optimization:** To mitigate backpressure, the system utilizes Spark's micro-batch architecture. By tuning the batch interval, we balance the need for "real-time" sentiment analysis against the blocking nature of synchronous API calls.

### 3. Decoupling Storage via Kafka Connect
* **Design Choice:** Rather than writing directly from Spark to Elasticsearch, processed data is written back to a Kafka topic, and a **Kafka Sink Connector** handles the ingestion into Elasticsearch.
* **Benefit:** This creates a resilient architecture. If the Elasticsearch cluster undergoes maintenance or fails, the data persists in the Kafka topic (based on retention policies) and automatically resumes syncing once the sink is restored, ensuring zero data loss.

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/ManojGowda27/Realtime_Data_Streaming.git
    ```

2. Navigate to the project directory:
    ```bash
    cd src
    ```

3. Run Docker Compose to spin up the spark cluster:
    ```bash
    docker-compose up
    ```

