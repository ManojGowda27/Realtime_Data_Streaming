# Real-Time Sentiment Analysis Pipeline | Apache Spark, Kafka, OpenAI & Elasticsearch
This project demonstrates a production-grade data engineering pipeline designed for the real-time streaming, processing, and AI-driven sentiment analysis of Yelp reviews. Built using Apache Spark Structured Streaming, Kafka, and Docker, this system is engineered for resilience and scalability. It is important to note that this pipeline has evolved significantly from an initial TCP-based prototype into its current fault-tolerant Kafka streaming architecture.

## Table of Contents
- [Introduction](#introduction)
- [System Architecture](#system-architecture)
- [Key Features](#key-features)
- [Technologies](#technologies)
- [Architectural Decisions & Evolution](#architectural-decisions--evolution)
- [Getting Started](#getting-started)

## Introduction

This project demonstrates a production-grade data engineering pipeline for real-time sentiment analysis. It ingests high-volume Yelp review data, processes it via **Apache Spark Structured Streaming**, enriches it using **OpenAI's GPT model** for sentiment classification, and indexes the results in **Elasticsearch** for low-latency querying.

The system is fully containerized using Docker and is designed to handle backpressure, data durability, and fault tolerance.

## System Architecture

![Initial_architecture.png](assets%2FInitial_architecture.png)

![Updated_architecture.png](assets%2FUpdated_architecture.png)


The pipeline consists of four distinct stages:

1.  **Ingestion Layer**: A Python-based **Kafka Producer** reads the Yelp dataset and pushes records to a `customers_review` Kafka topic. This decouples the data source from the processing layer, ensuring replayability.
2.  **Processing Layer**: **Apache Spark** (Master/Worker cluster) consumes the Kafka stream. It performs schema enforcement, watermark handling for late data, and transformation.
3.  **Enrichment Layer**: Spark makes asynchronous calls to the **OpenAI API** (GPT-3.5) to classify review sentiment (Positive/Negative/Neutral) in real-time.
4.  **Storage & Serving**: Enriched data is written back to a separate Kafka topic (`customers_review_enriched`), where a **Kafka Connect** sink synchronizes it with **Elasticsearch** for analytics.

## Key Features

* **Fault-Tolerant Ingestion**: Migrated from legacy TCP Sockets to **Apache Kafka** to ensure zero data loss during consumer downtime.
* **Distributed Processing**: Utilizes a custom-built Dockerized Spark Cluster (Master + Worker nodes) for scalable stream processing.
* **Smart AI Integration**: Implements filtering logic (e.g., processing specific star ratings) to optimize OpenAI API costs and reduce latency.
* **Schema Enforcement**: strict StructType definitions and Watermarking to handle out-of-order data and manage cluster memory efficiently.
* **Infrastructure as Code**: Entire stack (Zookeeper, Broker, Spark, Connectors) defined via `docker-compose`.

## Technologies

* **Language**: Python 3.9
* **Stream Processing**: Apache Spark Structured Streaming 3.5.0
* **Message Broker**: Apache Kafka (Confluent Platform)
* **AI/LLM**: OpenAI GPT-3.5 Turbo
* **Search Engine**: Elasticsearch
* **Containerization**: Docker & Docker Compose

## Architectural Decisions & Evolution

This pipeline has evolved from a TCP-based prototype to a resilient, distributed streaming architecture. Below are the key engineering decisions:

### 1. Ingestion Strategy: Moving from Socket to Kafka
* **Legacy Approach (MVP):** Initially utilized a raw TCP socket connection to stream JSON chunks directly into Spark. While simple, this created a tight coupling where any Spark downtime resulted in permanent data loss.
* **Production Implementation:** Migrated to **Apache Kafka**. By placing a durable buffer between the source and the processing engine, we achieved **decoupling** and **replayability**. We can now restart the Spark cluster without losing a single review.

### 2. Handling Late Data & Memory Management
* **Challenge:** In streaming, data often arrives out of order. Keeping state indefinitely leads to Out-Of-Memory (OOM) errors.
* **Solution:** Implemented **Watermarking** (10-minute threshold) on the event timestamp. This allows the engine to drop data that is too old to be relevant, keeping the state store small and efficient.

### 3. API Latency Management
* **Challenge:** Synchronous calls to OpenAI can block Spark executors, killing throughput.
* **Optimization:** We utilize Spark's micro-batch architecture to batch API requests. Additionally, logic was added to filter high-value records (e.g., 1-star reviews) *before* the API call, significantly reducing cost and latency overhead.

## Getting Started

### Prerequisites
* Docker Desktop (4GB+ RAM recommended)
* OpenAI API Key

### Installation

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/ManojGowda27/Realtime_Data_Streaming.git](https://github.com/ManojGowda27/Realtime_Data_Streaming.git)
    cd Realtime_Data_Streaming
    ```

2.  **Configure API Keys:**
    * Rename `config/config.example.py` to `config/config.py`.
    * Add your OpenAI API key inside the file.

3.  **Start the Infrastructure:**
    ```bash
    docker-compose up -d --build
    ```
    *This will start Zookeeper, Kafka, Spark Master, and Spark Worker.*

4.  **Start the Data Producer:**
    ```bash
    python jobs/kafka_producer.py
    ```

5.  **Submit the Spark Job:**
    ```bash
    docker exec -u 0 -it spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      /opt/spark/jobs/spark-streaming.py
    ```