# Kafka Streaming Pipeline

A simple real-time data engineering project that demonstrates how to stream data from Kafka using Spark Structured Streaming and store the processed data in Delta Lake.

## Tech Stack

- Python
- Apache Kafka
- Apache Spark Structured Streaming
- Delta Lake
- Docker
- Faker

## Project Structure

```
kafka-streaming-project/
│
├── producer/
│   └── producer.py
│
├── spark/
│   └── stream.py
│
├── data/
│   ├── checkpoints/
│   └── orders/
│
├── docker-compose.yml
├── docker-compose-spark.yml
├── requirements.txt
├── .gitignore
└── README.md
```

## Pipeline

```
Python Producer
       │
       ▼
Apache Kafka
       │
       ▼
Spark Structured Streaming
       │
       ▼
Delta Lake
```

## Features

- Generates random order events
- Publishes events to Kafka
- Reads Kafka events using Spark Structured Streaming
- Parses JSON records
- Stores streaming data in Delta Lake
- Uses checkpointing for fault tolerance

## Getting Started

### Clone the repository

```bash
git clone git@github.com:Eswar-45/Kafka-streaming-project.git
cd Kafka-streaming-project
```

### Create a virtual environment

```bash
python -m venv venv
```

### Activate it

**Windows**

```bash
venv\Scripts\activate
```

**Linux/macOS**

```bash
source venv/bin/activate
```

### Install dependencies

```bash
pip install -r requirements.txt
```

### Start Kafka

```bash
docker compose up -d
```

### Start Spark

```bash
docker compose -f docker-compose-spark.yml up -d
```

### Submit the streaming job

```bash
docker exec -it spark bash

/opt/spark/bin/spark-submit \
--packages io.delta:delta-spark_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
/opt/spark-apps/stream.py
```

### Run the producer

```bash
cd producer

python producer.py
```

## Output

Streaming data is written to:

```
data/
├── checkpoints/
└── orders/
```

## Future Improvements

- Data validation
- Kafka Connect
- Schema Registry
- Dockerized Spark Cluster
- Databricks Integration
- Airflow Orchestration
- Monitoring & Alerting

## Author

**Eswar Khandavalli**

GitHub: https://github.com/Eswar-45