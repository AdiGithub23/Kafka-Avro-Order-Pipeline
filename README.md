# Kafka-Avro-Order-Pipeline
A Kafka-based system that produces and consumes order messages using Avro serialization. This includes real-time price aggregation, retry logic for temporary failures, and a Dead Letter Queue (DLQ) for permanently failed messages with Avro serialization, retry logic, and DLQ handling.

Simple Kafka system .

## Features
- √ Avro serialization for order messages
- √ Running average calculation
- √ Retry logic (max 3 attempts)
- √ Dead Letter Queue for permanent failures

## Setup

### 1. Start Kafka
```bash
docker-compose up -d
```

### 2. Install Dependencies
```bash
pip install uv
```

```bash
uv pip install confluent-kafka fastavro
```

### 3. Run Producer (Terminal 1)
```bash
python producer/producer.py
```

### 4. Run Consumer (Terminal 2)
```bash
python consumer/consumer.py
```

## How It Works

1. **Producer** sends random orders every 2 seconds
2. **Consumer** processes orders:
   - Orders with `price <= 1000`: Success
   - Orders with `price > 1000`: Fail → Retry
   - After 3 retries: Send to DLQ

3. **Running Average** is calculated for all successful orders
---

## View Kafka UI
Open: http://localhost:8080

These Topics must be created:
- `orders` - Main topic
- `orders_retry` - Retry queue
- `orders_dlq` - Dead letter queue