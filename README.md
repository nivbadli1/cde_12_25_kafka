# Apache Kafka — Cloud&Big Data Engineering Course

This repository contains course materials, exercises, and solutions for the Apache Kafka module.

---

## Folder Structure

### 01_kafka_basics
Core Producer & Consumer Examples

Introduces the basic kafka-python API:

- kafka_producer.ipynb — Two producer examples:
  - Basic producer: send hardcoded messages to a topic
  - File-monitoring producer: tail a live log file and stream its lines to Kafka
- kafka_consumer.ipynb — Two consumer examples:
  - Basic consumer: read messages and print metadata (offset, timestamp, value)
  - File-to-file consumer: full pipeline — Kafka topic to local sink file

---

### 02_transaction_monitor
Warm-up Exercise — Real-Time Transaction Monitor

A single producer and consumer: flag transactions above 1000 ILS, write alerts to a file, print session stats every 10 messages.

```
[Producer] --> [Kafka: transactions] --> [Consumer] --> console + alerts.log
```

- exercise.md
- solution_producer.ipynb — Generates random transactions (account_id as key)
- solution_consumer.ipynb — Threshold alerting, alerts.log with flush, per-account stats summary

Key topics:
- JSON serialization and bytes keys
- Consumer groups and auto offset commit
- In-memory session state

---

### 03_fraud_detection
Intermediate Exercise — Real-Time Fraud Detection Pipeline

Builds on 02: the consumer also acts as a producer and routes events to downstream topics.

```
[Producer] --> [Kafka: transactions] --> [Consumer] --> [Kafka: alerts]
                                                    --> [Kafka: account-stats]
                                                    --> alerts.log
```

- exercise.md
- solution_producer.ipynb — Generates random enriched transactions (currency, type, merchant)
- solution_consumer.ipynb — Normalizes currency, classifies risk, detects velocity fraud, writes to multiple outputs

Key topics:
- Currency normalization (ILS / USD / EUR)
- Risk classification (LOW / MEDIUM / HIGH / CRITICAL)
- Account ID masking
- Velocity fraud detection using collections.deque
- Writing to 2 output topics and a log file simultaneously

---

### 04_multi_api_pipeline
Beginner-Intermediate Exercise — Multi-Topic Pipeline with Live APIs

Connect two free public APIs to Kafka — no API key required for either.

```
[Open-Meteo API] --> weather_producer --> [Kafka: weather] --> weather_consumer
[CoinGecko API]  --> crypto_producer  --> [Kafka: crypto]  --> crypto_consumer
```

- exercise.md
- api_solution_producer.ipynb — Two producers: weather (every 30s) + crypto prices (every 60s)
- api_solution_consumer.ipynb — Two consumers: weather formatter + crypto price-change alert system

Key topics:
- Calling REST APIs from inside a producer loop
- Message keying for per-city / per-coin ordering
- Price change tracking with a prev_prices dict
- Alert threshold detection (>= 2% change)

---

## Dependencies

```bash
pip install kafka-python requests
```
