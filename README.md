# Apache Kafka — Data Engineering Course

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

### 02_fraud_detection
Intermediate Exercise — Real-Time Fraud Detection Pipeline

Build a real-time fraud detection pipeline for a banking system.

```
[Producer] --> [Kafka: transactions] --> [Consumer] --> [Kafka: alerts]
                                                    --> [Kafka: account-stats]
                                                    --> alerts.log
```

- exercise.md
- solution_producer.ipynb — Generates random enriched transactions
- solution_consumer.ipynb — Normalizes currency, classifies risk, detects velocity fraud, writes to multiple outputs

Key topics:
- Currency normalization (ILS / USD / EUR)
- Risk classification (LOW / MEDIUM / HIGH / CRITICAL)
- Account ID masking
- Velocity fraud detection using collections.deque
- Writing to 2 output topics and a log file simultaneously

---

### 03_multi_api_pipeline
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

## Run Order

| Step | Action | Terminal |
|---|---|---|
| 1 | Start Kafka broker | — |
| 2 | Run consumer(s) | Terminal 1 |
| 3 | Run producer(s) | Terminal 2 |

## Dependencies

```bash
pip install kafka-python requests
```
