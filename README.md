# Apache Kafka — Cloud & Big Data Engineering Course

Course materials, exercises and solutions for the Apache Kafka (Python) module.
Work through the folders in order — each one builds on the previous.

```
01_kafka_basics          → course examples: first producer/consumer, then a File → Kafka → File pipeline
02_transaction_monitor   → warm-up exercise: one producer, one consumer, alerts + stats
03_fraud_detection       → main exercise: the consumer also produces to 2 downstream topics
04_multi_api_pipeline    → bonus: real data from 2 live public APIs (weather + bike-share)
```

Every script and notebook starts with a header that says **what to run before it**.
Long-running scripts never stop on their own — stop them with `Ctrl+C` (or the notebook ■ button).

## Dependencies

```bash
pip install kafka-python requests
```

The code connects to the broker at `course-kafka:9092` (the course Docker environment).

---

## 01_kafka_basics — Course Examples

The original course files, numbered in run order, with English comments.
Open a separate terminal for each script.

### Example 1 — Basic producer & consumer (`example_1_basic/`)

| Step | Terminal | Run | Why this order |
|---|---|---|---|
| 1 | 1 | `python 1_consumer.py` | A consumer without a group starts at the END of the topic — start it first so you don't miss messages |
| 2 | 2 | `python 2_producer.py` | Sends 2 messages and exits — watch them appear in terminal 1 |

### Example 2 — File → Kafka → File pipeline (`example_2_file_pipeline/`)

```
1_log_generator → srcFile.log → 3_producer → [kafka-tst-02] → 2_consumer → tarFile.log
```

One-time setup:

```bash
mkdir -p /home/developer/kafka/srcFiles /home/developer/kafka/sinkFiles
```

| Step | Terminal | Run | Why this order |
|---|---|---|---|
| 1 | 1 | `python 1_log_generator.py` | Creates `srcFile.log` — the producer needs the file to exist |
| 2 | 2 | `python 2_consumer.py` | Must be listening before the producer sends |
| 3 | 3 | `python 3_producer.py` | Tails the log file and sends new lines to Kafka |
| 4 | 4 | `python 4_check_file.py` | After ~30s — the lines from step 1 should appear in `tarFile.log` |

`notebooks/` contains the same two examples as Jupyter notebooks, for anyone who prefers Jupyter.

---

## 02_transaction_monitor — Warm-up Exercise

Flag transactions above 1000 ILS, write alerts to a file, print session stats every 10 messages.

```
[Producer] --> [Kafka: transactions] --> [Consumer] --> console + alerts.log
```

| Step | Run |
|---|---|
| 1 | `solution_consumer.ipynb` |
| 2 | `solution_producer.ipynb` |

Key topics: JSON serialization, bytes keys, consumer groups and offset commit, in-memory state.

---

## 03_fraud_detection — Main Exercise

Builds on 02: the consumer enriches each transaction and also acts as a producer, routing events to downstream topics.

```
[Producer] --> [Kafka: transactions] --> [Consumer] --> [Kafka: alerts]          (HIGH + CRITICAL)
                                                    --> [Kafka: account-stats]   (snapshot every 10 msgs)
                                                    --> alerts.log
```

| Step | Run |
|---|---|
| 1 | `solution_consumer.ipynb` |
| 2 | `solution_producer.ipynb` |

Key topics: currency normalization, risk classification, account masking, velocity fraud detection with `collections.deque`,
one consumer writing to 2 topics and a file.

> The producer simulates a fraudster: account `ACC_7` sends ~20% of all transactions,
> so the velocity alerts should point at it.

---

## 04_multi_api_pipeline — Bonus Exercise

Two producers pull real data from free public APIs (no API key), two consumers process it.

```
[Open-Meteo API] --> weather producer --> [Kafka: weather] --> weather consumer
[Vélib API]      --> bikes producer   --> [Kafka: bikes]   --> bikes consumer (operations alerts)
```

Each of the 4 loops runs forever and blocks its kernel, so run each one in its own notebook (or copy it to a `.py` file and use a separate terminal):

| Step | Run |
|---|---|
| 1 | Weather consumer (`api_solution_consumer.ipynb`, part 1) |
| 2 | Bikes consumer (`api_solution_consumer.ipynb`, part 2) |
| 3 | Weather producer (`api_solution_producer.ipynb`, part 1) |
| 4 | Bikes producer (`api_solution_producer.ipynb`, part 2) |

Key topics: calling a REST API inside a producer loop, enriching messages with a lookup dict,
keys for per-city / per-station ordering, tracking state with a `prev_*` dict, threshold alerts.
