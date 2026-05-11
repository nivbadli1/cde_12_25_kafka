# Apache Kafka — Data Engineering Course

קורס Data Engineering | נושא: Apache Kafka

תיקייה זו מכילה חומרי לימוד, תרגילים ופתרונות לנושא Kafka.

---

## 📁 מבנה התיקיות

### `01_kafka_basics/`
**דוגמאות בסיס — Producer & Consumer**

היחידה הראשונה מציגה את ה-API הבסיסי של `kafka-python`:
- **`kafka_producer.ipynb`** — שני producers: Basic producer + File-monitoring producer (קריאה מקובץ log חי ושידורו ל-Kafka)
- **`kafka_consumer.ipynb`** — שני consumers: Basic consumer + File-to-file consumer (pipeline מלא Kafka → sink file)

---

### `02_fraud_detection/`
**תרגיל Intermediate — Real-Time Fraud Detection Pipeline**

בניית pipeline לזיהוי הונאה בזמן אמת עבור מערכת בנקאית.

```
[Producer] → [Kafka: transactions] → [Consumer] → [Kafka: alerts]
                                                 → [Kafka: account-stats]
                                                 → alerts.log
```

- **`exercise.md`** — הוראות התרגיל המלאות (100 נקודות)
- **`solution_producer.ipynb`** — מייצר transactions אקראיות עם enrichment מלא
- **`solution_consumer.ipynb`** — normalization, risk classification, velocity detection, multi-topic output

**נושאים מרכזיים:**
- Currency normalization (ILS / USD / EUR)
- Risk classification (LOW / MEDIUM / HIGH / CRITICAL)
- Account masking
- Velocity fraud detection עם `collections.deque`
- כתיבה ל-2 טופיקים ו-log file במקביל

---

### `03_multi_api_pipeline/`
**תרגיל Beginner-Intermediate — Multi-Topic Pipeline with Live APIs**

חיבור של 2 APIs חינמיים ל-Kafka — ללא צורך ב-API key.

```
[Open-Meteo API]  →  weather_producer  →  [Kafka: weather]  →  weather_consumer
[CoinGecko API]   →  crypto_producer   →  [Kafka: crypto]   →  crypto_consumer
```

- **`exercise.md`** — הוראות התרגיל (4 חלקים)
- **`api_solution_producer.ipynb`** — 2 producers: מזג אוויר (כל 30 שניות) + קריפטו (כל 60 שניות)
- **`api_solution_consumer.ipynb`** — 2 consumers: weather formatter + crypto price-change alerts

**נושאים מרכזיים:**
- עבודה עם REST APIs בתוך producer
- Message keying לשמירת סדר per-city / per-coin
- Price change tracking עם `prev_prices` dict
- Alert threshold detection (≥2%)

---

## 🚀 סדר הרצה

| שלב | מה | טרמינל |
|---|---|---|
| 1 | הרצת Kafka broker | — |
| 2 | הרצת consumer/s | Terminal 1 |
| 3 | הרצת producer/s | Terminal 2 |

## 🛠 התקנת Dependencies

```bash
pip install kafka-python requests
```
