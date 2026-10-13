# Exercise — Real-Time Fraud Detection Pipeline

**Course:** Data Engineering | **Topic:** Apache Kafka  
**Level:** Intermediate

---

## Overview

You are building a **real-time fraud detection pipeline** for a banking system.

Transactions arrive from multiple accounts in different currencies. Your pipeline must enrich, normalize, classify, and monitor them — routing events to the right downstream systems as they happen.

This is a pattern used in production at companies like PayPal, Stripe, and Wix.

```
[Producer]                  [Kafka: transactions]         [Consumer]
  Generate transactions  →  ──────────────────────────►  Normalize currency
  Enrich with metadata                                    Classify risk level
  Serialize to JSON                                       Detect velocity fraud
  Send with account key                                        │
                                                               ├──► [Kafka: alerts]
                                                               │    (HIGH + CRITICAL only)
                                                               │
                                                               ├──► [Kafka: account-stats]
                                                               │    (summary every 10 msgs)
                                                               │
                                                               └──► alerts.log (file)
```

### Topics used in this exercise

| Topic | Written by | Read by | Purpose |
|---|---|---|---|
| `transactions` | Producer | Consumer | Raw transactions stream |
| `alerts` | Consumer | (downstream service) | Flagged transactions for notification system |
| `account-stats` | Consumer | (downstream dashboard) | Periodic account snapshots |

> In a real system, a separate **notification service** would consume `alerts` to send SMS/email,
> and a **dashboard service** would consume `account-stats` to update a live BI view.

---

## Message Structure

Each transaction the producer sends must include the following fields:

```python
{
    "account_id":        "ACC_3",
    "amount":            142.50,
    "currency":          "USD",         # ILS / USD / EUR
    "transaction_type":  "PURCHASE",    # TRANSFER / WITHDRAWAL / PURCHASE / DEPOSIT
    "merchant":          "Amazon",      # Only for PURCHASE, otherwise null
    "city":              "Tel Aviv",
    "timestamp":         "2024-01-15 10:32:11"
}
```

---

## Exchange Rates (hardcoded — no API needed)

```python
EXCHANGE_RATES = {
    "ILS": 1.0,
    "USD": 3.7,
    "EUR": 4.0
}
```

---

## Part 1 — Producer

### 1.1 — Data Generator

Create a generator function `transaction_generator()` that yields random transactions infinitely.

```python
ACCOUNTS          = ['ACC_1', 'ACC_2', 'ACC_3', 'ACC_4', 'ACC_5']
CITIES            = ['Tel Aviv', 'Jerusalem', 'Haifa', 'Beer Sheva', 'Eilat']
CURRENCIES        = ['ILS', 'USD', 'EUR']
TRANSACTION_TYPES = ['TRANSFER', 'WITHDRAWAL', 'PURCHASE', 'DEPOSIT']
MERCHANTS         = ['Amazon', 'Zara', 'Apple', 'IKEA', 'Spotify', 'Netflix']
```

Rules:
- `amount` — random float between **10 and 8000**, rounded to 2 decimal places
- `merchant` — only set if `transaction_type == 'PURCHASE'`, otherwise `None`
- `timestamp` — current datetime as a formatted string

### 1.2 — Producer Setup

Configure with:
- `acks = 1`
- `retries = 3`
- `compression_type = 'gzip'`

### 1.3 — Sending

- Serialize each transaction to JSON bytes
- Use `account_id` as the message **key** (as bytes)
- Call `flush()` after each send
- Add a **random delay of 1–3 seconds** between messages

---

## Part 2 — Consumer

The consumer reads from **`transactions`** and writes to **two Kafka topics** and **one file**.
You will need **two KafkaProducer instances** inside the consumer — one for `alerts`, one for `account-stats`.

### 2.1 — Deserialization & Normalization

For each incoming message:

1. Deserialize the JSON payload
2. **Normalize the amount to ILS:**
   ```python
   amount_ils = round(amount * EXCHANGE_RATES[currency], 2)
   ```
3. Add two fields to the enriched dict:
   - `amount_ils` — the normalized amount
   - `original` — formatted string, e.g. `"142.50 USD"`

### 2.2 — Risk Classification

Write a function `classify_risk(amount_ils)` that returns a risk level string:

| Amount (ILS) | Risk Level |
|---|---|
| Below 500 | `"LOW"` |
| 500 – 1,999 | `"MEDIUM"` |
| 2,000 – 4,999 | `"HIGH"` |
| 5,000 and above | `"CRITICAL"` |

### 2.3 — Account ID Masking

Write a function `mask_account(account_id)` that masks the numeric part:

```
"ACC_3"  →  "ACC_***"
"ACC_12" →  "ACC_***"
```

> 💡 Hint: use `.split('_')` to isolate the prefix, then concatenate `'_***'`

### 2.4 — Console Output

Print one line per message using this format:

```
🔵 LOW      | ACC_3 | 142.50 USD → 527.25 ILS  | PURCHASE @ Amazon    | Tel Aviv
🟡 MEDIUM   | ACC_1 | 350.00 ILS → 350.00 ILS  | TRANSFER             | Haifa
🔴 HIGH     | ACC_5 | 800.00 EUR → 3200.00 ILS  | WITHDRAWAL           | Jerusalem
⚫ CRITICAL  | ACC_2 | 2000.00 USD → 7400.00 ILS | DEPOSIT              | Eilat
```

```python
RISK_EMOJI = {"LOW": "🔵", "MEDIUM": "🟡", "HIGH": "🔴", "CRITICAL": "⚫"}
```

### 2.5 — Output Topic: `alerts`

For every **HIGH or CRITICAL** transaction, publish a message to the `alerts` topic.

The message value should be a JSON-serialized dict:

```python
{
    "account_id":       "ACC_***",          # ← masked!
    "amount_ils":       7400.00,
    "original":         "2000.00 USD",
    "risk":             "CRITICAL",
    "transaction_type": "DEPOSIT",
    "city":             "Eilat",
    "timestamp":        "2024-01-15 10:32:11"
}
```

Use `risk` as the **key** of the message (e.g. `b'CRITICAL'`).
This allows a downstream consumer to filter by risk level without reading the payload.

### 2.6 — Output Topic: `account-stats`

Every **10 messages**, publish a snapshot to the `account-stats` topic.

The message value should be a JSON-serialized dict:

```python
{
    "snapshot_at":     20,
    "timestamp":       "2024-01-15 10:35:00",
    "accounts": {
        "ACC_1": {
            "transactions": 5,
            "total_ils":    12500.00,
            "avg_ils":      2500.00,
            "top_type":     "PURCHASE"
        },
        ...
    },
    "risk_breakdown":  {"LOW": 3, "MEDIUM": 5, "HIGH": 7, "CRITICAL": 5},
    "velocity_alerts": 2
}
```

Use `"summary"` as the **key** of the message (as bytes).

> 💡 To find the most frequent transaction type per account, use `collections.Counter`
> and call `max(counter, key=counter.get)` to get the top key.

### 2.7 — Alert File

Write **only HIGH and CRITICAL** transactions to `alerts.log` using the **masked** account ID:

```
[2024-01-15 10:32:11] CRITICAL | ACC_*** | 2000.00 USD → 7400.00 ILS | DEPOSIT | Eilat
```

Call `f.flush()` after every write.

### 2.8 — Velocity Fraud Detection

Track the `account_id` of the **last 20 messages** using a `collections.deque`.
If the same account appears **4 or more times** in the window, print a velocity alert:

```
⚠️  VELOCITY ALERT | ACC_3 appeared 5 times in the last 20 transactions
```

> #### How to implement this — step by step:
>
> **Step 1 — Create the deque before the loop:**
> ```python
> from collections import deque
> recent_accounts = deque(maxlen=20)
> ```
> `maxlen=20` means it automatically drops the oldest item when a 21st item is added —
> you never need to remove elements manually. This is the standard Python pattern for a sliding window.
>
> **Step 2 — Append after every message:**
> ```python
> recent_accounts.append(acc)
> ```
>
> **Step 3 — Count occurrences in the current window:**
> ```python
> count_in_window = recent_accounts.count(acc)
> ```
> `deque.count(x)` works exactly like `list.count(x)`.
>
> **Step 4 — Fire the alert:**
> ```python
> if count_in_window >= 4:
>     print(f"⚠️  VELOCITY ALERT | {acc} appeared {count_in_window} times in the last 20 transactions")
> ```
>
> **Why `deque` and not a plain list?**
> With a list you would need to manually slice or pop old elements on every iteration.
> `deque(maxlen=N)` handles the sliding window automatically and is more memory-efficient.

---

## Tips & Common Mistakes

| Challenge | Hint |
|---|---|
| Key must be bytes | `account_id.encode('utf-8')` |
| Deserialize on consumer | `json.loads(message.value.decode('utf-8'))` |
| Two producers inside the consumer | Create `alerts_producer` and `stats_producer` separately before the loop |
| Most frequent type | `max(type_counter, key=type_counter.get)` |
| Flush the alert file | Don't forget — data loss is silent without it |
| `None` in JSON | `json.dumps` converts Python `None` to JSON `null` automatically |
