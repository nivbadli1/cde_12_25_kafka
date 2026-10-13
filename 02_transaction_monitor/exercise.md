# Exercise — Real-Time Transaction Monitor

---

## Background
You are building a simple real-time transaction monitoring system.
A data generator sends banking transactions to Kafka, and a consumer filters, reports, and tracks suspicious activity.

---

## Message Structure

```python
{
    "account_id": "ACC_3",
    "amount": 1540.75,
    "city": "Tel Aviv",
    "timestamp": "2024-01-15 10:32:11"
}
```

---

## Producer Task

1. Create a **generator** that produces random transactions:
   - `account_id` — randomly chosen from 5 accounts (`ACC_1` to `ACC_5`)
   - `amount` — random float between 10 and 5000
   - `city` — randomly chosen from a list of at least 4 cities
   - `timestamp` — current datetime as a formatted string

2. Send each transaction to a topic named `transactions`

3. Use `account_id` as the message **key**

4. Configure the producer with `acks=1` and `retries=3`

5. Add a random delay of **1–3 seconds** between messages to simulate a real stream

> 💡 **Hint:** The key must be sent as `bytes`, just like the value

---

## Consumer Task

### Part A — Basic Consumption & Alerting
1. Consume messages from the `transactions` topic with `group_id='monitors'`

2. For each message:
   - **Deserialize** the JSON payload
   - If the amount is **above 1000** — print:
     ```
     🚨 ALERT | ACC_3 | 1540.75 ILS | Tel Aviv
     ```
   - Otherwise — print:
     ```
     ✅ OK | ACC_1 | 250.00 ILS
     ```

3. Write **only alerts** to a file called `alerts.log`, one per line, and call `f.flush()` after every write

---

### Part B — Session Statistics
Track the following stats **in memory** across the entire consumer session, and print a summary **every 10 messages**:

1. **Per-account transaction count** — how many transactions each `account_id` has sent so far

2. **Per-account total amount** — the running sum of all amounts per account

3. **Alert rate** — what percentage of all messages so far were alerts

Print the summary in a clean, readable format, for example:
```
========== Summary (after 10 messages) ==========
ACC_1 | 3 transactions | total: 4821.50 ILS
ACC_2 | 2 transactions | total:  980.00 ILS
ACC_3 | 5 transactions | total: 7654.25 ILS
Alert rate: 40.0%  (4 out of 10)
=================================================
```
