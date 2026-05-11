# Exercise — Multi-Topic Kafka Pipeline with Live APIs

**Course:** Data Engineering | **Topic:** Apache Kafka  
**Level:** Beginner–Intermediate

---

## Overview

In this exercise you will build a pipeline that pulls **real data from two free public APIs**
and streams it through Kafka — no API key required for either one.

```
[Open-Meteo API]  →  weather_producer  →  [Kafka: weather]  →  weather_consumer
[CoinGecko API]   →  crypto_producer   →  [Kafka: crypto]   →  crypto_consumer
```

### APIs used

| API | What it provides | Docs |
|---|---|---|
| **Open-Meteo** | Real-time weather for any coordinates — completely free, no key | https://open-meteo.com |
| **CoinGecko** | Live crypto prices — free tier, no key required | https://www.coingecko.com/api |

---

## Part 1 — Weather Producer → `weather` topic

### What to fetch

Call the Open-Meteo API for **each city** in the list below and send one message per city per poll cycle.

```python
CITIES = [
    {"name": "Tel Aviv",  "lat": 32.07,  "lon": 34.78},
    {"name": "London",    "lat": 51.51,  "lon": -0.13},
    {"name": "New York",  "lat": 40.71,  "lon": -74.01},
]
```

API endpoint (no key needed):
```
GET https://api.open-meteo.com/v1/forecast
    ?latitude=32.07
    &longitude=34.78
    &current_weather=true
    &wind_speed_unit=kmh
```

Use this weather code lookup table to convert `weathercode` to a readable string:
```python
WEATHER_CODES = {
    0:  "Clear sky",
    1:  "Mainly clear",
    2:  "Partly cloudy",
    3:  "Overcast",
    61: "Rain",
    80: "Showers"
}
```

### Message format to send

```python
{
    "city":        "Tel Aviv",
    "latitude":    32.07,
    "longitude":   34.78,
    "temperature": 28.4,        # degrees Celsius
    "windspeed":   14.2,        # km/h
    "condition":   "Clear sky", # from WEATHER_CODES lookup
    "timestamp":   "2024-01-15 10:32:11"
}
```

### Requirements

- Use `city` name as the message **key** (as bytes)
- Poll all 3 cities, then **sleep 30 seconds**, then repeat
- Use `acks=1` and `retries=3`
- Call `flush()` after each send

---

## Part 2 — Crypto Producer → `crypto` topic

### What to fetch

Call the CoinGecko API once per cycle to get prices for 3 coins.

```python
COINS = ["bitcoin", "ethereum", "solana"]
```

API endpoint (no key needed):
```
GET https://api.coingecko.com/api/v3/simple/price
    ?ids=bitcoin,ethereum,solana
    &vs_currencies=usd
```

The response looks like:
```json
{
    "bitcoin":  {"usd": 67450.12},
    "ethereum": {"usd": 3521.88},
    "solana":   {"usd": 158.34}
}
```

### Message format to send

Send **one message per coin**:

```python
{
    "coin":      "bitcoin",
    "price_usd": 67450.12,
    "timestamp": "2024-01-15 10:32:11"
}
```

### Requirements

- Use `coin` name as the message **key** (as bytes)
- After sending all 3 coins, **sleep 60 seconds**, then repeat
- Use `acks=1` and `retries=3`
- Call `flush()` after each send

---

## Part 3 — Weather Consumer

Read from the `weather` topic and print one formatted line per message:

```
📍 Tel Aviv     | 🌡  28.4°C | 💨 14.2 km/h | Clear sky
📍 London       | 🌡  17.1°C | 💨 22.8 km/h | Overcast
📍 New York     | 🌡  21.9°C | 💨  9.4 km/h | Partly cloudy
```

### Requirements

- `group_id = 'weather-monitor'`
- Deserialize each message from JSON bytes
- Use f-string formatting to align columns cleanly

---

## Part 4 — Crypto Consumer

Read from the `crypto` topic, print prices, and detect significant price changes.

### Keep track of the last seen price

For each coin, store the **previous price** in a dict. Compare the new price to the previous one and calculate the percentage change:

```python
change_pct = ((new_price - prev_price) / prev_price) * 100
```

### Output format

```
📈 bitcoin    | $   67,450.12 | change: +3.21%
📉 ethereum   | $    3,521.88 | change: -1.45%
📈 solana     | $      158.34 | change: +0.82%
```

Use `📈` when the change is positive and `📉` when negative.

### Price alert

If `abs(change_pct) >= 2.0` — print a price alert:

```
⚠️  PRICE ALERT: bitcoin moved +3.21% since last message!
```

### Requirements

- `group_id = 'crypto-monitor'`
- Initialize `prev_prices = {}` before the loop
- After printing, update `prev_prices[coin] = new_price`
- Skip the change calculation on the **first message** for each coin
  (when `coin` is not yet in `prev_prices`)

> 💡 Hint: Use `if coin not in prev_prices` to detect the first message for each coin.

---

## Tips

| Challenge | Hint |
|---|---|
| Key must be bytes | `city_name.encode('utf-8')` |
| Deserialize on consumer | `json.loads(message.value.decode('utf-8'))` |
| First message has no prev price | `if coin not in prev_prices: prev_prices[coin] = price; continue` |
| `requests` not installed | `pip install requests kafka-python` |
| API returns unexpected format | Always `print(response.status_code)` before `.json()` to debug |

---

## Run Order

| Step | What | Terminal |
|---|---|---|
| 1 | Start Kafka broker | — |
| 2 | Run `weather_consumer` | Terminal 1 |
| 3 | Run `crypto_consumer` | Terminal 2 |
| 4 | Run `weather_producer` | Terminal 3 |
| 5 | Run `crypto_producer` | Terminal 4 |

---

