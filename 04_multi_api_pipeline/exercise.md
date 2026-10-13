# Exercise — Multi-Topic Kafka Pipeline with Live APIs

**Course:** Data Engineering | **Topic:** Apache Kafka  
**Level:** Beginner–Intermediate

---

## Overview

In this exercise you will build a pipeline that pulls **real data from two free public APIs**
and streams it through Kafka — no API key required for either one.

```
[Open-Meteo API]  →  weather_producer  →  [Kafka: weather]  →  weather_consumer
[Vélib API]       →  bikes_producer    →  [Kafka: bikes]    →  bikes_consumer
```

### APIs used

| API | What it provides | Docs |
|---|---|---|
| **Open-Meteo** | Real-time weather for any coordinates — completely free, no key | https://open-meteo.com |
| **Vélib (Paris bike-share)** | Live number of bikes and free docks at every station — open data, no key | https://www.velib-metropole.fr/donnees-open-data-gbfs-du-service-velib-metropole |

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

## Part 2 — Bike-Share Producer → `bikes` topic

### The business story

A bike-share company has hundreds of stations. When a station runs out of bikes, customers can't rent;
when it is full, customers can't return. An operations team moves bikes between stations with trucks,
so they need a **live stream of station status**. You will build that stream.

### What to fetch

Two endpoints (no key needed):

```
# 1) Station information — names and capacity. Fetch ONCE, before the loop.
GET https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json

# 2) Station status — live bikes / free docks. Fetch on EVERY cycle.
GET https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json
```

Both responses have the same shape — a list under `data.stations`:

```json
// station_information.json
{"data": {"stations": [
    {"station_id": 213688169, "name": "Benjamin Godard - Victor Hugo", "capacity": 35, ...},
    ...
]}}

// station_status.json
{"data": {"stations": [
    {"station_id": 213688169, "num_bikes_available": 4, "num_docks_available": 31, ...},
    ...
]}}
```

### Steps

1. Before the loop: fetch station information and build a lookup dict for the **first 5 stations**:
   ```python
   info = requests.get(INFO_URL, timeout=10).json()["data"]["stations"][:5]
   stations = {s["station_id"]: {"name": s["name"], "capacity": s["capacity"]} for s in info}
   ```
2. In the loop: fetch station status, and for every station whose `station_id` is in `stations`,
   send one message.

### Message format to send

```python
{
    "station_id":      213688169,
    "name":            "Benjamin Godard - Victor Hugo",
    "capacity":        35,
    "bikes_available": 4,
    "docks_available": 31,
    "timestamp":       "2024-01-15 10:32:11"
}
```

### Requirements

- Use `station_id` as the message **key** (convert to string, then to bytes: `str(station_id).encode('utf-8')`)
- After sending all 5 stations, **sleep 60 seconds**, then repeat
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

## Part 4 — Bikes Consumer

Read from the `bikes` topic, print the station status, and alert the operations team.

### Output format

```
🚲 Benjamin Godard - Victor Hugo  |  4/35 bikes ( 11%) | change: -2
🚲 Cassini - Denfert-Rochereau    | 18/25 bikes ( 72%) | change: +1
```

- `fill %` = `bikes_available / capacity * 100`
- `change` = bikes now minus bikes in the **previous message for the same station**

### Keep track of the last seen value

Store the previous `bikes_available` per station in a dict, exactly like a running state:

```python
prev_bikes = {}          # { station_id: bikes_available }
change = bikes - prev_bikes[station_id]
```

On the **first message** for a station there is no previous value — print `change: new` instead.

### Operations alerts

| Condition | Print |
|---|---|
| `bikes_available <= 2` | `⚠️  LOW BIKES: <name> has only <n> bikes — send a truck with bikes!` |
| `docks_available <= 2` | `⚠️  STATION FULL: <name> has only <n> free docks — take bikes away!` |

### Requirements

- `group_id = 'bikes-monitor'`
- Initialize `prev_bikes = {}` before the loop
- Update `prev_bikes[station_id] = bikes` **after** printing

> 💡 Hint: Use `if station_id not in prev_bikes` to detect the first message for each station.

---

## Tips

| Challenge | Hint |
|---|---|
| Key must be bytes | `city_name.encode('utf-8')` |
| Deserialize on consumer | `json.loads(message.value.decode('utf-8'))` |
| First message has no previous value | `if station_id not in prev_bikes:` → print `change: new` |
| `requests` not installed | `pip install requests kafka-python` |
| API returns unexpected format | Always `print(response.status_code)` before `.json()` to debug |

---

## Run Order

| Step | What | Terminal |
|---|---|---|
| 1 | Start Kafka broker | — |
| 2 | Run `weather_consumer` | Terminal 1 |
| 3 | Run `bikes_consumer` | Terminal 2 |
| 4 | Run `weather_producer` | Terminal 3 |
| 5 | Run `bikes_producer` | Terminal 4 |

---

