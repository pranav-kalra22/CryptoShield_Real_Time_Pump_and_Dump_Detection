# 🛡️ Crypto-Shield — Complete Setup Guide

## File Overview
```
crypto-shield/
├── docker-compose.yml      ← Infrastructure (Kafka + Spark + MongoDB)
├── producer.py             ← Fake data generator (for testing)
├── producer_realtime.py    ← LIVE data: Reddit + CoinGecko
├── producer_replay.py      ← BIG DATA: Kaggle dataset replay
├── consumer.py             ← Spark Streaming + graph detection
├── dashboard.py            ← Streamlit live dashboard
└── requirements.txt
```

---

## Prerequisites
| Tool | Version | Install |
|------|---------|---------|
| Docker Desktop | latest | docker.com |
| Python | 3.9+ | python.org |
| Java JDK | **17 or 21** | adoptium.net |
| Apache Spark | 4.1.1 | spark.apache.org |

---

## Step 1 — Install Python packages
```bash
pip install -r requirements.txt
```

---

## Step 2 — Start Infrastructure
```bash
docker-compose up -d
docker ps   # verify all 6 containers are running
```

| Service       | URL                   |
|---------------|-----------------------|
| Kafka UI      | http://localhost:8080 |
| Spark Web UI  | http://localhost:8181 |
| MongoDB UI    | http://localhost:8082 |

---

## Step 3 — Choose Your Data Source

### Option A: Fake Data (simplest, for testing)
```bash
python producer.py          # normal traffic
python producer.py --pump   # inject a pump attack
```

### Option B: Real Live Data (best for demo)
1. Get Reddit API credentials at https://www.reddit.com/prefs/apps
2. Fill in `REDDIT_CLIENT_ID` and `REDDIT_CLIENT_SECRET` in `producer_realtime.py`
3. Run:
```bash
python producer_realtime.py
```
This streams live posts from r/CryptoMoonShots and real CoinGecko prices simultaneously.

### Option C: Kaggle Big Data Replay (best for proving scale)
```bash
# Download datasets
pip install kaggle
kaggle datasets download -d jarradbe/historical-crypto-pump-dumps -p data/ --unzip
kaggle datasets download -d alaix14/bitcoin-tweets-20160101-to-20190329 -p data/ --unzip

# Replay at 100x speed (millions of events)
python producer_replay.py --dataset all --speed 100

# Max throughput stress test
python producer_replay.py --dataset tweets --speed 0
```

---

## Step 4 — Start Spark Consumer
```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
    consumer.py
```

---

## Step 5 — View Dashboard
```bash
streamlit run dashboard.py
# Open http://localhost:8501
```

---

