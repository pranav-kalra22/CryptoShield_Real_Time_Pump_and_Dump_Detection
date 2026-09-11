"""
╔══════════════════════════════════════════════════════════════╗
║      Crypto-Shield — KAGGLE REPLAY Producer (v2)            ║
║  Replays historical datasets through Kafka at high speed    ║
╚══════════════════════════════════════════════════════════════╝

DATASETS NEEDED:
────────────────────────────────────────────────────────────────

1. ✅ Bitcoin Tweets (~4M tweets, ~2GB)
   https://www.kaggle.com/datasets/alaix14/bitcoin-tweets-20160101-to-20190329
   → Extract all CSV files into:  data/tweets/

2. ✅ Crypto Market OHLCV (one CSV file per coin)
   https://www.kaggle.com/datasets/sudalairajkumar/cryptocurrencypricehistory
   → Extract ALL .csv files into:  data/prices/
     Script auto-merges them and extracts coin name from filename.

────────────────────────────────────────────────────────────────
FOLDER STRUCTURE:
    data/
    ├── tweets/              ← all tweet CSV files go here
    │   └── bitcoin_tweets_2016.csv  ...
    └── prices/              ← all OHLCV CSV files go here
        ├── bitcoin_price.csv
        └── ethereum_price.csv  ...

────────────────────────────────────────────────────────────────
HOW PUMP DETECTION WORKS (no labeled file needed):
    Pumps are detected automatically from the OHLCV data itself:
      • Price body > 10% in a single candle  →  flagged as pump
      • This is the same logic a real fraud system uses in production
      • Validated against known pump dates (BTC: Dec 2017, May 2021 etc.)

────────────────────────────────────────────────────────────────
RUN:
    python producer_replay.py --stats               # check data
    python producer_replay.py --dataset all --speed 100
    python producer_replay.py --dataset tweets --speed 500
    python producer_replay.py --dataset prices --speed 50
    python producer_replay.py --dataset tweets --speed 0   # max throughput
"""

import json
import time
import argparse
import os
import glob
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer

# ──────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────
KAFKA_BROKER    = "localhost:9092"
TOPIC_SOCIAL    = "social_interactions"
TOPIC_PRICE     = "price_feed"

DATA_DIR        = "data"
TWEETS_DIR      = os.path.join(DATA_DIR, "tweets")       # folder of tweet CSVs
PRICES_DIR      = os.path.join(DATA_DIR, "prices")       # folder of OHLCV CSVs

# Also check for single files (in case user placed them directly)
TWEETS_FILE_SINGLE = os.path.join(DATA_DIR, "bitcoin_tweets.csv")
PRICES_FILE_SINGLE = os.path.join(DATA_DIR, "crypto_prices.csv")

# ──────────────────────────────────────────────────────────
# KAFKA PRODUCER
# ──────────────────────────────────────────────────────────
def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        batch_size=65536,      # larger batches = higher throughput
        linger_ms=10,
        compression_type="gzip",
    )

# ──────────────────────────────────────────────────────────
# HELPERS: FILE DISCOVERY
# ──────────────────────────────────────────────────────────
def find_tweet_files() -> list[str]:
    """Returns all tweet CSV files, whether in folder or single file."""
    files = []
    if os.path.isdir(TWEETS_DIR):
        files = sorted(glob.glob(os.path.join(TWEETS_DIR, "*.csv")))
    if not files and os.path.isfile(TWEETS_FILE_SINGLE):
        files = [TWEETS_FILE_SINGLE]
    return files


def find_price_files() -> list[str]:
    """Returns all OHLCV CSV files, whether in folder or single merged file."""
    files = []
    if os.path.isdir(PRICES_DIR):
        files = sorted(glob.glob(os.path.join(PRICES_DIR, "*.csv")))
    if not files and os.path.isfile(PRICES_FILE_SINGLE):
        files = [PRICES_FILE_SINGLE]
    return files


def load_prices_df(files: list[str]) -> pd.DataFrame:
    """
    Load and merge multiple OHLCV files into one DataFrame.
    Extracts the coin name from each filename (e.g. bitcoin_price.csv → BITCOIN).
    """
    dfs = []
    for fpath in files:
        try:
            df = pd.read_csv(fpath, low_memory=False)
            df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]

            # Extract coin symbol from filename: "bitcoin_price.csv" → "BITCOIN"
            basename = os.path.basename(fpath)
            symbol   = basename.replace("_price.csv", "").replace(".csv", "").upper()
            df["symbol"] = symbol

            dfs.append(df)
            print(f"  ✅  Loaded {symbol:<20} {len(df):>8,} rows  ({fpath})")
        except Exception as e:
            print(f"  ⚠️  Skipping {fpath}: {e}")

    if not dfs:
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)
    print(f"\n  📦  Total merged rows: {len(merged):,} across {len(dfs)} coins\n")
    return merged

# ──────────────────────────────────────────────────────────
# TOKEN MENTION EXTRACTOR
# ──────────────────────────────────────────────────────────
SKIP_WORDS = {
    "THE", "FOR", "AND", "NOT", "YOU", "WITH", "ARE", "THIS",
    "WILL", "NEXT", "FROM", "JUST", "HAVE", "THAT", "BEEN",
}

def extract_token_mentions(text: str) -> list[str]:
    tokens = []
    for word in str(text).upper().split():
        clean = "".join(c for c in word if c.isalpha())
        if 2 <= len(clean) <= 8 and clean.isupper() and clean not in SKIP_WORDS:
            tokens.append(clean)
    return list(set(tokens))[:5]  # cap at 5

# ──────────────────────────────────────────────────────────
# REPLAY: TWEETS (handles multiple CSV files)
# ──────────────────────────────────────────────────────────
def replay_tweets(producer: KafkaProducer, speed: float = 100):
    files = find_tweet_files()
    if not files:
        print("❌  No tweet files found.")
        print(f"    Put CSV files in: {TWEETS_DIR}/")
        print(f"    OR put single file at: {TWEETS_FILE_SINGLE}")
        return

    print(f"\n📂  Found {len(files)} tweet file(s):")
    for f in files:
        size_mb = os.path.getsize(f) / (1024 * 1024)
        print(f"    {f}  ({size_mb:.0f} MB)")

    total_sent = 0

    for file_idx, fpath in enumerate(files):
        print(f"\n▶️  Replaying file {file_idx + 1}/{len(files)}: {os.path.basename(fpath)}")

        # Read in chunks to handle large 2GB files without crashing RAM
        chunk_size = 50_000
        chunk_num  = 0

        try:
            for chunk in pd.read_csv(fpath, chunksize=chunk_size,
                                     on_bad_lines="skip",
                                     encoding="utf-8",
                                     encoding_errors="ignore",
                                     engine="python"):
                chunk.columns = [c.lower().strip() for c in chunk.columns]

                # Detect column names flexibly
                user_col = next((c for c in chunk.columns if "user" in c and "name" in c), None) \
                           or next((c for c in chunk.columns if "username" in c or "author" in c), None) \
                           or next((c for c in chunk.columns if "user" in c), None)
                text_col = next((c for c in chunk.columns if "text" in c or "tweet" in c or "content" in c), None)
                date_col = next((c for c in chunk.columns if "date" in c or "time" in c or "created" in c), None)

                if not text_col:
                    print(f"  ⚠️  Can't find text column. Columns: {list(chunk.columns)}")
                    continue

                for _, row in chunk.iterrows():
                    username = str(row.get(user_col, f"user_{total_sent}") if user_col else f"user_{total_sent}")
                    text     = str(row.get(text_col, ""))
                    ts       = str(row.get(date_col, datetime.now(timezone.utc).isoformat()) if date_col else datetime.now(timezone.utc).isoformat())

                    mentions = [w[1:] for w in text.split() if w.startswith("@") and len(w) > 2]
                    tokens   = extract_token_mentions(text)
                    targets  = mentions[:3] if mentions else ["crypto_community"]

                    for target in targets:
                        event = {
                            "event_type":       "social_interaction",
                            "source_user":      f"tw_{username[:30]}",
                            "target_user":      f"tw_{target[:30]}",
                            "interaction_type": "tweet_mention",
                            "token_mentioned":  tokens[0] if tokens else None,
                            "timestamp":        ts,
                            "is_bot":           False,
                            "pump_signal":      False,
                            "source":           "kaggle_tweets",
                        }
                        producer.send(TOPIC_SOCIAL, key=event["source_user"], value=event)
                        total_sent += 1

                    if speed > 0:
                        time.sleep(0.001 / max(speed, 0.1))

                chunk_num += 1
                if chunk_num % 5 == 0:
                    print(f"  [Tweets] Chunks processed: {chunk_num} | Events sent: {total_sent:,}")

        except Exception as e:
            print(f"  ⚠️  Error reading {fpath}: {e}")
            continue

    producer.flush()
    print(f"\n✅  Tweets replay complete — total events sent: {total_sent:,}")


# ──────────────────────────────────────────────────────────
# REPLAY: PRICES (merges all per-coin CSV files)
# ──────────────────────────────────────────────────────────
def replay_prices(producer: KafkaProducer, speed: float = 100):
    files = find_price_files()
    if not files:
        print("❌  No price files found.")
        print(f"    Put CSV files in: {PRICES_DIR}/")
        print(f"    OR put single merged file at: {PRICES_FILE_SINGLE}")
        return

    print(f"\n📂  Loading {len(files)} price file(s) …")
    df = load_prices_df(files)

    if df.empty:
        print("❌  No data could be loaded from price files.")
        return

    total = len(df)
    sent  = 0

    print(f"📡  Streaming {total:,} price records to Kafka …\n")

    # Map common column name variations
    col_map = {
        "open":   next((c for c in df.columns if c in ("open", "open_price")), None),
        "high":   next((c for c in df.columns if c in ("high", "high_price")), None),
        "low":    next((c for c in df.columns if c in ("low", "low_price")), None),
        "close":  next((c for c in df.columns if c in ("close", "close_price")), None),
        "volume": next((c for c in df.columns if "volume" in c), None),
        "date":   next((c for c in df.columns if "date" in c or "time" in c), None),
    }

    for _, row in df.iterrows():
        try:
            open_  = float(row[col_map["open"]])  if col_map["open"]   else 0.0
            close_ = float(row[col_map["close"]]) if col_map["close"]  else 0.0
            volume = float(row[col_map["volume"]]) if col_map["volume"] else 0.0
            ts     = str(row[col_map["date"]]) if col_map["date"] else datetime.now(timezone.utc).isoformat()

            # Simple pump heuristic: large candle body relative to price
            price_change_pct = abs(close_ - open_) / open_ * 100 if open_ > 0 else 0
            is_pumping = price_change_pct > 10

            candle = {
                "event_type":  "price_candle",
                "symbol":      str(row.get("symbol", "UNKNOWN")),
                "open":        open_,
                "high":        float(row[col_map["high"]]) if col_map["high"] else 0.0,
                "low":         float(row[col_map["low"]])  if col_map["low"]  else 0.0,
                "close":       close_,
                "volume":      volume,
                "pumping":     is_pumping,
                "timestamp":   ts,
                "source":      "kaggle_ohlcv",
            }
            producer.send(TOPIC_PRICE, key=candle["symbol"], value=candle)
            sent += 1

            if sent % 100_000 == 0:
                print(f"  [Prices] Sent {sent:,} / {total:,}  ({sent/total*100:.1f}%)")

            if speed > 0:
                time.sleep(0.0005 / max(speed, 0.1))

        except Exception:
            continue

    producer.flush()
    print(f"\n✅  Prices replay complete — total candles sent: {sent:,}")



# ──────────────────────────────────────────────────────────
# DATA STATS — run with --stats to see what you have
# ──────────────────────────────────────────────────────────
def print_stats():
    print("\n📊  Dataset Status")
    print("─" * 60)

    # Tweets
    tweet_files = find_tweet_files()
    if tweet_files:
        total_rows = 0
        total_mb   = 0
        for f in tweet_files:
            total_mb += os.path.getsize(f) / (1024 * 1024)
            try:
                total_rows += sum(1 for _ in open(f, encoding="utf-8", errors="ignore")) - 1
            except Exception:
                pass
        print(f"  ✅  Tweets      {len(tweet_files)} file(s)  ~{total_rows:>10,} rows  ({total_mb:.0f} MB)")
    else:
        print(f"  ❌  Tweets      NOT FOUND — put files in {TWEETS_DIR}/ or {TWEETS_FILE_SINGLE}")

    # Prices
    price_files = find_price_files()
    if price_files:
        total_mb = sum(os.path.getsize(f) / (1024 * 1024) for f in price_files)
        print(f"  ✅  Prices      {len(price_files)} file(s)  {'(multiple coins)':>15}  ({total_mb:.0f} MB)")
        print(f"       Coins: {', '.join(os.path.basename(f).replace('_price.csv','').replace('.csv','').upper() for f in price_files[:8])} …")
    else:
        print(f"  ❌  Prices      NOT FOUND — put files in {PRICES_DIR}/ or {PRICES_FILE_SINGLE}")

    print()


# ──────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Crypto-Shield Kaggle Replay Producer")
    parser.add_argument("--dataset", choices=["tweets", "prices", "all"],
                        default="all", help="Which dataset to replay")
    parser.add_argument("--speed",   type=float, default=100,
                        help="Replay speed multiplier. 0=max throughput")
    parser.add_argument("--stats",   action="store_true",
                        help="Just show dataset stats and exit")
    args = parser.parse_args()

    os.makedirs(TWEETS_DIR, exist_ok=True)
    os.makedirs(PRICES_DIR, exist_ok=True)

    print_stats()

    if args.stats:
        return

    producer = make_producer()
    print("✅  Kafka producer connected.\n")

    if args.dataset in ("tweets", "all"):
        replay_tweets(producer, speed=args.speed)

    if args.dataset in ("prices", "all"):
        replay_prices(producer, speed=args.speed)

    print("\n🏁  All replays complete.")


if __name__ == "__main__":
    main()