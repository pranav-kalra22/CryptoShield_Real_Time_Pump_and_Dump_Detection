"""
╔══════════════════════════════════════════════════════════════╗
║      Crypto-Shield — LIVE Producer (CoinGecko Only)         ║
║  Streams real-time crypto prices to Kafka — no API key      ║
╚══════════════════════════════════════════════════════════════╝

WHAT THIS DOES:
  • Polls CoinGecko every 30 seconds for live prices of 15 tokens
  • Detects sudden price/volume spikes in real time
  • Generates synthetic social interaction bursts whenever a price
    spike is detected — simulating the coordinated social activity
    that accompanies a real pump event
  • Streams everything to Kafka (social_interactions + price_feed)

WHY NO REDDIT/TWITTER:
  Reddit disabled self-serve app creation (2024/2025).
  Twitter/X API requires paid access.
  CoinGecko free tier needs NO signup, NO API key.

RUN:
    pip install -r requirements.txt
    python producer_realtime.py

    # Runs forever until Ctrl+C
    # Best used alongside producer_replay.py for maximum volume
"""

import os
import json
import time
import requests
from datetime import datetime, timezone

from kafka import KafkaProducer

# ──────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_SOCIAL   = os.getenv("TOPIC_SOCIAL", "social_interactions")
TOPIC_PRICE    = os.getenv("TOPIC_PRICE", "price_feed")

# CoinGecko free tier — no API key, max ~30 calls/min
COINGECKO_URL  = os.getenv("COINGECKO_URL", "https://api.coingecko.com/api/v3/simple/price")
POLL_INTERVAL  = int(os.getenv("COINGECKO_POLL_INTERVAL", "30"))        # seconds between price polls
SPIKE_THRESHOLD = float(os.getenv("PRICE_SPIKE_THRESHOLD", "0.2"))      # % price change between polls = suspicious

# Tokens to monitor — mix of large + known pump targets
TOKENS = [
    "bitcoin", "ethereum", "dogecoin", "shiba-inu", "pepe",
    "floki", "baby-doge-coin", "solana", "ripple", "cardano",
    "polkadot", "avalanche-2", "chainlink", "uniswap", "litecoin",
]

# How many synthetic social events to generate per price spike
SOCIAL_BURST_SIZE = int(os.getenv("SOCIAL_BURST_SIZE", "25"))   # coordinator blasts 25 users = star topology

# ──────────────────────────────────────────────────────────
# KAFKA PRODUCER
# ──────────────────────────────────────────────────────────
def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

# ──────────────────────────────────────────────────────────
# COINGECKO FETCH
# ──────────────────────────────────────────────────────────
def fetch_prices() -> dict | None:
    """
    Fetches current USD price, 24h volume, and 24h change for all tokens.
    Returns None if the request fails.
    """
    try:
        params = {
            "ids":                 ",".join(TOKENS),
            "vs_currencies":       "usd",
            "include_24hr_vol":    "true",
            "include_24hr_change": "true",
            "include_last_updated_at": "true",
        }
        resp = requests.get(COINGECKO_URL, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"  ⚠️  CoinGecko request failed: {e}")
        return None

# ──────────────────────────────────────────────────────────
# SOCIAL BURST GENERATOR
# ──────────────────────────────────────────────────────────
def send_social_burst(producer: KafkaProducer, symbol: str,
                      change_pct: float, ts: str):
    """
    When a price spike is detected, generate a realistic social burst:
      1. Star topology  — one coordinator mentions many users (pump signal)
      2. Bot ring       — small clique of bots all talking to each other
    These mimic the social patterns that precede/accompany real pump events.
    """
    coordinator = f"live_coord_{symbol}"

    # 1. Star topology — coordinator → SOCIAL_BURST_SIZE victims
    for i in range(SOCIAL_BURST_SIZE):
        event = {
            "event_type":       "social_interaction",
            "source_user":      coordinator,
            "target_user":      f"live_user_{symbol}_{i:04d}",
            "interaction_type": "mention",
            "token_mentioned":  symbol,
            "timestamp":        ts,
            "is_bot":           True,
            "pump_signal":      True,
            "source":           "coingecko_spike_synthetic",
            "price_change_pct": round(change_pct, 4),
        }
        producer.send(TOPIC_SOCIAL, key=coordinator, value=event)

    # 2. Bot ring — 8 bots forming a dense clique
    bot_ring = [f"live_bot_{symbol}_{i:02d}" for i in range(8)]
    for src in bot_ring:
        for tgt in bot_ring:
            if src != tgt:
                event = {
                    "event_type":       "social_interaction",
                    "source_user":      src,
                    "target_user":      tgt,
                    "interaction_type": "retweet",
                    "token_mentioned":  symbol,
                    "timestamp":        ts,
                    "is_bot":           True,
                    "pump_signal":      True,
                    "source":           "coingecko_spike_synthetic",
                }
                producer.send(TOPIC_SOCIAL, key=src, value=event)

    print(f"  📡  Social burst sent — {SOCIAL_BURST_SIZE} star edges "
          f"+ {len(bot_ring) * (len(bot_ring)-1)} clique edges for {symbol}")

# ──────────────────────────────────────────────────────────
# MAIN LOOP
# ──────────────────────────────────────────────────────────
def main():
    print("🔌  Connecting to Kafka at", KAFKA_BROKER)
    producer = make_producer()
    print("✅  Connected.\n")
    print(f"📡  Monitoring {len(TOKENS)} tokens via CoinGecko")
    print(f"⚡  Spike threshold: ±{SPIKE_THRESHOLD}% per {POLL_INTERVAL}s poll")
    print(f"🔄  Polling every {POLL_INTERVAL} seconds — Ctrl+C to stop\n")

    # Track previous prices to detect sudden moves
    prev_prices: dict[str, float] = {}
    poll_count = 0

    try:
        while True:
            poll_count += 1
            ts   = datetime.now(timezone.utc).isoformat()
            data = fetch_prices()

            if data is None:
                print(f"[Poll {poll_count}] ⚠️  Skipping — will retry in {POLL_INTERVAL}s")
                time.sleep(POLL_INTERVAL)
                continue

            spikes_detected = 0

            for token_id, metrics in data.items():
                price      = metrics.get("usd", 0) or 0
                volume_24h = metrics.get("usd_24h_vol", 0) or 0
                change_24h = metrics.get("usd_24h_change", 0) or 0
                symbol     = token_id.upper().replace("-", "_")

                # Calculate change since LAST poll (not 24h — much more sensitive)
                prev        = prev_prices.get(token_id, price)
                poll_change = ((price - prev) / prev * 100) if prev > 0 else 0.0
                is_spike    = abs(poll_change) >= SPIKE_THRESHOLD

                # ── Send price candle to Kafka ─────────────
                candle = {
                    "event_type":            "price_candle",
                    "symbol":                symbol,
                    "price_usd":             price,
                    "volume_24h_usd":        volume_24h,
                    "change_24h_pct":        round(change_24h, 4),
                    "change_since_last_poll": round(poll_change, 4),
                    "pumping":               is_spike,
                    "timestamp":             ts,
                    "source":                "coingecko_live",
                }
                producer.send(TOPIC_PRICE, key=token_id, value=candle)

                # ── If spike, also send social burst ───────
                if is_spike:
                    spikes_detected += 1
                    direction = "🚀 UP" if poll_change > 0 else "🔻 DOWN"
                    print(f"  🚨 SPIKE [{symbol}] {direction} {poll_change:+.2f}% "
                          f"in last {POLL_INTERVAL}s  |  price=${price:.6f}  "
                          f"vol=${volume_24h:,.0f}")
                    send_social_burst(producer, symbol, poll_change, ts)

                prev_prices[token_id] = price

            producer.flush()

            # Summary line every poll
            status = f"🚨 {spikes_detected} spike(s)" if spikes_detected else "✅ stable"
            print(f"[Poll {poll_count:04d}] {status} | "
                  f"{len(data)} tokens checked | "
                  f"{datetime.now().strftime('%H:%M:%S')}")

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n⛔  Stopped after {poll_count} polls.")
    finally:
        producer.flush()
        producer.close()

# ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()