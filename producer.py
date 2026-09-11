"""
╔══════════════════════════════════════════════════════════╗
║         Crypto-Shield — Data Producer                   ║
║  Simulates social interactions + price feed to Kafka    ║
╚══════════════════════════════════════════════════════════╝

Two Kafka topics produced:
  • social_interactions  — tweets/retweets/mentions between users
  • price_feed           — OHLCV candles for a low-cap token

"""

import json
import time
import random
import argparse
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

# ──────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
TOPIC_SOCIAL = "social_interactions"
TOPIC_PRICE  = "price_feed"
TOKEN_SYMBOL = "SCAMCOIN"

NORMAL_INTERVAL_SEC  = 0.5   # one event every 0.5 s in normal mode
PUMP_INTERVAL_SEC    = 0.05  # 20x speed during pump
PUMP_DURATION_SEC    = 60    # pump lasts 60 seconds

fake = Faker()

# ──────────────────────────────────────────────────────────
# USER POOLS
# ──────────────────────────────────────────────────────────
# Organic users — random activity
ORGANIC_USERS = [f"user_{fake.user_name()}" for _ in range(200)]

# Bot ring — tightly connected clique (will form dense subgraph)
BOT_RING = [f"bot_{i:03d}" for i in range(20)]

# Pump coordinator — star topology hub
PUMP_COORDINATOR = "coordinator_whale"

# ──────────────────────────────────────────────────────────
# KAFKA PRODUCER SETUP
# ──────────────────────────────────────────────────────────
def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

# ──────────────────────────────────────────────────────────
# EVENT GENERATORS
# ──────────────────────────────────────────────────────────
INTERACTION_TYPES = ["retweet", "mention", "reply", "like", "share"]

def organic_interaction() -> dict:
    """Normal random social interaction between two organic users."""
    src, tgt = random.sample(ORGANIC_USERS, 2)
    return {
        "event_type":        "social_interaction",
        "source_user":       src,
        "target_user":       tgt,
        "interaction_type":  random.choice(INTERACTION_TYPES),
        "token_mentioned":   TOKEN_SYMBOL if random.random() < 0.1 else None,
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "is_bot":            False,
        "pump_signal":       False,
    }

def bot_ring_interaction() -> dict:
    """
    Bot ring: every bot interacts with every other bot → dense clique.
    GraphX will detect this as abnormally high clustering coefficient.
    """
    src = random.choice(BOT_RING)
    tgt = random.choice([b for b in BOT_RING if b != src])
    return {
        "event_type":        "social_interaction",
        "source_user":       src,
        "target_user":       tgt,
        "interaction_type":  random.choice(["retweet", "mention"]),
        "token_mentioned":   TOKEN_SYMBOL,
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "is_bot":            True,
        "pump_signal":       False,
    }

def pump_coordinator_blast() -> dict:
    """
    Star topology: coordinator → many followers simultaneously.
    Simulates the pump signal broadcast.
    """
    tgt = random.choice(ORGANIC_USERS + BOT_RING)
    return {
        "event_type":        "social_interaction",
        "source_user":       PUMP_COORDINATOR,
        "target_user":       tgt,
        "interaction_type":  "mention",
        "token_mentioned":   TOKEN_SYMBOL,
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "is_bot":            True,
        "pump_signal":       True,   # ← ground-truth label
    }

# ──────────────────────────────────────────────────────────
# PRICE FEED GENERATOR
# ──────────────────────────────────────────────────────────
_price_state = {"price": 0.000045, "volume": 1000}

def price_candle(pumping: bool = False) -> dict:
    """
    Generates a fake 1-minute OHLCV candle.
    During a pump: price spikes 5–25%, volume explodes 10–50x.
    """
    s = _price_state
    if pumping:
        change  = random.uniform(0.05, 0.25)   # +5% to +25%
        vol_mul = random.uniform(10, 50)
    else:
        change  = random.uniform(-0.03, 0.03)   # ±3% normal drift
        vol_mul = 1.0

    open_  = s["price"]
    close  = round(open_ * (1 + change), 8)
    high   = round(max(open_, close) * random.uniform(1.0, 1.05), 8)
    low    = round(min(open_, close) * random.uniform(0.95, 1.0), 8)
    volume = round(s["volume"] * vol_mul * random.uniform(0.8, 1.2), 2)

    s["price"]  = close          # carry forward
    s["volume"] = volume * 0.1   # mean-revert slowly

    return {
        "event_type": "price_candle",
        "symbol":     TOKEN_SYMBOL,
        "open":       open_,
        "high":       high,
        "low":        low,
        "close":      close,
        "volume":     volume,
        "pumping":    pumping,    # ground-truth label
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }

# ──────────────────────────────────────────────────────────
# MAIN LOOP
# ──────────────────────────────────────────────────────────
def run(inject_pump: bool = False):
    print("Connecting to Kafka at", KAFKA_BROKER)
    producer = make_producer()
    print(f"[OK] Connected. Streaming to '{TOPIC_SOCIAL}' and '{TOPIC_PRICE}'")
    print("     Press Ctrl+C to stop.\n")

    pump_end_time = time.time() + PUMP_DURATION_SEC if inject_pump else 0
    event_count   = 0

    try:
        while True:
            now     = time.time()
            pumping = inject_pump and (now < pump_end_time)
            interval = PUMP_INTERVAL_SEC if pumping else NORMAL_INTERVAL_SEC

            # ── Social events ──────────────────────────────
            if pumping:
                # During pump: mix coordinator blasts + bot ring + organic
                weights = [0.4, 0.4, 0.2]
                choice  = random.choices(
                    ["coordinator", "bot_ring", "organic"], weights=weights
                )[0]
                if choice == "coordinator":
                    event = pump_coordinator_blast()
                elif choice == "bot_ring":
                    event = bot_ring_interaction()
                else:
                    event = organic_interaction()
            else:
                # Normal: mostly organic, occasional bot ring noise
                if random.random() < 0.05:
                    event = bot_ring_interaction()
                else:
                    event = organic_interaction()

            producer.send(
                TOPIC_SOCIAL,
                key=event["source_user"],
                value=event,
            )

            # ── Price candle (every 10 social events) ──────
            if event_count % 10 == 0:
                candle = price_candle(pumping=pumping)
                producer.send(TOPIC_PRICE, key=TOKEN_SYMBOL, value=candle)
                print(
                    f"[{'*** PUMP ***' if pumping else 'NORMAL'}] "
                    f"Price: ${candle['close']:.8f}  "
                    f"Vol: {candle['volume']:,.0f}  "
                    f"Social events sent: {event_count}"
                )

            event_count += 1
            time.sleep(interval)

            # Announce pump end
            if inject_pump and not pumping and event_count > 1:
                inject_pump = False   # only announce once
                print("\n[DONE] PUMP PHASE ENDED -- back to normal traffic\n")

    except KeyboardInterrupt:
        print(f"\n[STOPPED] after {event_count} events.")
    finally:
        producer.flush()
        producer.close()

# ──────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crypto-Shield data producer")
    parser.add_argument(
        "--pump",
        action="store_true",
        help="Immediately inject a 60-second pump event into the stream",
    )
    args = parser.parse_args()
    run(inject_pump=args.pump)
