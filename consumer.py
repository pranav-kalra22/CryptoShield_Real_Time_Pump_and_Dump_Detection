"""
╔══════════════════════════════════════════════════════════════╗
║      Crypto-Shield — Spark Streaming Consumer (v2)          ║
║  Graph detection + Price-Social correlation + Metrics       ║
╚══════════════════════════════════════════════════════════════╝

WHAT'S NEW vs v1:
  • Reads BOTH social_interactions and price_feed topics
  • Cross-correlates social anomalies with price spikes
  • Tracks precision / recall using ground-truth labels
  • Confidence scoring: LOW / MEDIUM / HIGH / CRITICAL

Run:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
        consumer.py
"""

import os
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType
)
from pymongo import MongoClient

from detection_engine import (
    build_graph,
    detect_star_topology,
    detect_clique,
    compute_confidence,
    update_metrics,
    get_default_metrics_summary,
    _default_tracker as _metrics_tracker,
    CLIQUE_MIN_SIZE,
    CLIQUE_DENSITY_THRESH,
    STAR_MIN_DEGREE,
    STAR_DEGREE_RATIO,
)

# ──────────────────────────────────────────────────────────
# CONFIG (Environment variables with sensible defaults)
# ──────────────────────────────────────────────────────────
KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPICS        = os.getenv("KAFKA_TOPICS", "social_interactions,price_feed")
MONGO_URI     = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB      = os.getenv("MONGO_DB", "crypto_shield")
MONGO_ALERTS  = os.getenv("MONGO_ALERTS_COLLECTION", "alerts")
MONGO_METRICS = os.getenv("MONGO_METRICS_COLLECTION", "metrics")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/crypto_shield_v2_checkpoint")

# ──────────────────────────────────────────────────────────
# SCHEMAS
# ──────────────────────────────────────────────────────────
SOCIAL_SCHEMA = StructType([
    StructField("event_type",       StringType(),  True),
    StructField("source_user",      StringType(),  True),
    StructField("target_user",      StringType(),  True),
    StructField("interaction_type", StringType(),  True),
    StructField("token_mentioned",  StringType(),  True),
    StructField("timestamp",        StringType(),  True),
    StructField("is_bot",           BooleanType(), True),
    StructField("pump_signal",      BooleanType(), True),
    StructField("source",           StringType(),  True),
])

PRICE_SCHEMA = StructType([
    StructField("event_type",                 StringType(),  True),
    StructField("symbol",                     StringType(),  True),
    StructField("close",                      DoubleType(),  True),
    StructField("price_usd",                  DoubleType(),  True),
    StructField("volume",                     DoubleType(),  True),
    StructField("volume_24h_usd",             DoubleType(),  True),
    StructField("change_since_last_poll_pct", DoubleType(),  True),
    StructField("change_24h_pct",             DoubleType(),  True),
    StructField("pumping",                    BooleanType(), True),
    StructField("timestamp",                  StringType(),  True),
    StructField("source",                     StringType(),  True),
])

# ──────────────────────────────────────────────────────────
# MICRO-BATCH PROCESSOR
# ──────────────────────────────────────────────────────────
def process_batch(batch_df, batch_id: int):
    rows = batch_df.collect()
    if not rows:
        return

    social_rows = [r for r in rows if r.event_type == "social_interaction"]
    price_rows  = [r for r in rows if r.event_type == "price_candle"]

    edges = [
        (r.source_user, r.target_user)
        for r in social_rows if r.source_user and r.target_user
    ]

    price_pumping   = any(getattr(r, "pumping", False) for r in price_rows)
    pumping_symbols = list({r.symbol for r in price_rows
                            if getattr(r, "pumping", False) and r.symbol})

    bot_count = sum(1 for r in social_rows if getattr(r, "is_bot", False))
    gt_ratio  = bot_count / len(social_rows) if social_rows else 0.0

    alerts = []
    if len(edges) >= 3:
        adj, out_deg, in_deg = build_graph(edges)
        alerts = detect_star_topology(adj, out_deg, in_deg) + detect_clique(adj)

    confidence          = compute_confidence(alerts, price_pumping, gt_ratio)
    precision, recall, f1 = update_metrics(alerts, social_rows)

    status = "[ANOMALY]" if alerts else "[CLEAN]"
    print(
        f"[Batch {batch_id:04d}] {status} | "
        f"social={len(social_rows)} price={len(price_rows)} | "
        f"alerts={len(alerts)} conf={confidence} | "
        f"P={precision:.2f} R={recall:.2f} F1={f1:.2f}"
    )

    if not alerts and not price_pumping:
        return

    client = MongoClient(MONGO_URI)
    db     = client[MONGO_DB]
    ts     = datetime.now(timezone.utc).isoformat()

    for alert in alerts:
        alert.update({
            "batch_id":           batch_id,
            "confidence":         confidence,
            "price_corroborated": price_pumping,
            "pumping_tokens":     pumping_symbols,
            "social_edge_count":  len(edges),
            "gt_bot_ratio":       round(gt_ratio, 3),
            "detected_at":        ts,
        })
        db[MONGO_ALERTS].insert_one(alert)

    if alerts and price_pumping:
        db[MONGO_ALERTS].insert_one({
            "alert_type":        "PUMP_AND_DUMP_CONFIRMED",
            "batch_id":          batch_id,
            "confidence":        "CRITICAL",
            "pumping_tokens":    pumping_symbols,
            "social_alerts":     len(alerts),
            "social_edge_count": len(edges),
            "detected_at":       ts,
            "severity":          "CRITICAL",
        })
        print(f"  *** PUMP-AND-DUMP CONFIRMED --- tokens: {pumping_symbols}")

    db[MONGO_METRICS].replace_one(
        {"_id": "running_metrics"},
        {
            "_id":             "running_metrics",
            "precision":       precision,
            "recall":          recall,
            "f1_score":        f1,
            "true_positives":  _metrics_tracker.tp,
            "false_positives": _metrics_tracker.fp,
            "false_negatives": _metrics_tracker.fn,
            "total_batches":   _metrics_tracker.total,
            "updated_at":      ts,
        },
        upsert=True,
    )
    client.close()

# ──────────────────────────────────────────────────────────
# SPARK SESSION
# ──────────────────────────────────────────────────────────
def main():
    spark = (
        SparkSession.builder
        .appName("CryptoShield-Consumer-v2")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[OK] Spark session started.\n")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("kafka.metadata.max.age.ms", "5000")
        .load()
    )

    # Parse unified — use the broader schema for all events
    parsed = (
        raw.select(
            from_json(col("value").cast("string"), SOCIAL_SCHEMA).alias("s"),
            from_json(col("value").cast("string"), PRICE_SCHEMA).alias("p"),
        )
        .select(
            # Social fields
            col("s.event_type"),
            col("s.source_user"),
            col("s.target_user"),
            col("s.interaction_type"),
            col("s.token_mentioned"),
            col("s.is_bot"),
            col("s.pump_signal"),
            # Price fields
            col("p.symbol"),
            col("p.pumping"),
            col("s.timestamp"),
            col("s.source"),
        )
    )

    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    print("[STREAMING] Listening on topics: social_interactions, price_feed")
    query.awaitTermination()


if __name__ == "__main__":
    main()