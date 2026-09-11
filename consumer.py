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

import json
from collections import defaultdict
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType
)
from pymongo import MongoClient

# ──────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────
KAFKA_BROKER  = "localhost:9092"
TOPICS        = "social_interactions,price_feed"
MONGO_URI     = "mongodb://localhost:27017/"
MONGO_DB      = "crypto_shield"
MONGO_ALERTS  = "alerts"
MONGO_METRICS = "metrics"

CLIQUE_MIN_SIZE        = 5
CLIQUE_DENSITY_THRESH  = 0.6
STAR_MIN_DEGREE        = 10
STAR_DEGREE_RATIO      = 0.8

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
# GRAPH ANALYSIS
# ──────────────────────────────────────────────────────────
def build_graph(edges: list):
    adjacency  = defaultdict(set)
    out_degree = defaultdict(int)
    in_degree  = defaultdict(int)
    for src, tgt in edges:
        adjacency[src].add(tgt)
        adjacency[tgt].add(src)
        out_degree[src] += 1
        in_degree[tgt]  += 1
    return dict(adjacency), dict(out_degree), dict(in_degree)


def detect_star_topology(adjacency, out_degree, in_degree) -> list:
    alerts = []
    for node, out_count in out_degree.items():
        if out_count < STAR_MIN_DEGREE:
            continue
        total = out_count + in_degree.get(node, 0)
        ratio = out_count / total if total > 0 else 0
        if ratio >= STAR_DEGREE_RATIO:
            alerts.append({
                "alert_type": "STAR_TOPOLOGY",
                "hub_node":   node,
                "out_degree": out_count,
                "out_ratio":  round(ratio, 3),
                "severity":   "HIGH",
            })
    return alerts


def detect_clique(adjacency) -> list:
    alerts  = []
    visited = set()
    for node, neighbors in adjacency.items():
        if node in visited or len(neighbors) < CLIQUE_MIN_SIZE - 1:
            continue
        candidate  = frozenset({node} | set(neighbors))
        if len(candidate) < CLIQUE_MIN_SIZE:
            continue
        nodes_list = list(candidate)
        edges_in   = sum(
            1 for i, u in enumerate(nodes_list)
            for v in nodes_list[i + 1:]
            if v in adjacency.get(u, set())
        )
        n        = len(candidate)
        possible = n * (n - 1) / 2
        density  = edges_in / possible if possible > 0 else 0
        if density >= CLIQUE_DENSITY_THRESH:
            visited.update(candidate)
            alerts.append({
                "alert_type":   "CLIQUE_DETECTED",
                "nodes":        nodes_list[:20],
                "node_count":   n,
                "edge_density": round(density, 3),
                "severity":     "CRITICAL" if density > 0.8 else "HIGH",
            })
    return alerts


def compute_confidence(alerts: list, price_pumping: bool, gt_ratio: float) -> str:
    score = 0
    score += len([a for a in alerts if a["alert_type"] == "CLIQUE_DETECTED"]) * 3
    score += len([a for a in alerts if a["alert_type"] == "STAR_TOPOLOGY"])   * 2
    if price_pumping:
        score += 3
    score += int(gt_ratio * 5)
    if score >= 8: return "CRITICAL"
    if score >= 5: return "HIGH"
    if score >= 2: return "MEDIUM"
    return "LOW"

# ──────────────────────────────────────────────────────────
# PRECISION / RECALL TRACKER
# ──────────────────────────────────────────────────────────
_metrics = {"tp": 0, "fp": 0, "fn": 0, "total": 0}

def update_metrics(alerts: list, rows: list):
    global _metrics
    ground_truth = any(getattr(r, "pump_signal", False) for r in rows)
    detected     = len(alerts) > 0
    if ground_truth and detected:     _metrics["tp"] += 1
    elif not ground_truth and detected: _metrics["fp"] += 1
    elif ground_truth and not detected: _metrics["fn"] += 1
    _metrics["total"] += 1
    tp, fp, fn = _metrics["tp"], _metrics["fp"], _metrics["fn"]
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1        = (2 * precision * recall / (precision + recall)
                 if (precision + recall) > 0 else 0.0)
    return round(precision, 3), round(recall, 3), round(f1, 3)

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
            "true_positives":  _metrics["tp"],
            "false_positives": _metrics["fp"],
            "false_negatives": _metrics["fn"],
            "total_batches":   _metrics["total"],
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
        .option("checkpointLocation", "/tmp/crypto_shield_v2_checkpoint")
        .start()
    )

    print("[STREAMING] Listening on topics: social_interactions, price_feed")
    query.awaitTermination()


if __name__ == "__main__":
    main()