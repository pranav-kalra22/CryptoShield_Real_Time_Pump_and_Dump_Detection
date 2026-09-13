#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────
# CryptoShield — Unix Quick Runner
# ──────────────────────────────────────────────────────────

set -e

cmd="${1:-help}"

case "$cmd" in
  infra)
    docker-compose up -d
    ;;
  down)
    docker-compose down
    ;;
  test)
    python -m unittest discover -s tests -v
    ;;
  producer-test)
    python producer.py
    ;;
  producer-pump)
    python producer.py --pump
    ;;
  producer-live)
    python producer_realtime.py
    ;;
  producer-replay)
    python producer_replay.py --dataset all --speed 100
    ;;
  consumer)
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 consumer.py
    ;;
  dashboard)
    streamlit run dashboard.py
    ;;
  *)
    echo "CryptoShield Quick Runner"
    echo "========================="
    echo "Usage: ./run.sh [command]"
    echo ""
    echo "  infra            - Start Docker infrastructure"
    echo "  down             - Stop Docker infrastructure"
    echo "  test             - Run unit tests"
    echo "  producer-test    - Run synthetic test producer"
    echo "  producer-pump    - Run synthetic test producer with pump injection"
    echo "  producer-live    - Run real-time CoinGecko producer"
    echo "  producer-replay  - Run Kaggle replay producer"
    echo "  consumer         - Run Spark consumer"
    echo "  dashboard        - Launch Streamlit dashboard"
    ;;
esac
