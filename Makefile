# ──────────────────────────────────────────────────────────
# CryptoShield — Developer Makefile
# ──────────────────────────────────────────────────────────

.PHONY: help infra down test producer-test producer-live producer-replay consumer dashboard clean

help:
	@echo "CryptoShield Automation Helper"
	@echo "=============================="
	@echo "make infra            - Start Docker infrastructure (Kafka, Mongo, UIs)"
	@echo "make down             - Stop Docker infrastructure"
	@echo "make test             - Run unit tests"
	@echo "make producer-test    - Run synthetic test producer (normal traffic)"
	@echo "make producer-pump    - Run synthetic test producer (injected pump attack)"
	@echo "make producer-live    - Run real-time CoinGecko producer"
	@echo "make producer-replay  - Run Kaggle dataset replay producer (100x speed)"
	@echo "make consumer         - Run Spark Streaming consumer"
	@echo "make dashboard        - Run Streamlit real-time dashboard"
	@echo "make clean            - Remove checkpoints and temp files"

infra:
	docker-compose up -d

down:
	docker-compose down

test:
	python -m unittest discover -s tests -v

producer-test:
	python producer.py

producer-pump:
	python producer.py --pump

producer-live:
	python producer_realtime.py

producer-replay:
	python producer_replay.py --dataset all --speed 100

consumer:
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 consumer.py

dashboard:
	streamlit run dashboard.py

clean:
	rm -rf __pycache__ tests/__pycache__ .pytest_cache /tmp/crypto_shield_v2_checkpoint checkpoint
