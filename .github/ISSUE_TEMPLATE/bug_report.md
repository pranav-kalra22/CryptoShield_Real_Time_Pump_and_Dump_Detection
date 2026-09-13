---
name: 🐛 Bug Report
about: Create a report to help us fix an issue or broken pipeline component
title: "[BUG] "
labels: ["bug"]
assignees: ""
---

**Describe the Bug**
A clear and concise description of what the bug is.

**Component Affected**
- [ ] Ingestion Producer (`producer.py`, `producer_realtime.py`, `producer_replay.py`)
- [ ] Kafka Streaming Broker
- [ ] Spark Streaming Consumer (`consumer.py`)
- [ ] Fraud Detection Engine (`detection_engine.py`)
- [ ] MongoDB Persistence
- [ ] Streamlit Dashboard (`dashboard.py`)
- [ ] Infrastructure / Docker Compose

**To Reproduce**
Steps to reproduce the behavior:
1. Start infrastructure via `docker-compose up -d`
2. Run command `...`
3. See error

**Expected Behavior**
A clear and concise description of what you expected to happen.

**Logs & Screenshots**
If applicable, paste terminal tracebacks, Spark UI errors, or screenshots.

**Environment Details:**
 - OS: [e.g. Windows 11, Ubuntu 22.04, macOS Sonoma]
 - Python Version: [e.g. 3.10, 3.11, 3.12]
 - Apache Spark Version: [e.g. 4.1.1]
 - Docker Compose Version: [e.g. v2.27]

**Additional Context**
Add any other context about the problem here.
