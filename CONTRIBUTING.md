# Contributing to CryptoShield

First off, thank you for considering contributing to **CryptoShield**! 🎉 It is open-source contributions that make distributed streaming fraud detection robust and accessible.

Please take a moment to review this document before submitting your contribution.

---

## 📋 Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md). Please report unacceptable behavior following the guidelines in that document.

---

## 🛠️ Getting Started

### 1. Fork and Clone the Repository

```bash
git clone https://github.com/<your-username>/CryptoShield_Real_Time_Pump_and_Dump_Detection.git
cd CryptoShield_Real_Time_Pump_and_Dump_Detection
```

### 2. Set Up a Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/macOS:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Upgrade pip and install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Start Infrastructure (Docker)

```bash
docker-compose up -d
```

Verify that Kafka (`localhost:9092`), MongoDB (`localhost:27017`), Kafka-UI (`localhost:8080`), and Mongo Express (`localhost:8082`) are healthy.

---

## 🧪 Running Tests & Linting

Before opening a pull request, ensure all tests and linters pass:

```bash
# Run unit tests
python -m unittest discover -s tests -v

# Run flake8 syntax and complexity checks
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics --exclude=data/
flake8 . --count --exit-zero --max-complexity=15 --max-line-length=127 --statistics --exclude=data/
```

Or run via the provided helpers:
- **Linux/macOS:** `make test`
- **Windows:** `run.bat test`

---

## 🌿 Branching Strategy & Git Conventions

1. **Branch Naming**:
   - `feat/feature-name` for new features or algorithms
   - `fix/bug-description` for bug fixes
   - `docs/documentation-update` for doc improvements
   - `perf/optimization` for performance enhancements

2. **Commit Messages**:
   Follow [Conventional Commits](https://www.conventionalcommits.org/):
   ```text
   feat: add PageRank weighting to graph detection engine
   fix: handle null ticker timestamps in replay producer
   docs: update Spark 4.1.1 cluster deployment guide
   test: add edge-case tests for isolated graph nodes
   ```

---

## 🚀 Pull Request Process

1. **Keep PRs focused**: Address a single problem or feature per PR.
2. **Include unit tests**: Any new detection algorithm, data producer, or transformation logic should have corresponding unit tests in `tests/`.
3. **Verify CI**: Ensure all checks in the GitHub Actions CI pipeline pass.
4. **Update documentation**: If introducing new environment variables, CLI options, or architectural components, update `README.md` and `.env.example`.
5. **Fill out the PR Template**: Clearly describe the motivation, solution, and manual testing performed.

Thank you for helping keep the cryptocurrency ecosystem safe from market manipulation! 🛡️
