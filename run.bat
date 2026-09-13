@echo off
REM ──────────────────────────────────────────────────────────
REM CryptoShield — Windows Quick Runner
REM ──────────────────────────────────────────────────────────

if "%1"=="" goto help
if "%1"=="infra" goto infra
if "%1"=="down" goto down
if "%1"=="test" goto test
if "%1"=="producer-test" goto producer_test
if "%1"=="producer-pump" goto producer_pump
if "%1"=="producer-live" goto producer_live
if "%1"=="producer-replay" goto producer_replay
if "%1"=="consumer" goto consumer
if "%1"=="dashboard" goto dashboard

:help
echo CryptoShield Windows Helper
echo ===========================
echo Usage: run.bat [command]
echo.
echo   infra            - Start Docker infrastructure
echo   down             - Stop Docker infrastructure
echo   test             - Run unit tests
echo   producer-test    - Run synthetic test producer
echo   producer-pump    - Run synthetic test producer with pump injection
echo   producer-live    - Run real-time CoinGecko producer
echo   producer-replay  - Run Kaggle replay producer
echo   consumer         - Run Spark consumer
echo   dashboard        - Launch Streamlit dashboard
goto :eof

:infra
docker-compose up -d
goto :eof

:down
docker-compose down
goto :eof

:test
python -m unittest discover -s tests -v
goto :eof

:producer_test
python producer.py
goto :eof

:producer_pump
python producer.py --pump
goto :eof

:producer_live
python producer_realtime.py
goto :eof

:producer_replay
python producer_replay.py --dataset all --speed 100
goto :eof

:consumer
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 consumer.py
goto :eof

:dashboard
streamlit run dashboard.py
goto :eof
