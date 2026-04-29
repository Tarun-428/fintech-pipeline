# Quick Start Guide (5-15 Minutes)

## TL;DR - Get It Running Fast

New to the project? Follow this to get everything working in 15 minutes.

---

## Prerequisites (2 min)

- Docker installed: https://www.docker.com/
- Python 3.8+ installed
- Git (optional)
- A Snowflake account (free trial: https://signup.snowflake.com)

Verify:
```bash
docker --version
python3 --version
```

---

## Step 1: Clone/Enter Project (1 min)

```bash
# If you don't have the project yet
cd ~/Desktop  # Or wherever you want it
git clone <project-repo>
cd stock-market

# Or if you already have it
cd /home/lap-46/Desktop/stock-market
```

---

## Step 2: Create & Setup Python Environment (2 min)

```bash
# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate  # Mac/Linux
# OR on Windows:
# venv\Scripts\activate

# Install packages
pip install kafka-python requests snowflake-connector-python
```

---

## Step 3: Create .env Configuration (2 min)

Create file: `.env`

```bash
# Open editor
nano .env
```

Paste this (replace with YOUR values):

```bash
# Snowflake - Get from https://snowflake.com after login
SNOWFLAKE_ACCOUNT=xy12345.us-east-1.snowflakecomputing.com
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=FINTECH_ANALYTICS
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=ACCOUNTADMIN

# Kafka (don't change these usually)
KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:39092
KAFKA_TOPIC=market_prices
KAFKA_CONSUMER_GROUP=market_prices_snowflake_sink
```

Save: Press `Ctrl+X`, then `Y`, then `Enter`

---

## Step 4: Start Docker Services (2 min)

```bash
# Start all containers
docker-compose up -d

# Verify running
docker ps

# Should see: kafka, zookeeper, postgres, kafka-ui
```

---

## Step 5: Setup Snowflake (3 min)

Login to https://snowflake.com, then run this SQL:

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS FINTECH_ANALYTICS;

-- Create schema
CREATE SCHEMA IF NOT EXISTS FINTECH_ANALYTICS.RAW;

-- Create table
CREATE TABLE IF NOT EXISTS FINTECH_ANALYTICS.RAW.MARKET_PRICES (
    ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    SYMBOL VARCHAR,
    PRICE NUMBER(18, 8),
    EVENT_TIME TIMESTAMP_NTZ,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    KAFKA_TOPIC VARCHAR,
    KAFKA_PARTITION NUMBER,
    KAFKA_OFFSET NUMBER,
    PAYLOAD VARIANT
);

-- Verify
SELECT * FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES LIMIT 5;
```

---

## Step 6: Run Producer (Keep Terminal Open) (1 min)

```bash
# In stock-market directory
source venv/bin/activate

# Run producer
python producer.py

# Should show:
# Sending: {'symbol': 'BTCUSDT', 'price': 42500.50, ...}
# Sending: {'symbol': 'ETHUSDT', 'price': 2500.25, ...}

# Keep this running in background!
```

---

## Step 7: Run Kafka Consumer (New Terminal) (1 min)

```bash
# In new terminal
source venv/bin/activate

# Run consumer
python fintech-airflow/dags/scripts/kafka_consumer.py

# Should show:
# Queued message offset 0: {'symbol': 'BTCUSDT', ...}
# Inserted 100 rows into Snowflake.
```

---

## Step 8: Verify Data in Snowflake (1 min)

Login to Snowflake and run:

```sql
SELECT * FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES 
ORDER BY INGESTED_AT DESC 
LIMIT 10;
```

Should see data! ✅

---

## Done! 🎉

You have:
- ✅ Docker running (Kafka, Zookeeper)
- ✅ Producer sending prices from Binance
- ✅ Consumer reading from Kafka
- ✅ Data stored in Snowflake

### Next: Try This

**View Kafka Messages:**
```bash
# In new terminal
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server localhost:29092 --topic market_prices --max-messages 5
```

**View Kafka UI (Visual):**
```bash
# Open browser
http://localhost:8080
```

**Check Message Count:**
```bash
# In Snowflake
SELECT COUNT(*) FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES;
```

---

## Optional: Setup Airflow (Auto-Scheduling)

Want Airflow to run everything automatically on a schedule?

```bash
# In fintech-airflow directory
cd fintech-airflow

# Start Airflow
astro dev start

# Opens: http://localhost:8080
# Username: admin
# Password: admin

# Or without astro:
airflow db init
airflow webserver &
airflow scheduler &
```

---

## Optional: Setup Power BI (Dashboards)

Want pretty dashboards?

See: [POWERBI_SETUP.md](POWERBI_SETUP.md)

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Docker not starting | `sudo systemctl start docker` |
| Kafka connection refused | `docker ps` (verify running) |
| No messages | `python producer.py` (start producer) |
| Snowflake error | Check username/password in .env |
| "Module not found" | `pip install -r fintech-airflow/requirements.txt` |

More issues? See: [TROUBLESHOOTING_GUIDE.md](TROUBLESHOOTING_GUIDE.md)

---

## Full Documentation

- **Complete Guide**: [COMPLETE_GUIDE.md](COMPLETE_GUIDE.md)
- **Troubleshooting**: [TROUBLESHOOTING_GUIDE.md](TROUBLESHOOTING_GUIDE.md)
- **Power BI Setup**: [POWERBI_SETUP.md](POWERBI_SETUP.md)

---

## Commands Cheat Sheet

```bash
# Docker
docker ps                       # List containers
docker-compose up -d           # Start services
docker-compose down            # Stop services
docker logs kafka              # View logs

# Kafka
docker exec kafka bash
kafka-topics --bootstrap-server localhost:29092 --list

# Python/Producer
source venv/bin/activate
python producer.py

# Snowflake Query
# Login at https://snowflake.com

# Airflow
astro dev start
airflow dags list
airflow dags trigger fintech_realtime_pipeline
```

---

**Time to First Data: ~15 minutes** ⚡
