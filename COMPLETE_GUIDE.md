# Complete Stock Market Real-Time Data Pipeline Guide

## What We Built - Project Overview

Hey! So basically, we built a **real-time stock market data pipeline** that:
1. **Collects** live stock prices from Binance API
2. **Sends** them to Kafka (a streaming platform)
3. **Consumes** the data from Kafka
4. **Stores** it in Snowflake (a cloud data warehouse)
5. **Visualizes** it in Power BI dashboards

Think of it like this:
- **Producer** = Getting live price data from Binance API
- **Kafka** = A fast highway to transport the data
- **Snowflake** = A database to store everything
- **Airflow** = A scheduler that automates all the steps
- **Power BI** = Beautiful dashboards to see the data

```
┌──────────────┐      ┌────────┐       ┌────────────┐      ┌──────────┐      ┌──────────┐
│  Binance API │─────▶│ Kafka  │──────▶│ Snowflake  │─────▶│ Airflow  │─────▶│ Power BI │
│(Stock Prices)│      │ Topics │       │(Data Store)│      │Scheduler │      │Dashboard│
└──────────────┘      └────────┘       └────────────┘      └──────────┘      └──────────┘
```

---

## Part 1: Prerequisites & Installation

### What You Need (Before Starting)

1. **Git** - For version control
2. **Docker** - For containers (Kafka, Postgres, etc.)
3. **Docker Compose** - For running multiple containers
4. **Python 3.8+** - For running scripts and Airflow
5. **A Snowflake Account** - For data warehouse (free trial available)
6. **A Power BI Account** - For dashboards (optional but recommended)

### Step 1: Install Docker & Docker Compose

**On Ubuntu/Debian:**
```bash
# Update system packages
sudo apt-get update
sudo apt-get upgrade -y

# Install Docker
sudo apt-get install -y docker.io

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify installation
docker --version
docker-compose --version
```

**On Mac (using Homebrew):**
```bash
brew install docker docker-compose
```

**On Windows:**
Download Docker Desktop from https://www.docker.com/products/docker-desktop

### Step 2: Clone/Create Project

```bash
# Create project directory
mkdir stock-market
cd stock-market

# Initialize git (optional)
git init
```

### Step 3: Create Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip
```

### Step 4: Install Python Packages

```bash
# Basic packages for producer
pip install kafka-python requests

# For Airflow (optional locally, but needed in Docker)
pip install apache-airflow

# For Snowflake
pip install snowflake-connector-python

# For Power BI
pip install powerbi-client msal
```

---

## Part 2: Project File Structure

After everything is set up, your project should look like this:

```
stock-market/
├── docker-compose.yml          # Starts all services (Kafka, Zookeeper, Postgres, etc.)
├── producer.py                 # Sends stock prices from Binance to Kafka
├── .env                        # Your secrets and configuration
├── .env.powerbi.template       # Power BI configuration template
│
├── fintech-airflow/            # Airflow project (automated scheduler)
│   ├── dags/
│   │   ├── fintech_pipeline.py        # Main Airflow DAG
│   │   ├── powerbi_dag.py             # Power BI refresh DAG
│   │   └── scripts/
│   │       ├── kafka_consumer.py      # Reads Kafka → writes to Snowflake
│   │       ├── powerbi_config.py      # Power BI configuration
│   │       └── powerbi_setup.py       # Power BI initialization
│   ├── requirements.txt        # Python packages needed
│   ├── Dockerfile             # Docker image config
│   └── airflow_settings.yaml  # Airflow connections
│
├── COMPLETE_GUIDE.md          # This file!
├── POWERBI_SETUP.md           # Power BI detailed guide
└── POWERBI_QUICKREF.md        # Power BI quick reference
```

---

## Part 3: Docker Containers Explained

### What Docker Does For Us

Instead of installing Kafka, Zookeeper, Postgres on your machine (messy!), Docker runs them in isolated containers. Think of containers like lightweight virtual machines.

### Our Containers (in docker-compose.yml)

| Container | What It Does | Port |
|-----------|-------------|------|
| **Zookeeper** | Helps Kafka coordinate | 2181 |
| **Kafka** | Message queue/streaming | 9092, 39092 |
| **Postgres** | Database for Debezium CDC | 5433 |
| **Kafka Connect** | Streams changes from databases | 8083 |
| **Kafka UI** | Visual interface to see Kafka topics | 8080 |

### Starting the Containers

```bash
# From the stock-market directory
cd /home/lap-46/Desktop/stock-market

# Start all containers
docker-compose up -d

# Check status
docker ps

# View logs
docker logs kafka
docker logs zookeeper

# Stop everything
docker-compose down
```

**Common Issues & Fixes:**

❌ **"Permission denied while trying to connect to Docker daemon"**
```bash
# Fix: Add your user to docker group
sudo usermod -aG docker $USER
# Then logout and login
```

❌ **"Port 9092 is already in use"**
```bash
# Find what's using it
sudo lsof -i :9092
# Kill the process
sudo kill <PID>

# Or change port in docker-compose.yml
# Change "9092:9092" to "9093:9092"
```

❌ **"Containers keep restarting"**
```bash
# Check logs
docker-compose logs

# Rebuild images
docker-compose down -v  # Removes volumes
docker-compose up -d --build
```

---

## Part 4: The Producer (Collecting Stock Prices)

### What producer.py Does

It runs continuously and:
1. Calls Binance API every 5 seconds
2. Gets current prices for BTCUSDT, ETHUSDT
3. Sends them to Kafka topic "market_prices"

### producer.py Code Explained

```python
from kafka import KafkaProducer
import json

# Connect to Kafka running on localhost
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Where Kafka is running
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Convert to JSON
)

# Symbols we're tracking
SYMBOLS = ["BTCUSDT", "ETHUSDT"]

# Keep running forever
while True:
    for symbol in SYMBOLS:
        # Get price from Binance API
        price = get_price(symbol)  
        
        # Create event
        event = {
            "symbol": symbol,
            "price": price,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Send to Kafka
        producer.send("market_prices", value=event)
```

### Running the Producer

```bash
# Make sure you're in stock-market directory and venv is activated
source venv/bin/activate

# Make sure Docker containers are running
docker ps  # Should see kafka, zookeeper, etc.

# Run the producer
python producer.py
```

**Output should look like:**
```
Sending: {'symbol': 'BTCUSDT', 'price': 42500.50, 'timestamp': '2024-01-15 10:30:45'}
Sending: {'symbol': 'ETHUSDT', 'price': 2500.25, 'timestamp': '2024-01-15 10:30:45'}
```

### Troubleshooting Producer

❌ **"Connection refused"**
- Kafka is not running
- Fix: Run `docker-compose up -d`

❌ **"ModuleNotFoundError: No module named 'kafka'"**
- kafka-python not installed
- Fix: `pip install kafka-python`

❌ **"Connection to Binance API failed"**
- Internet issue or Binance is down
- Fix: Check internet, or use mock data temporarily

---

## Part 5: Kafka Explained

### What is Kafka?

Think of Kafka like a **mailbox**:
- **Producer** puts messages in (stock prices)
- **Topic** is the mailbox name ("market_prices")
- **Consumer** reads the messages

### Key Kafka Concepts

| Term | Meaning |
|------|---------|
| **Topic** | Like a channel or mailbox (we have "market_prices") |
| **Partition** | Splits topic data across multiple brokers for speed |
| **Consumer Group** | Multiple readers working together |
| **Broker** | Kafka server (we have 1 in docker-compose) |
| **Message** | Single data point (one stock price) |

### Checking Kafka Status

```bash
# Enter Kafka container
docker exec -it kafka bash

# List topics
kafka-topics --bootstrap-server localhost:29092 --list

# Check topic details
kafka-topics --bootstrap-server localhost:29092 --describe --topic market_prices

# View messages in topic
kafka-console-consumer --bootstrap-server localhost:29092 --topic market_prices --from-beginning
```

### Using Kafka UI (Visual Interface)

Open your browser: http://localhost:8080

You'll see:
- All Kafka topics
- Messages in each topic
- Consumer groups
- Partition details

Super useful for debugging!

---

## Part 6: Snowflake Setup

### What is Snowflake?

Snowflake is a **cloud data warehouse**. Instead of storing data in files, we store it in tables that we can query with SQL.

### Creating Snowflake Account

1. Go to https://signup.snowflake.com
2. Sign up (free trial = $400 credit!)
3. Choose cloud provider (AWS, Azure, GCP) and region
4. Verify email
5. Set username and password

### Getting Snowflake Credentials

After login, you need:
- **Account Identifier**: In account URL like `xy12345.us-east-1`
- **Username**: What you created
- **Password**: What you created
- **Warehouse**: Default is `COMPUTE_WH`
- **Database**: We'll create `FINTECH_ANALYTICS`

### Creating .env File

In your `stock-market` directory, create `.env`:

```bash
# Snowflake Configuration
SNOWFLAKE_ACCOUNT=xy12345.us-east-1.snowflakecomputing.com
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=FINTECH_ANALYTICS
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=ACCOUNTADMIN

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:39092
KAFKA_TOPIC=market_prices
KAFKA_CONSUMER_GROUP=market_prices_snowflake_sink
```

⚠️ **IMPORTANT: Never commit .env to Git!** It has passwords!

### Creating Snowflake Tables

Connect to Snowflake and run this SQL:

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS FINTECH_ANALYTICS;

-- Create schema
CREATE SCHEMA IF NOT EXISTS FINTECH_ANALYTICS.RAW;

-- Create table for market prices
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

-- Verify table
SELECT * FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES LIMIT 5;
```

---

## Part 7: Airflow Setup & Running

### What is Airflow?

Airflow is an **automation scheduler**. Instead of running commands manually, you define workflows (DAGs) that run automatically on a schedule.

Example:
- 8:00 AM - Check Kafka
- 8:05 AM - Read data from Kafka
- 8:10 AM - Load into Snowflake
- 8:15 AM - Validate data
- All happens **automatically every hour**

### Airflow Project Structure

```
fintech-airflow/
├── dags/                        # Workflow definitions
│   ├── fintech_pipeline.py      # Main ETL workflow
│   ├── powerbi_dag.py           # Power BI refresh workflow
│   └── scripts/
│       ├── kafka_consumer.py    # Read Kafka → Snowflake
│       ├── powerbi_config.py
│       └── powerbi_setup.py
├── requirements.txt             # Python packages
├── airflow_settings.yaml        # Connections & variables
└── Dockerfile                   # Docker configuration
```

### Starting Airflow

```bash
# Enter the airflow directory
cd fintech-airflow

# Start Airflow with Astronomer
astro dev start

# Or with basic airflow
airflow db init
airflow webserver --port 8080 &
airflow scheduler &
```

**First time starting Airflow:**
- Takes 2-3 minutes to initialize
- Opens http://localhost:8080 automatically
- Username: `admin`
- Password: `admin`

### Understanding Our DAGs

#### DAG 1: fintech_pipeline.py

This runs **every hour** and does:
1. **check_kafka** - Verifies Kafka is running
2. **load_market_prices_to_snowflake** - Reads from Kafka, writes to Snowflake
3. **validate_output** - Checks data was loaded successfully

Code preview:
```python
with DAG(
    dag_id="fintech_realtime_pipeline",
    schedule="@hourly",          # Runs every hour
    start_date=datetime(2024, 1, 1),
    catchup=False
):
    check_kafka >> load_snowflake >> validate
```

#### DAG 2: powerbi_dag.py

This runs **every hour** and refreshes Power BI dashboards:
1. **setup_powerbi** - Ensures Power BI is configured
2. **refresh_market_prices_dataset** - Refreshes data
3. **validate_dashboard_data** - Checks Snowflake data is fresh
4. **generate_dashboard_summary** - Logs metrics

### Monitoring DAGs in Airflow UI

1. Open http://localhost:8080
2. Click "DAGs" on left menu
3. Click a DAG to see:
   - Task history
   - Logs for each task
   - Run duration
   - Success/failure status

### Common Airflow Issues

❌ **"DAG not appearing in Airflow UI"**
- Python syntax error in DAG file
- Fix: Check console output or logs
```bash
airflow dags list -v  # Shows errors
```

❌ **"Task failed with error"**
- Click task → View logs
- Common causes:
  - Kafka not running
  - Snowflake credentials wrong
  - Network issue

Fix:
```bash
# Check logs
docker logs airflow-scheduler

# Test Kafka connection
docker exec -it kafka bash
kafka-topics --bootstrap-server localhost:29092 --list
```

❌ **"ImportError: No module named 'snowflake'"**
- Snowflake package not installed
- Fix: Add to `requirements.txt` and rebuild

---

## Part 8: kafka_consumer.py Explained

### What It Does

This script is the **bridge** between Kafka and Snowflake:
1. Listens to Kafka topic "market_prices"
2. Reads stock price messages
3. Inserts them into Snowflake table

### How It Works

```python
# 1. Connect to Kafka
consumer = KafkaConsumer(
    "market_prices",  # Topic name
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=CONSUMER_GROUP,
    auto_offset_reset='latest'  # Start from latest message
)

# 2. Listen for messages
for message in consumer:
    # 3. Normalize the data
    symbol = message.value['symbol']
    price = message.value['price']
    
    # 4. Insert into Snowflake
    insert_market_prices(cursor, [row])
```

### Environment Variables It Needs

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:39092
KAFKA_TOPIC=market_prices
KAFKA_MAX_MESSAGES=100  # How many to read before stopping

# Snowflake
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_DATABASE=FINTECH_ANALYTICS
SNOWFLAKE_SCHEMA=RAW
```

### Running Manually (for testing)

```bash
cd fintech-airflow/dags/scripts

# Make sure .env is set up
source /path/to/.env

# Run
python kafka_consumer.py
```

**Expected output:**
```
[2024-01-15 10:30:45] Starting Kafka consumer...
[2024-01-15 10:30:45] Connecting to Kafka at host.docker.internal:39092
[2024-01-15 10:30:45] Subscribing to topic: market_prices
[2024-01-15 10:30:46] Listening for messages...
[2024-01-15 10:30:47] Queued message offset 0: {'symbol': 'BTCUSDT', 'price': 42500.50, ...}
[2024-01-15 10:30:47] Inserted 100 rows into Snowflake.
```

### Troubleshooting

❌ **"NoBrokersAvailable"**
- Kafka is not running
- Fix: `docker-compose up -d kafka`

❌ **"connection refused" for Snowflake**
- Wrong account name or password
- Fix: Check .env file, verify Snowflake login works

❌ **"No messages received"**
- Topic is empty (producer hasn't sent data yet)
- Fix: Run producer.py first, then consumer

---

## Part 9: Setting Up Power BI

### What Power BI Does

Power BI is a **visualization tool**. It:
1. Connects to your Snowflake data
2. Creates interactive charts and dashboards
3. Refreshes automatically with latest data

### Step 1: Get Power BI Account

1. Go to https://app.powerbi.com
2. Sign up with work email (free with Office 365)
3. Or buy Power BI Pro ($10/month)

### Step 2: Create Azure AD App Registration

Power BI API needs credentials. Here's how to get them:

1. Go to https://portal.azure.com
2. Search "App registrations"
3. Click "New registration"
4. Name it "Fintech-PowerBI-Integration"
5. Click "Register"
6. Copy the **Client ID** (this is POWERBI_CLIENT_ID)
7. Click "Certificates & secrets" → "New client secret"
8. Copy the secret (this is POWERBI_CLIENT_SECRET)
9. Go to "API permissions" → "Add permission"
10. Search "Power BI Service" → Select it
11. Choose "Dataset.ReadWrite.All" permission

Save these values:
- Tenant ID (Azure AD → Overview → Directory ID)
- Client ID (from above)
- Client Secret (from above)

### Step 3: Create .env.powerbi File

```bash
# Copy the template
cp .env.powerbi.template .env.powerbi

# Edit with your values
nano .env.powerbi
```

Fill in:
```bash
POWERBI_TENANT_ID=your-tenant-id
POWERBI_CLIENT_ID=your-client-id
POWERBI_CLIENT_SECRET=your-client-secret
POWERBI_WORKSPACE_ID=your-workspace-id

SNOWFLAKE_ACCOUNT=your-account.snowflakecomputing.com
SNOWFLAKE_DATABASE=FINTECH_ANALYTICS
SNOWFLAKE_SCHEMA=RAW
```

### Step 4: Initialize Power BI

```bash
cd fintech-airflow/dags/scripts
python powerbi_setup.py
```

This creates:
- Dataset configuration for market prices
- Dashboard JSON template
- Saves to `/tmp/powerbi_dashboard.json`

### Step 5: Create Snowflake Connection in Power BI Desktop

1. Download Power BI Desktop (free)
2. Open it
3. "Get Data" → Search "Snowflake"
4. Enter:
   - Server: Your Snowflake account URL
   - Database: FINTECH_ANALYTICS
   - Import Mode: DirectQuery (for real-time)
5. Click "Load"

### Step 6: Build Dashboards

Using the data from Snowflake, create visualizations:

1. **Line Chart**: Average Price Trend
   - X-axis: EVENT_TIME (by date)
   - Y-axis: PRICE (average)
   - Shows price movement over time

2. **Bar Chart**: Price by Symbol
   - X-axis: SYMBOL
   - Y-axis: PRICE (average)
   - Compare prices across coins

3. **Card**: Total Messages
   - Just count of rows
   - Shows "42,500 messages ingested"

4. **Table**: Latest Prices
   - Show columns: SYMBOL, PRICE, EVENT_TIME
   - Sort by INGESTED_AT descending
   - Top 20 rows

5. **Pie Chart**: Partition Distribution
   - Categories: KAFKA_PARTITION
   - Values: Count of messages
   - See data distribution

### Step 7: Publish to Power BI Service

1. "File" → "Publish"
2. Select workspace
3. Click "Publish"
4. Go to Power BI Service (online)
5. Dashboard is now shared!

### Step 8: Enable Automatic Refresh

1. Power BI Service → Settings ⚙️
2. "Datasets" section
3. Click your dataset
4. "Scheduled Refresh"
5. Set to daily or hourly
6. Or use Airflow DAG (powerbi_dag.py) for automatic refresh

---

## Part 10: Complete Workflow (All Together)

### Running Everything Step-by-Step

**Step 1: Start Docker Containers (5 seconds)**
```bash
cd /home/lap-46/Desktop/stock-market
docker-compose up -d
docker ps  # Verify all running
```

**Step 2: Start Airflow (2 minutes)**
```bash
cd fintech-airflow
astro dev start
# Or: airflow webserver & airflow scheduler &
```

**Step 3: Start Producer (3 seconds)**
```bash
# In a new terminal
cd /home/lap-46/Desktop/stock-market
source venv/bin/activate
python producer.py
# Keep this running!
```

**Step 4: Monitor Kafka (Check messages are flowing)**
```bash
# In another terminal
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server localhost:29092 --topic market_prices --from-beginning
```

**Step 5: Check Airflow UI (View automation)**
```
Open http://localhost:8080
Watch DAGs run automatically every hour
```

**Step 6: Check Snowflake (Verify data loaded)**
```sql
SELECT * FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES 
ORDER BY INGESTED_AT DESC 
LIMIT 10;
```

**Step 7: Check Power BI Dashboard (See visualizations)**
```
Open Power BI Service
View your dashboard refreshing with latest data
```

### Data Flow Diagram

```
┌─────────────────────────────────────────────────────────┐
│                  PRODUCER (producer.py)                 │
│  Binance API → Gets price every 5 seconds               │
└────────────────────────────┬────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────┐
│                    KAFKA (Docker)                       │
│  Topic: "market_prices"                                 │
│  - Partition 0: Some messages                           │
│  - Partition 1: Some messages                           │
│  Viewed at: http://localhost:8080                       │
└────────────────────────────┬────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────┐
│              AIRFLOW DAG (Every Hour)                   │
│  1. Check Kafka ✓                                       │
│  2. Read Kafka Messages                                 │
│  3. Validate Output                                     │
└────────────────────────────┬────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────┐
│         KAFKA CONSUMER (kafka_consumer.py)              │
│  Reads from Kafka → Processes → Inserts into Snowflake │
└────────────────────────────┬────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────┐
│              SNOWFLAKE (Cloud DB)                       │
│  Database: FINTECH_ANALYTICS                            │
│  Table: RAW.MARKET_PRICES                               │
│  - SYMBOL, PRICE, EVENT_TIME, INGESTED_AT, etc         │
│  Contains: 100,000+ rows of stock prices                │
└────────────────────────────┬────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────┐
│              POWER BI DASHBOARD                         │
│  Live Visualizations:                                   │
│  - Price Trends (Line Chart)                            │
│  - Price by Symbol (Bar Chart)                          │
│  - Message Count (Card)                                 │
│  - Latest Prices (Table)                                │
│  Refreshes hourly automatically                         │
└─────────────────────────────────────────────────────────┘
```

---

## Part 11: Common Errors & How to Fix Them

### Error #1: "Cannot connect to Docker daemon"

**What it means:** Docker is not running

**How to fix:**
```bash
# Start Docker
sudo systemctl start docker  # Linux
# Or open Docker Desktop app (Mac/Windows)

# Verify it's running
docker ps
```

---

### Error #2: "Connection refused" when running producer.py

**What it means:** Kafka is not running or producer is trying wrong address

**How to fix:**
```bash
# Check Kafka is running
docker ps | grep kafka

# If not running:
docker-compose up -d kafka

# Check producer.py for correct address
nano producer.py
# Should have: bootstrap_servers='localhost:9092'
```

---

### Error #3: "No module named 'kafka'" or 'snowflake' or 'airflow'

**What it means:** Python packages not installed

**How to fix:**
```bash
# Activate virtual environment
source venv/bin/activate

# Install missing packages
pip install kafka-python
pip install snowflake-connector-python
pip install apache-airflow

# Or install all from requirements
pip install -r fintech-airflow/requirements.txt
```

---

### Error #4: Airflow DAG not showing in UI

**What it means:** DAG has syntax error or can't be found

**How to fix:**
```bash
# Check for syntax errors
python -m py_compile fintech-airflow/dags/fintech_pipeline.py

# Check Airflow can find it
airflow dags list -v

# Check logs
airflow dags test fintech_realtime_pipeline

# DAGs must be in fintech-airflow/dags/ folder
ls fintech-airflow/dags/
```

---

### Error #5: Snowflake connection fails with "Invalid account name"

**What it means:** Wrong Snowflake account identifier

**How to fix:**
```bash
# Check your .env file
cat .env | grep SNOWFLAKE_ACCOUNT

# Should look like: xy12345.us-east-1.snowflakecomputing.com
# NOT: xy12345.us-east-1 (missing .snowflakecomputing.com)

# Get correct one from Snowflake:
# 1. Log in to https://snowflake.com
# 2. Look at URL: https://[ACCOUNT_ID].snowflakecomputing.com
```

---

### Error #6: "Consumer timeout - no messages received"

**What it means:** Kafka topic is empty, no data from producer

**How to fix:**
```bash
# Check producer is running
ps aux | grep producer.py

# If not running:
cd /home/lap-46/Desktop/stock-market
python producer.py  # Keep this terminal open

# Wait 5-10 seconds for messages

# Verify messages in Kafka
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server localhost:29092 --topic market_prices --max-messages 5
```

---

### Error #7: Power BI dataset not refreshing

**What it means:** Power BI can't connect to Snowflake or refresh failed

**How to fix:**
```bash
# Check Snowflake is running and has data
SELECT COUNT(*) FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES;

# Verify credentials in Power BI
# File → Options → Data Source Settings
# Check Snowflake account, user, password

# In Power BI Service:
# Settings → Datasets → Your Dataset → Refresh Now
# Check logs for errors
```

---

### Error #8: Port already in use (Port 8080, 9092, etc)

**What it means:** Another application is using the port

**How to fix:**
```bash
# Find what's using port 8080
sudo lsof -i :8080

# Kill the process
sudo kill <PID>

# Or change port in docker-compose.yml
# Change: "8080:8080" to "8081:8080"
# Change: "9092:9092" to "9093:9092"

# Then restart
docker-compose down
docker-compose up -d
```

---

### Error #9: "Host.docker.internal: Name or service not known"

**What it means:** Docker can't resolve hostname (Linux-specific issue)

**How to fix (Linux):**
```bash
# Add to docker-compose.yml in services section:
# extra_hosts:
#   - "host.docker.internal:host-gateway"

# For Kafka service, add:
kafka:
  ...
  extra_hosts:
    - "host.docker.internal:host-gateway"
  ...

# Then restart
docker-compose down
docker-compose up -d
```

**Or use IP address instead:**
```bash
# Find your machine IP
hostname -I

# Use that in KAFKA_BOOTSTRAP_SERVERS
# Instead of: host.docker.internal:39092
# Use: 192.168.x.x:39092
```

---

### Error #10: Airflow scheduler not running/DAGs not triggering

**What it means:** Scheduler is stopped or has error

**How to fix:**
```bash
# Check if scheduler is running
ps aux | grep scheduler

# If not, start it
airflow scheduler &

# Or with astro
astro dev start

# Check scheduler logs
airflow logs -f

# Verify DAG has start date in past
# In your DAG: start_date=datetime(2024, 1, 1)
# Should be before today's date

# Force trigger manually (for testing)
airflow dags trigger fintech_realtime_pipeline
```

---

## Part 12: Monitoring & Troubleshooting

### How to Monitor Everything

#### 1️⃣ Check Docker Containers

```bash
# See all running containers
docker ps

# See container logs (live)
docker logs -f kafka

# Check container stats (CPU, memory)
docker stats

# Enter container to debug
docker exec -it kafka bash
```

#### 2️⃣ Check Kafka Topics

```bash
# List all topics
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# See messages in topic (last 10)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic market_prices --max-messages 10

# See topic details
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --describe --topic market_prices
```

#### 3️⃣ Check Airflow DAGs

```bash
# List DAGs
airflow dags list

# List DAG runs
airflow dags list-runs -d fintech_realtime_pipeline

# Test DAG
airflow dags test fintech_realtime_pipeline

# Check DAG logs
airflow logs -d fintech_realtime_pipeline

# Trigger manually
airflow dags trigger fintech_realtime_pipeline
```

#### 4️⃣ Check Snowflake Data

```sql
-- Count rows
SELECT COUNT(*) FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES;

-- See latest data
SELECT * FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES 
ORDER BY INGESTED_AT DESC LIMIT 10;

-- Check data freshness
SELECT MAX(INGESTED_AT) as latest_data FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES;

-- See by symbol
SELECT SYMBOL, COUNT(*) FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES 
GROUP BY SYMBOL;
```

#### 5️⃣ Check Kafka UI

```
Open browser → http://localhost:8080

See:
- Topics
- Messages
- Consumer groups
- Brokers status
```

---

## Part 13: Performance Tips & Best Practices

### 1. Kafka Configuration

```bash
# In docker-compose.yml, adjust based on data volume
KAFKA_HEAP_OPTS: "-Xms256m -Xmx512m"  # Increase if needed
```

### 2. Snowflake Performance

```sql
-- Create indexes for faster queries
CREATE INDEX idx_symbol ON FINTECH_ANALYTICS.RAW.MARKET_PRICES(SYMBOL);
CREATE INDEX idx_event_time ON FINTECH_ANALYTICS.RAW.MARKET_PRICES(EVENT_TIME);

-- Partition by date (optional)
ALTER TABLE FINTECH_ANALYTICS.RAW.MARKET_PRICES 
CLUSTER BY DATE(EVENT_TIME);
```

### 3. Airflow Optimization

```yaml
# In airflow_settings.yaml
core:
  max_active_tasks_per_dag: 16
  parallelism: 32
```

### 4. Power BI Dashboards

- Use **DirectQuery** for real-time (slower but current)
- Use **Import** for speed (refreshed hourly)
- Create **aggregations** in Snowflake for faster queries

---

## Part 14: Security Best Practices

### Never Do This ❌

```bash
# Don't commit .env to Git
git add .env          # NO!

# Don't hardcode passwords
password = "mypassword123"  # NO!

# Don't share credentials in logs
print(SNOWFLAKE_PASSWORD)   # NO!
```

### Do This Instead ✅

```bash
# Use .env file (not in Git)
cat .env
SNOWFLAKE_PASSWORD=xyz123

# Use environment variables
export SNOWFLAKE_PASSWORD=$(cat ~/.secrets/snowflake_pw)

# Use .gitignore
echo ".env" >> .gitignore
echo ".env.local" >> .gitignore

# Use secrets manager (for production)
# AWS Secrets Manager, Vault, etc.
```

### Snowflake Security

```sql
-- Create dedicated user for Airflow
CREATE USER airflow_user 
  PASSWORD = 'very_strong_password_123!@#'
  DEFAULT_ROLE = 'ANALYST'
  DEFAULT_WAREHOUSE = 'COMPUTE_WH';

-- Grant minimal permissions
GRANT USAGE ON DATABASE FINTECH_ANALYTICS TO ROLE ANALYST;
GRANT USAGE ON SCHEMA FINTECH_ANALYTICS.RAW TO ROLE ANALYST;
GRANT SELECT, INSERT ON TABLE FINTECH_ANALYTICS.RAW.MARKET_PRICES TO ROLE ANALYST;
```

---

## Part 15: Next Steps & Enhancements

### What You Can Add Next

1. **Real-time Alerting**
   ```python
   # Send alert if price drops > 5%
   if (old_price - new_price) / old_price > 0.05:
       send_alert("Price dropped significantly!")
   ```

2. **Data Quality Checks**
   ```python
   # Check data is not null
   if price is None or symbol is None:
       raise ValueError("Invalid data!")
   ```

3. **Multiple Data Sources**
   ```python
   # Add more symbols
   SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT"]
   
   # Add more exchanges
   # Coinbase, Kraken, FTX, etc.
   ```

4. **ML Models**
   ```python
   # Predict next price using historical data
   from sklearn.ensemble import RandomForestRegressor
   model = RandomForestRegressor()
   model.fit(historical_prices, next_prices)
   ```

5. **API Endpoint**
   ```python
   # Create Flask API to query latest prices
   from flask import Flask
   app = Flask(__name__)
   
   @app.route('/prices')
   def get_prices():
       return query_snowflake()
   ```

6. **Slack/Email Notifications**
   ```python
   # Alert when new data loaded
   send_slack_message("Loaded 500 new prices!")
   send_email("Daily Market Report", report_html)
   ```

---

## Part 16: Command Reference

### Quick Command Cheat Sheet

```bash
# Docker
docker ps                          # List running containers
docker logs <container>            # View logs
docker exec -it <container> bash   # Enter container
docker-compose up -d               # Start all services
docker-compose down                # Stop all services
docker-compose logs -f             # Stream logs

# Airflow
airflow dags list                  # List DAGs
airflow dags trigger <dag>         # Run DAG manually
airflow logs -d <dag>              # View logs
astro dev start                    # Start with Astronomer
astro dev stop                     # Stop with Astronomer

# Python
python3 -m venv venv               # Create virtual environment
source venv/bin/activate           # Activate venv
pip install -r requirements.txt    # Install packages
python producer.py                 # Run producer
python -m py_compile file.py       # Check syntax

# Kafka
kafka-topics --list                # List topics
kafka-console-consumer             # Read messages
kafka-topics --describe            # Topic details

# Git
git init                           # Initialize repo
git add .                          # Stage changes
git commit -m "message"            # Commit
git push                           # Push to remote
echo ".env" >> .gitignore          # Add to gitignore
```

---

## Part 17: Testing Your Setup

### Test Checklist

Run through this to make sure everything works:

```bash
# 1. Docker is running
docker ps
# ✅ Should see containers: kafka, zookeeper, postgres, etc.

# 2. Kafka is working
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list
# ✅ Should see: market_prices

# 3. Producer connects to Kafka
python producer.py
# ✅ Should see: Sending: {'symbol': 'BTCUSDT', ...}
# Let it run for 10 seconds, then Ctrl+C

# 4. Messages in Kafka
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic market_prices --max-messages 5
# ✅ Should see: {'symbol': 'BTCUSDT', 'price': 42500.50, ...}

# 5. Snowflake connection
python fintech-airflow/dags/scripts/kafka_consumer.py
# ✅ Should see: Inserted X rows into Snowflake

# 6. Check data in Snowflake
# Login to Snowflake and run:
SELECT COUNT(*) FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES;
# ✅ Should see: Row count > 0

# 7. Airflow is running
open http://localhost:8080
# ✅ Should see: Airflow UI with DAGs

# 8. Check Power BI connection
# In Power BI Desktop, connect to Snowflake
# ✅ Should see: MARKET_PRICES table
```

---

## Part 18: Conclusion

### What You've Built

You've created a **production-grade data pipeline** that:
- ✅ Collects real-time stock prices
- ✅ Streams through Kafka
- ✅ Stores in Snowflake
- ✅ Automates with Airflow
- ✅ Visualizes in Power BI

### Key Files to Remember

| File | Purpose |
|------|---------|
| `producer.py` | Collects data from Binance |
| `docker-compose.yml` | Runs Kafka, Zookeeper, Postgres |
| `fintech-airflow/dags/fintech_pipeline.py` | Main Airflow workflow |
| `fintech-airflow/dags/scripts/kafka_consumer.py` | Kafka → Snowflake |
| `fintech-airflow/dags/powerbi_dag.py` | Refresh Power BI |
| `.env` | Your secrets (don't share!) |

### Keeping It Running

```bash
# Monitor everything daily
docker ps
airflow dags list
# Power BI Service → Check dashboard refreshed

# Weekly
# Check Snowflake storage usage
# Review Airflow logs for errors

# Monthly
# Optimize Snowflake queries
# Review Power BI report usage
# Update packages: pip install --upgrade -r requirements.txt
```

### Getting Help

- **Docker Issues**: https://docs.docker.com/
- **Kafka Docs**: https://kafka.apache.org/documentation/
- **Airflow Docs**: https://airflow.apache.org/docs/
- **Snowflake Docs**: https://docs.snowflake.com/
- **Power BI Docs**: https://docs.microsoft.com/power-bi/

---

## Final Notes

This pipeline is **scalable**. As you grow:
- Add more symbols to track
- Add more data sources
- Increase Kafka partitions
- Scale Snowflake warehouse
- Create more Power BI reports

Everything is **containerized** and **automated**, so you can:
- Run on any machine
- Deploy to cloud (AWS, GCP, Azure)
- Scale horizontally
- Monitor with alerts

Good luck! 🚀

---

**Document Version**: 1.0
**Last Updated**: April 2024
**Maintained By**: [Your Name]
