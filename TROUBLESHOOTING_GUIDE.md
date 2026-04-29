# Troubleshooting Guide - Common Issues & Quick Fixes

## Table of Contents
1. [Docker Issues](#docker-issues)
2. [Kafka Issues](#kafka-issues)
3. [Airflow Issues](#airflow-issues)
4. [Producer Issues](#producer-issues)
5. [Snowflake Issues](#snowflake-issues)
6. [Power BI Issues](#power-bi-issues)
7. [Network & Port Issues](#network--port-issues)

---

## Docker Issues

### ❌ "Permission denied while trying to connect to Docker daemon"

**Problem:** You don't have permission to run Docker commands

**Solution:**
```bash
# Add yourself to docker group
sudo usermod -aG docker $USER

# Apply new group settings
newgrp docker

# Verify
docker ps
```

---

### ❌ "Cannot connect to Docker daemon"

**Problem:** Docker daemon is not running

**Solution:**
```bash
# Linux
sudo systemctl start docker
sudo systemctl enable docker  # Auto-start on boot

# Mac
# Just open Docker Desktop app

# Verify
docker ps
```

---

### ❌ "docker-compose: command not found"

**Problem:** Docker Compose not installed

**Solution:**
```bash
# Linux
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Mac (using Homebrew)
brew install docker-compose

# Windows: Download Docker Desktop (includes Compose)

# Verify
docker-compose --version
```

---

### ❌ Containers keep restarting/crashing

**Problem:** Container exits immediately with error

**Solution:**
```bash
# Check logs
docker-compose logs kafka
docker-compose logs zookeeper

# Common cause: Port already in use
sudo lsof -i :9092
sudo kill <PID>

# Rebuild everything from scratch
docker-compose down -v  # Remove volumes too
docker-compose up -d --build

# Check status
docker ps
docker logs kafka  # View details
```

---

### ❌ "Services are running but can't connect"

**Problem:** Docker containers running but can't reach them

**Solution:**
```bash
# Check if containers are actually running
docker ps

# If not all running:
docker-compose up -d

# Test connection from host
nc -zv localhost 9092      # Kafka
nc -zv localhost 2181      # Zookeeper
nc -zv localhost 8080      # Kafka UI

# If nc not available:
docker run --rm -it nicolaka/netshoot bash
# Inside container: nc -zv kafka 29092
```

---

## Kafka Issues

### ❌ "Connection refused" when running producer.py

**Problem:** Kafka broker not reachable

**Solution:**
```bash
# Step 1: Verify Kafka is running
docker ps | grep kafka
# Should see: confluentinc/cp-kafka

# Step 2: Check Kafka logs
docker logs kafka

# Step 3: Verify port is listening
docker exec kafka bash -c "netstat -tlnp | grep 9092"

# Step 4: Test from within Docker
docker exec kafka bash
kafka-broker-api-versions --bootstrap-server localhost:9092

# Step 5: Check producer.py bootstrap servers
cat producer.py | grep bootstrap_servers
# Should match: localhost:9092 or host.docker.internal:39092

# Step 6: If on Linux, add host gateway
# Edit docker-compose.yml:
kafka:
  extra_hosts:
    - "host.docker.internal:host-gateway"

docker-compose down
docker-compose up -d kafka
```

---

### ❌ "NoBrokersAvailable" error

**Problem:** Kafka consumer can't find brokers

**Solution:**
```bash
# Check KAFKA_BOOTSTRAP_SERVERS environment variable
echo $KAFKA_BOOTSTRAP_SERVERS

# Should be set in .env:
cat .env | grep KAFKA_BOOTSTRAP

# If not set:
export KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:39092

# Or set in script directly:
nano fintech-airflow/dags/scripts/kafka_consumer.py
# Check: KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:39092")

# On Linux, might need IP instead of hostname:
# Find your IP: hostname -I
# Change KAFKA_BOOTSTRAP_SERVERS=192.168.x.x:39092
```

---

### ❌ "No messages received" / "Consumer timeout"

**Problem:** Topic is empty, no data from producer

**Solution:**
```bash
# Step 1: Check producer is running
ps aux | grep producer.py

# If not running:
cd /home/lap-46/Desktop/stock-market
source venv/bin/activate
python producer.py
# Keep this terminal open!

# Step 2: Wait for messages (5-10 seconds)

# Step 3: Verify messages are in Kafka
docker exec kafka bash
kafka-console-consumer --bootstrap-server localhost:29092 --topic market_prices --max-messages 5

# Step 4: If still no messages, check producer errors
# Look at terminal running producer.py for error messages

# Step 5: Check internet connection for Binance API
curl https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT
# Should return: {"symbol":"BTCUSDT","price":"42500.50"}

# Step 6: If Binance API down, use test data instead
# Edit producer.py - replace get_price() with mock prices
```

---

### ❌ "Topic does not exist"

**Problem:** Trying to read from topic that wasn't created

**Solution:**
```bash
# Check what topics exist
docker exec kafka bash
kafka-topics --bootstrap-server localhost:29092 --list

# If market_prices doesn't exist:
# Option 1: Let producer create it (Kafka auto-creates by default)
python producer.py
# Will create market_prices topic automatically

# Option 2: Create manually
docker exec kafka bash
kafka-topics --bootstrap-server localhost:29092 --create \
  --topic market_prices \
  --partitions 1 \
  --replication-factor 1

# Verify
kafka-topics --bootstrap-server localhost:29092 --list
```

---

## Airflow Issues

### ❌ DAG not appearing in Airflow UI

**Problem:** DAG file exists but not shown in UI

**Solution:**
```bash
# Step 1: Check DAG file syntax
python -m py_compile fintech-airflow/dags/fintech_pipeline.py
# No output = OK, error = has syntax error

# Step 2: Check DAG location
ls -la fintech-airflow/dags/
# Should see: fintech_pipeline.py, powerbi_dag.py

# Step 3: Check Airflow can find it
airflow dags list -v
# If error shown here, copy error message

# Step 4: Check DAG import errors
airflow dags test fintech_realtime_pipeline
# Will show import errors if any

# Step 5: Common issues:
# - Missing imports: from datetime import datetime
# - Wrong DAG directory (must be fintech-airflow/dags/)
# - Syntax error in file

# Step 6: Restart Airflow scheduler
# With astro:
astro dev stop
astro dev start

# With airflow:
ps aux | grep scheduler
# Kill any scheduler processes
pkill -f scheduler
airflow scheduler &
```

---

### ❌ "Task failed with error"

**Problem:** Task ran but failed

**Solution:**
```bash
# Step 1: View task logs in Airflow UI
# DAGs → fintech_realtime_pipeline → click task → View Logs

# Step 2: Or view logs from command line
airflow logs -d fintech_realtime_pipeline -t load_market_prices_to_snowflake

# Step 3: Common causes:
# - Kafka not running: docker ps | grep kafka
# - Snowflake credentials wrong: cat .env | grep SNOWFLAKE
# - Network issue: Can't connect to services
# - Missing packages: pip install snowflake-connector-python

# Step 4: Test task independently
cd fintech-airflow/dags/scripts
python kafka_consumer.py
# Shows what's failing more clearly

# Step 5: Once fixed, restart Airflow scheduler
# Force re-parse DAGs:
astro dev restart
```

---

### ❌ Scheduler not triggering DAGs

**Problem:** DAG should run hourly but doesn't

**Solution:**
```bash
# Step 1: Check scheduler is running
ps aux | grep scheduler
# Should see: airflow scheduler

# Step 2: If not running, start it
airflow scheduler &

# Step 3: Check DAG has correct schedule
airflow dags list
# Look at "Schedule" column - should show schedule_interval

# Step 4: DAG must have start_date in the PAST
# In fintech_pipeline.py:
# start_date=datetime(2024, 1, 1)  ✅ OK (past)
# start_date=datetime(2099, 1, 1)  ❌ WRONG (future)

# Step 5: Force trigger for testing
airflow dags trigger fintech_realtime_pipeline

# Step 6: Check task instance
airflow dags list-runs -d fintech_realtime_pipeline

# Step 7: View scheduler logs
airflow logs -f  # Follow scheduler logs
```

---

### ❌ "ImportError: No module named" for custom script

**Problem:** DAG can't import your custom modules

**Solution:**
```bash
# Step 1: Make sure script exists
ls fintech-airflow/dags/scripts/kafka_consumer.py

# Step 2: Check imports in DAG
# fintech_pipeline.py should have proper path

# Step 3: Install missing packages
pip install -r fintech-airflow/requirements.txt

# Step 4: If using astro, rebuild Docker image
astro dev stop
astro dev start --build

# Step 5: Add script to PYTHONPATH (if needed)
# In fintech_pipeline.py:
import sys
sys.path.insert(0, '/usr/local/airflow/dags/scripts')
from kafka_consumer import some_function
```

---

## Producer Issues

### ❌ "ModuleNotFoundError: No module named 'kafka'"

**Problem:** kafka-python not installed

**Solution:**
```bash
# Activate virtual environment first
source venv/bin/activate

# Install kafka-python
pip install kafka-python

# Verify
python -c "import kafka; print(kafka.__version__)"
```

---

### ❌ "No module named 'requests'"

**Problem:** requests library not installed

**Solution:**
```bash
source venv/bin/activate
pip install requests

# Verify
python -c "import requests; print('OK')"
```

---

### ❌ Producer runs but sends no messages

**Problem:** Producer connects but no data flowing

**Solution:**
```bash
# Step 1: Check producer output
python producer.py
# Should show: Sending: {'symbol': 'BTCUSDT', ...}

# Step 2: If not showing, check Binance API
curl https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT
# Should return JSON with price

# Step 3: If API returns error, Binance might be down
# Or too many requests - wait a minute, then retry

# Step 4: Check error in producer
# If seeing exception, read the error carefully

# Step 5: Test with hardcoded data
# Edit producer.py:
# Instead of: price = get_price(symbol)
# Use: price = 42500.50

# Then run:
python producer.py

# Step 6: Verify Kafka is receiving
# In another terminal:
docker exec kafka bash
kafka-console-consumer --bootstrap-server localhost:29092 --topic market_prices --max-messages 5
```

---

### ❌ Producer keeps crashing

**Problem:** Producer exits with error

**Solution:**
```bash
# Step 1: Run with verbose error output
python -u producer.py 2>&1 | tee producer.log

# Step 2: Common causes:
# - Invalid JSON: json.loads() error
# - Network timeout: Binance API slow
# - Kafka disconnected: Connection lost

# Step 3: Add error handling
# Edit producer.py - make sure try/except is catching errors

# Step 4: Run in a loop with restart
while true; do
  python producer.py
  echo "Producer crashed, restarting in 5 seconds..."
  sleep 5
done

# Step 5: Or use systemd service/supervisor for auto-restart
```

---

## Snowflake Issues

### ❌ "Connection refused" / "Invalid account name"

**Problem:** Can't connect to Snowflake

**Solution:**
```bash
# Step 1: Check .env file
cat .env | grep SNOWFLAKE_ACCOUNT

# Step 2: Verify account format
# WRONG: xy12345.us-east-1
# RIGHT: xy12345.us-east-1.snowflakecomputing.com

# Step 3: Get correct account from Snowflake
# Login to https://snowflake.com
# Look at browser URL - extract account ID

# Step 4: Test connection manually
python3 << 'EOF'
import snowflake.connector

try:
    conn = snowflake.connector.connect(
        account='YOUR_ACCOUNT_ID',
        user='YOUR_USERNAME',
        password='YOUR_PASSWORD',
        warehouse='COMPUTE_WH'
    )
    print("✅ Connection successful!")
    conn.close()
except Exception as e:
    print(f"❌ Error: {e}")
EOF

# Step 5: If still failing, check:
# - Username is correct
# - Password is correct
# - Account exists and isn't suspended
# - Account is in right region
```

---

### ❌ "User does not have permission"

**Problem:** Connected to Snowflake but no table access

**Solution:**
```bash
# Step 1: Check user role
SELECT CURRENT_ROLE();

# Step 2: Check table permissions
SHOW GRANTS ON TABLE FINTECH_ANALYTICS.RAW.MARKET_PRICES;

# Step 3: Grant permissions (as ACCOUNTADMIN)
-- Login as ACCOUNTADMIN first
USE ROLE ACCOUNTADMIN;

-- Create user if needed
CREATE USER pipeline_user 
PASSWORD = 'Strong_Password_123!';

-- Create role
CREATE ROLE analyst_role;

-- Grant permissions
GRANT USAGE ON DATABASE FINTECH_ANALYTICS TO ROLE analyst_role;
GRANT USAGE ON SCHEMA FINTECH_ANALYTICS.RAW TO ROLE analyst_role;
GRANT SELECT, INSERT ON TABLE FINTECH_ANALYTICS.RAW.MARKET_PRICES TO ROLE analyst_role;

-- Assign role to user
GRANT ROLE analyst_role TO USER pipeline_user;

# Step 4: Use pipeline_user in .env
SNOWFLAKE_USER=pipeline_user
SNOWFLAKE_PASSWORD=Strong_Password_123!
```

---

### ❌ "Table does not exist"

**Problem:** Trying to insert into table that doesn't exist

**Solution:**
```sql
-- Step 1: Check if database exists
SHOW DATABASES LIKE 'FINTECH_ANALYTICS';

-- If not, create it:
CREATE DATABASE FINTECH_ANALYTICS;

-- Step 2: Check if schema exists
USE DATABASE FINTECH_ANALYTICS;
SHOW SCHEMAS LIKE 'RAW';

-- If not, create it:
CREATE SCHEMA RAW;

-- Step 3: Check if table exists
USE SCHEMA FINTECH_ANALYTICS.RAW;
SHOW TABLES LIKE 'MARKET_PRICES';

-- If not, create it:
CREATE TABLE MARKET_PRICES (
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

-- Step 4: Verify
SELECT * FROM MARKET_PRICES LIMIT 1;
```

---

### ❌ No data in Snowflake after running consumer

**Problem:** Consumer ran but no rows inserted

**Solution:**
```bash
# Step 1: Check consumer was actually reading messages
# Look at consumer output - should show:
# "Queued message offset 0: {'symbol': 'BTCUSDT', ...}"

# Step 2: If no messages queued:
# Kafka topic is empty - run producer first
python producer.py  # Let it run for 10 seconds

# Step 3: Check Snowflake for errors
SELECT * FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES;
-- If this errors, table setup failed

# Step 4: Check consumer error logs
python fintech-airflow/dags/scripts/kafka_consumer.py 2>&1 | head -20

# Step 5: Common issues:
# - Wrong Snowflake password
# - Table permissions wrong
# - Kafka messages format doesn't match expected structure

# Step 6: Test with sample data
python3 << 'EOF'
import snowflake.connector
import json
import os
from datetime import datetime

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
)

cursor = conn.cursor()
cursor.execute("USE DATABASE FINTECH_ANALYTICS")
cursor.execute("USE SCHEMA RAW")

test_row = (
    'BTCUSDT',
    '42500.50',
    datetime.now().isoformat(),
    'market_prices',
    0,
    0,
    json.dumps({'symbol': 'BTCUSDT', 'price': 42500.50})
)

cursor.execute("""
INSERT INTO MARKET_PRICES (SYMBOL, PRICE, EVENT_TIME, KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET, PAYLOAD)
VALUES (%s, TRY_TO_DECIMAL(%s, 18, 8), TRY_TO_TIMESTAMP_NTZ(%s), %s, %s, %s, PARSE_JSON(%s))
""", test_row)

conn.commit()
print("✅ Test insert successful!")
EOF
```

---

## Power BI Issues

### ❌ "Cannot authenticate to Power BI"

**Problem:** Power BI setup fails with auth error

**Solution:**
```bash
# Step 1: Verify Azure AD app registration
# Go to https://portal.azure.com
# Azure AD → App registrations
# Search for your app, verify it exists

# Step 2: Check credentials in .env.powerbi
cat .env.powerbi | grep POWERBI

# Should have:
# POWERBI_TENANT_ID=xxx
# POWERBI_CLIENT_ID=xxx
# POWERBI_CLIENT_SECRET=xxx

# Step 3: Verify they're correct (from Azure Portal)
# App registrations → Your app
# Client ID (copy exactly)
# Certificates & secrets → Client secret (copy exactly)
# Azure AD → Properties → Tenant ID (copy exactly)

# Step 4: Test authentication manually
python3 << 'EOF'
import os
from msal import PublicClientApplication

TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

try:
    from azure.identity import ClientSecretCredential
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )
    token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
    print("✅ Authentication successful!")
except Exception as e:
    print(f"❌ Error: {e}")
EOF

# Step 5: If still failing:
# Check app permissions in Azure AD
# Must have: Power BI Service → Dataset.ReadWrite.All
```

---

### ❌ "Dataset not found" in Power BI Service

**Problem:** Published dataset but can't find it

**Solution:**
```bash
# Step 1: Verify dataset was created
# Power BI Service → Datasets
# Should see: "Market Prices Dataset"

# Step 2: If not showing:
# Run setup script again
python fintech-airflow/dags/scripts/powerbi_setup.py

# Step 3: Check workspace ID is correct
cat .env.powerbi | grep WORKSPACE_ID

# Step 4: Create dataset manually if needed
# Power BI Service → Create → Dataset
# Connect to Snowflake
# Select MARKET_PRICES table

# Step 5: Or use Power BI Desktop
# File → Publish
# Select workspace
```

---

### ❌ "Snowflake connection fails in Power BI"

**Problem:** Can't connect to Snowflake from Power BI

**Solution:**
```bash
# Step 1: Verify Snowflake account in Power BI
# Power BI Desktop → Get Data → Snowflake
# Server: your-account.snowflakecomputing.com
# Database: FINTECH_ANALYTICS

# Step 2: Check credentials
# Snowflake username and password

# Step 3: Verify user has permissions
# In Snowflake:
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON DATABASE FINTECH_ANALYTICS TO USER your_user;
GRANT USAGE ON SCHEMA FINTECH_ANALYTICS.RAW TO USER your_user;
GRANT SELECT ON TABLE FINTECH_ANALYTICS.RAW.MARKET_PRICES TO USER your_user;

# Step 4: Test Snowflake connection first
# Before connecting in Power BI, verify you can login to Snowflake directly
# https://snowflake.com

# Step 5: Check firewall rules
# Power BI needs to reach Snowflake
# If behind company network, might need VPN

# Step 6: Use DirectQuery for real-time
# In Power BI: Get Data → Advanced Options
# Import Mode: DirectQuery
```

---

### ❌ Dashboard not refreshing

**Problem:** Data in Snowflake updated but dashboard shows old data

**Solution:**
```bash
# Step 1: Manually refresh dataset
# Power BI Service → Datasets
# Your dataset → Settings ⚙️
# Scheduled refresh → Refresh now

# Step 2: Check refresh schedule
# Settings → Refresh schedule
# Should be set to hourly or daily

# Step 3: Check refresh history
# Settings → Refresh history
# Look for errors

# Step 4: If Airflow DAG handles refresh:
# Check DAG ran successfully
airflow dags list-runs -d powerbi_dashboard_refresh
# Look for status

# Step 5: Common issues:
# - Snowflake user doesn't have permissions
# - Data gateway offline (if using on-premise)
# - Power BI Premium required for frequent refresh
# - Timeout during large data transfers

# Step 6: Force refresh from Power BI Desktop
# File → Publish
# Choose workspace
# Will refresh when published
```

---

## Network & Port Issues

### ❌ "Port X already in use"

**Problem:** Docker can't bind to port

**Solution:**
```bash
# Find what's using port
sudo lsof -i :9092
# Shows: COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME

# Kill the process
sudo kill <PID>

# Or change port in docker-compose.yml
# Find line like: "9092:9092"
# Change first number: "9093:9092"
# (9093 = external, 9092 = internal container)

# Then restart
docker-compose down
docker-compose up -d
```

---

### ❌ "host.docker.internal: Name or service not known" (Linux only)

**Problem:** Docker container can't resolve hostname (Linux-specific)

**Solution:**
```bash
# Option 1: Add to docker-compose.yml
# Under kafka service, add:
kafka:
  extra_hosts:
    - "host.docker.internal:host-gateway"

# Then restart:
docker-compose down
docker-compose up -d

# Option 2: Use your IP address instead
hostname -I
# Returns something like: 192.168.1.100

# Edit .env:
KAFKA_BOOTSTRAP_SERVERS=192.168.1.100:39092

# Option 3: Use container network hostname
# Inside container, use: kafka:29092
# Not: host.docker.internal:39092
```

---

### ❌ "Connection timeout" when accessing services

**Problem:** Can connect to port but connection times out

**Solution:**
```bash
# Step 1: Verify port is listening
sudo netstat -tlnp | grep 9092
# Should show: LISTEN

# Step 2: Check firewall
sudo ufw status
# If active, allow port:
sudo ufw allow 9092

# Step 3: Test connectivity
telnet localhost 9092
# Or: nc -zv localhost 9092

# Step 4: Check if service is actually running
docker logs kafka
# Should show: "Broker started" or similar

# Step 5: If service keeps crashing:
# Check logs for startup errors
docker-compose logs -f kafka

# Step 6: For remote access (not localhost)
# Services only accessible from localhost by default
# Edit docker-compose.yml:
# Change: "9092:9092" (only localhost)
# To: "0.0.0.0:9092:9092" (all interfaces)
# ⚠️ WARNING: Security risk! Only for private networks
```

---

## Quick Diagnostic Checklist

Copy and run this to diagnose issues quickly:

```bash
#!/bin/bash

echo "=== System Info ==="
docker --version
docker-compose --version
python3 --version

echo -e "\n=== Docker Containers ==="
docker ps

echo -e "\n=== Kafka Health ==="
docker exec kafka bash -c "kafka-broker-api-versions --bootstrap-server localhost:9092" 2>/dev/null && echo "✅ Kafka OK" || echo "❌ Kafka Failed"

echo -e "\n=== Kafka Topics ==="
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null

echo -e "\n=== Airflow DAGs ==="
airflow dags list 2>/dev/null && echo "✅ Airflow OK" || echo "❌ Airflow Failed"

echo -e "\n=== Required Packages ==="
python3 -c "import kafka; print('✅ kafka-python OK')" 2>/dev/null || echo "❌ kafka-python Missing"
python3 -c "import snowflake; print('✅ snowflake-connector OK')" 2>/dev/null || echo "❌ snowflake-connector Missing"
python3 -c "import airflow; print('✅ airflow OK')" 2>/dev/null || echo "❌ airflow Missing"

echo -e "\n=== Environment Variables ==="
echo "KAFKA_BOOTSTRAP_SERVERS: $KAFKA_BOOTSTRAP_SERVERS"
echo "SNOWFLAKE_ACCOUNT: $SNOWFLAKE_ACCOUNT"
echo "SNOWFLAKE_USER: $SNOWFLAKE_USER"

echo -e "\n=== Diagnostic Complete ==="
```

---

## When All Else Fails

1. **Check logs everywhere:**
   ```bash
   docker logs <container>
   airflow logs -d <dag>
   cat /tmp/*.log
   ```

2. **Restart everything:**
   ```bash
   docker-compose down -v
   docker-compose up -d
   astro dev restart
   ```

3. **Search error message online**
   - Often the exact error message in Google finds the answer
   - StackOverflow is your friend

4. **Check official documentation:**
   - Docker: https://docs.docker.com/
   - Kafka: https://kafka.apache.org/documentation/
   - Airflow: https://airflow.apache.org/docs/
   - Snowflake: https://docs.snowflake.com/
   - Power BI: https://docs.microsoft.com/power-bi/

5. **Ask for help:**
   - Include error message
   - Include steps you already tried
   - Include output of: `docker ps`, `airflow dags list`, etc.
