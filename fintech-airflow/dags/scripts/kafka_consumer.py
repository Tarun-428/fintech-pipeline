#!/usr/bin/env python3
"""
Kafka-to-Snowflake consumer for Airflow.
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import snowflake.connector

# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:39092")
TRADES_TOPIC = os.getenv("KAFKA_TOPIC", "market_prices")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "market_prices_snowflake_sink")
CONNECT_RETRIES = int(os.getenv("KAFKA_CONNECT_RETRIES", "6"))
RETRY_DELAY_SECONDS = int(os.getenv("KAFKA_RETRY_DELAY_SECONDS", "10"))
MAX_MESSAGES = int(os.getenv("KAFKA_MAX_MESSAGES", "100"))
CONSUMER_TIMEOUT_MS = int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "10000"))

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FINTECH_ANALYTICS")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_MARKET_PRICES_TABLE", "MARKET_PRICES")

REQUIRED_SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
    "SNOWFLAKE_USER": SNOWFLAKE_USER,
    "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
}


def quote_identifier(value):
    """Quote a Snowflake identifier while preserving case and special characters."""
    return '"' + value.replace('"', '""') + '"'


def ensure_snowflake_config():
    missing = [name for name, value in REQUIRED_SNOWFLAKE_ENV.items() if not value]
    if missing:
        raise ValueError(f"Missing required Snowflake env vars: {', '.join(missing)}")


def connect_to_snowflake():
    ensure_snowflake_config()

    connection_args = {
        "account": SNOWFLAKE_ACCOUNT,
        "user": SNOWFLAKE_USER,
        "password": SNOWFLAKE_PASSWORD,
        "warehouse": SNOWFLAKE_WAREHOUSE,
    }
    if SNOWFLAKE_ROLE:
        connection_args["role"] = SNOWFLAKE_ROLE

    return snowflake.connector.connect(**connection_args)


def setup_snowflake(cursor):
    database = quote_identifier(SNOWFLAKE_DATABASE)
    schema = quote_identifier(SNOWFLAKE_SCHEMA)
    table = quote_identifier(SNOWFLAKE_TABLE)

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
    cursor.execute(f"USE DATABASE {database}")
    cursor.execute(f"USE SCHEMA {schema}")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
            SYMBOL VARCHAR,
            PRICE NUMBER(18, 8),
            EVENT_TIME TIMESTAMP_NTZ,
            INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            KAFKA_TOPIC VARCHAR,
            KAFKA_PARTITION NUMBER,
            KAFKA_OFFSET NUMBER,
            PAYLOAD VARIANT
        )
        """
    )


def normalize_market_price(message):
    value = message.value
    event_time = value.get("timestamp") or value.get("event_time")
    price = value.get("price")

    return (
        value.get("symbol"),
        None if price is None else str(price),
        event_time,
        message.topic,
        message.partition,
        message.offset,
        json.dumps(value),
    )


def insert_market_prices(cursor, rows):
    table = quote_identifier(SNOWFLAKE_TABLE)
    sql = f"""
    INSERT INTO {table} (
        SYMBOL,
        PRICE,
        EVENT_TIME,
        KAFKA_TOPIC,
        KAFKA_PARTITION,
        KAFKA_OFFSET,
        PAYLOAD
    )
    SELECT
        %s,
        TRY_TO_DECIMAL(%s, 18, 8),
        TRY_TO_TIMESTAMP_NTZ(%s),
        %s,
        %s,
        %s,
        PARSE_JSON(%s)
    """

    for row in rows:
        cursor.execute(sql, row)

def consume_trades():
    """Consume market price messages from Kafka and persist them in Snowflake."""
    print(f"[{datetime.now()}] Starting Kafka consumer...")
    print(f"[{datetime.now()}] Connecting to Kafka at {KAFKA_BOOTSTRAP}")
    print(f"[{datetime.now()}] Subscribing to topic: {TRADES_TOPIC}")
    print(f"[{datetime.now()}] Writing to Snowflake table: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}")
    
    consumer = None
    snowflake_connection = None

    try:
        snowflake_connection = connect_to_snowflake()
        cursor = snowflake_connection.cursor()
        try:
            setup_snowflake(cursor)

            for attempt in range(1, CONNECT_RETRIES + 1):
                try:
                    consumer = KafkaConsumer(
                        TRADES_TOPIC,
                        bootstrap_servers=KAFKA_BOOTSTRAP,
                        group_id=CONSUMER_GROUP,
                        auto_offset_reset='latest',
                        enable_auto_commit=False,
                        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                        consumer_timeout_ms=CONSUMER_TIMEOUT_MS
                    )
                    break
                except NoBrokersAvailable:
                    if attempt == CONNECT_RETRIES:
                        raise

                    print(
                        f"[{datetime.now()}] Kafka broker not available "
                        f"(attempt {attempt}/{CONNECT_RETRIES}). "
                        f"Retrying in {RETRY_DELAY_SECONDS}s..."
                    )
                    time.sleep(RETRY_DELAY_SECONDS)

            print(f"[{datetime.now()}] Listening for messages...")
            rows = []

            for message in consumer:
                rows.append(normalize_market_price(message))
                print(f"[{datetime.now(timezone.utc)}] Queued message offset {message.offset}: {message.value}")

                if len(rows) >= MAX_MESSAGES:
                    print(f"[{datetime.now()}] Reached max batch size of {MAX_MESSAGES}. Exiting.")
                    break

            if not rows:
                print(f"[{datetime.now()}] No messages received in {CONSUMER_TIMEOUT_MS / 1000:.0f} seconds.")
                print(f"[{datetime.now()}] Topic '{TRADES_TOPIC}' may be empty or no new data.")
            else:
                insert_market_prices(cursor, rows)
                snowflake_connection.commit()
                consumer.commit()
                print(f"[{datetime.now()}] Inserted {len(rows)} rows into Snowflake.")
        finally:
            cursor.close()
            
    except Exception as e:
        print(f"[{datetime.now()}] Error: {e}")
        if snowflake_connection is not None:
            snowflake_connection.rollback()
        sys.exit(1)
    finally:
        if consumer is not None:
            consumer.close()
        if snowflake_connection is not None:
            snowflake_connection.close()
        print(f"[{datetime.now()}] Consumer closed.")

if __name__ == "__main__":
    consume_trades()
