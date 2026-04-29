import requests
import time
import json
from kafka import KafkaProducer

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

SYMBOLS = ["BTCUSDT", "ETHUSDT"]

def get_price(symbol):
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    response = requests.get(url)
    data = response.json()
    return float(data["price"])

while True:
    for symbol in SYMBOLS:
        try:
            price = get_price(symbol)

            event = {
                "symbol": symbol,
                "price": price,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }

            print("Sending:", event)

            producer.send("market_prices", value=event)

        except Exception as e:
            print("Error:", e)

    time.sleep(5)