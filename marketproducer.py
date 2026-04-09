import time
import uuid
import random
import json
from faker import Faker
from kafka import KafkaProducer
import yfinance as yf
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


fake = Faker()

symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA']

stock_dict = {"AAPL": yf.Ticker("AAPL").info['regularMarketPrice'], "GOOGL": yf.Ticker("GOOGL").info['regularMarketPrice'], "MSFT": yf.Ticker("MSFT").info['regularMarketPrice'], "AMZN": yf.Ticker("AMZN").info['regularMarketPrice'], "TSLA": yf.Ticker("TSLA").info['regularMarketPrice'], "META": yf.Ticker("META").info['regularMarketPrice'], "NVDA": yf.Ticker("NVDA").info['regularMarketPrice']}

records = 10
counter = 0
TOPIC = "marketdata"


def generate_trade_ticker(records=10):
    for _ in range(records):
        symbol = random.choice(symbols)

        BASE_PRICES = stock_dict.copy()  

        change = random.gauss(0, 0.002)  
        change = max(-0.005, min(0.005, change))  

        new_price = stock_dict[symbol] * (1 + change)

        base = BASE_PRICES[symbol]
        new_price = max(base * 0.90, min(base * 1.10, new_price))

        stock_dict[symbol] = round(new_price, 2)

        yield {
            "trade_id": str(uuid.uuid4()),
            "symbol": symbol,
            "price": round(stock_dict[symbol], 2),
            "quantity": random.randint(1, 1000),
            "side": random.choice(["buy", "sell"]),
            "ts_event": time.time(),
            "ts_producer": time.time()
        }


def build_producer():
    return KafkaProducer(
        bootstrap_servers=["broker:9092"],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )


try:
    producer = build_producer()
    print("Starting Kafka Producer... Press Ctrl+C to stop.")
    while True:
        counter += 1

        for data in generate_trade_ticker(records):
            future = producer.send(TOPIC, value=data)
            metadata = future.get(timeout=10)
            print(
                f"Sent trade {data['trade_id']} to "
                f"{metadata.topic} partition {metadata.partition} offset {metadata.offset}"
            )

        print(f"Produced {counter * records} messages.")
        producer.flush()
        time.sleep(2)

except KeyboardInterrupt:
    print("Stopping Kafka Producer...")
except Exception as exc:
    print(f"Kafka producer error: {exc}")

finally:
    print("Flushing remaining messages...")
    if "producer" in locals():
        producer.flush()
        producer.close()
    print("All messages flushed. Exiting.")

