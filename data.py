import time
import uuid
import random
from faker import Faker
from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2 import extras

fake = Faker()

symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA']

stock_dict = {"AAPL": 258.65, "GOOGL": 299.99, "MSFT": 372.88, "AMZN": 212.79, "TSLA": 352.82, "META": 573.02, "NVDA": 177.64}


def generate_trade_ticker():
    symbol = random.choice(symbols)

    stock_dict[symbol] *= (1 + random.uniform(-0.01, 0.01))

    return {
        "trade_id": str(uuid.uuid4()),
        "symbol": symbol,
        "price": round(stock_dict[symbol], 2),
        "quantity": random.randint(1, 1000),
        "side": random.choice(["buy", "sell"]),
        "ts_event": time.time_ns(),
        "ts_producer": time.time_ns()
    }


generate_trade_ticker()