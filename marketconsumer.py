import json
import psycopg2
import time
from kafka import KafkaConsumer
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_USER = os.getenv("POSTGRES_USER", "noahbrannon")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

def get_consumer(retries=10, delay=5):
    for attempt in range(retries):
        try:
            return KafkaConsumer(
                "marketdata",
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="marketdata-consumers"
            )
        except Exception as e:
            print(f"Kafka not ready, attempt {attempt + 1}/{retries}. Retrying in {delay}s... ({e})")
            time.sleep(delay)
    raise Exception("Could not connect to Kafka after multiple retries")

def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database="noahbrannon",
        port="5432",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def insert_trade_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    consumer = get_consumer()

    print("Consumer state: Inserting trade data into database...")

    try:
        for message in consumer:
            time_received = time.time()
            trade = message.value
            print(f"Received trade: {trade}")

            insert_query = """
    INSERT INTO marketdata (trade_id, symbol, price, quantity, side, ts_event, ts_producer, ts_consumer_received)
    VALUES (%s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s), to_timestamp(%s))
    ON CONFLICT (trade_id) DO NOTHING
"""
            cursor.execute(insert_query, (
                trade["trade_id"],
                trade["symbol"],
                trade["price"],
                trade["quantity"],
                trade["side"],
                trade["ts_event"],
                trade["ts_producer"],
                time_received
            ))
            conn.commit()
            print(f"Inserted trade {trade['trade_id']} into database.")
    except Exception as e:
        print(f"Error: {e}")
    except psycopg2.Error.UniqueViolation:
        print(f"Duplicate trade_id detected. Skipping insertion for {trade['trade_id']}.")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    insert_trade_data()