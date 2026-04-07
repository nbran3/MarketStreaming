import json
import psycopg2
import time
from kafka import KafkaConsumer

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="noahbrannon",
        port="5432"
    )

consumer = KafkaConsumer(
    "marketdata",
    bootstrap_servers=["broker:9092"],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="marketdata-consumers"
)

def insert_trade_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    print("Consumer state: Inserting trade data into database...")

    try:
        for message in consumer:
            time_received = time.time()
            trade = message.value
            print(f"Received trade: {trade}")

            insert_query = """
                INSERT INTO marketdata (trade_id, symbol, price, quantity, side, ts_event, ts_producer, ts_consumer_received)
                VALUES (%s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s), to_timestamp(%s))
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
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    insert_trade_data()