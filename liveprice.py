import psycopg2
import pandas as pd
import time
import streamlit as st

def get_tickers():
    conn = psycopg2.connect(
        host="localhost",
        database="noahbrannon",
        port="5432"
        )
    cursor = conn.cursor()
    cursor.execute("SELECT distinct(symbol) FROM marketdata")
    rows = cursor.fetchall()
    tickers = [row[0] for row in rows]
    return tickers

def get_price():
    conn = psycopg2.connect(
        host="localhost",
        database="noahbrannon",
        port="5432"
        )
    cursor = conn.cursor()
    cursor.execute("SELECT symbol, price, ts_event FROM marketdata ORDER BY ts_event DESC")
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=["symbol", "price", "ts_event"])
    return df


while True:
    current_tickers = get_tickers()
    df = get_price()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True).dt.tz_convert("America/Detroit")

    for ticker in current_tickers:
        ticker_df = df[df["symbol"] == ticker][["ts_event", "price"]].head(50).set_index("ts_event")
        
        st.subheader(f"{ticker} Price")
        st.line_chart(ticker_df)

    st.write("Last updated: ", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
    time.sleep(5)
    st.rerun()





