import altair as alt
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
    cursor.execute("SELECT symbol, float8(price), ts_event FROM marketdata ORDER BY ts_event DESC")
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=["symbol", "price", "ts_event"])
    return df


while True:
    current_tickers = get_tickers()
    df = get_price()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True).dt.tz_convert("America/Detroit")
    df["price"] = df["price"]


    cols = st.columns(2) 

    for i, ticker in enumerate(current_tickers):
        ticker_df = df[df["symbol"] == ticker][["ts_event", "price"]].head(50).set_index("ts_event")

        with cols[i % 2]:
            st.subheader(f"{ticker}")
            if ticker_df.empty:
                st.write("No data yet.")
                continue

            chart_df = ticker_df.reset_index()
            min_price = chart_df["price"].min()
            max_price = chart_df["price"].max()
            padding = max((max_price - min_price) * 0.1, 0.5)

            chart = (
                alt.Chart(chart_df)
                .mark_line()
                .encode(
                    x=alt.X("ts_event:T", title="Time"),
                    y=alt.Y(
                        "price:Q",
                        title="Price",
                        scale=alt.Scale(domain=[min_price - padding, max_price + padding]),
                    ),
                )
                .properties(height=200)
            )
            st.altair_chart(chart, use_container_width=True)

    st.write("Last updated: ", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
    time.sleep(5)
    st.rerun()



