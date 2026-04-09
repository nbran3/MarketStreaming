import psycopg2
import pandas as pd
import time
import streamlit as st

def get_quantities():
    conn = psycopg2.connect(
        host="localhost",
        database="noahbrannon",
        port="5432"
        )
    cursor = conn.cursor()
    cursor.execute("SELECT symbol, side, SUM(quantity) as total_quantity FROM marketdata GROUP BY symbol, side")
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=["symbol", "side", "total_quantity"])
    return df
    
st.title("Live Trade Quantities Dashboard")

while True:
    df = get_quantities()

    buys = df[df["side"] == "buy"].set_index("symbol")["total_quantity"]
    sells = df[df["side"] == "sell"].set_index("symbol")["total_quantity"]

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Buy Quantities")
        st.bar_chart(buys)
    with col2:
        st.subheader("Sell Quantities")
        st.bar_chart(sells)

    st.write("Last updated: ", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
    time.sleep(5)
    st.rerun()