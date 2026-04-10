import psycopg2
import pandas as pd
import streamlit as st
import time

def get_latency_data():
    conn = psycopg2.connect(
        host="localhost",
        database="noahbrannon",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT trade_id, ts_event, inserted_at, ts_consumer_received, ts_producer FROM marketdata")
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=["trade_id", "ts_event", "inserted_at", "ts_consumer_received", "ts_producer"])
    df['e2e_latency'] = (df['inserted_at'] - df['ts_event']).dt.total_seconds()
    df['consumer_latency'] = (df['ts_consumer_received'] - df['ts_producer']).dt.total_seconds()
    df['producer_latency'] = (df['ts_producer'] - df['ts_event']).dt.total_seconds()
    return df



while True:
    st.cache_data.clear()

    df = get_latency_data()
    st.title("Trade Data Latency Dashboard")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(label="Average End-to-End Latency (seconds)", value=f"{df['e2e_latency'].mean():.4f}")
    with col2:
        st.metric(label="Average Consumer Latency (seconds)", value=f"{df['consumer_latency'].mean():.4f}")
    with col3:
        st.metric(label="Average Producer Latency (seconds)", value=f"{df['producer_latency'].mean():.4f}")

    col4, col5 = st.columns(2)
    with col4:
        st.metric(label='Throughput (trades per second)', value=f"{len(df) / (df['inserted_at'].max() - df['inserted_at'].min()).total_seconds():.2f}")
    with col5:
        st.metric(label="Jitter (seconds)", value=f"{df['e2e_latency'].std():.4f}")

    col6, col7, col8, col9 = st.columns(4)
    with col6:
        st.metric(label='50th Percentile End-to-End Latency (seconds)', value=f"{df['e2e_latency'].quantile(0.5):.4f}")
    with col7:
        st.metric(label='90th Percentile End-to-End Latency (seconds)', value=f"{df['e2e_latency'].quantile(0.9):.4f}")
    with col8:
        st.metric(label='95th Percentile End-to-End Latency (seconds)', value=f"{df['e2e_latency'].quantile(0.95):.4f}")
    with col9:
        st.metric(label='99th Percentile End-to-End Latency (seconds)', value=f"{df['e2e_latency'].quantile(0.99):.4f}")

    col10, = st.columns(1)
    with col10:
        rollingp99 = df['e2e_latency'].rolling(window=100).quantile(0.99)

        st.line_chart(rollingp99, height=300, width=600)    

    st.write("Last updated: ", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
    time.sleep(5)
    st.rerun()
