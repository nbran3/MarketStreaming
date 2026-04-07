CREATE TABLE marketdata(
    trade_id uuid PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    price DECIMAL(18, 4) NOT NULL,
    quantity INT NOT NULL,
    side VARCHAR(7) NOT NULL,
    ts_event TIMESTAMPTZ NOT NULL,
    ts_producer TIMESTAMPTZ NOT NULL,
    ts_consumer_received TIMESTAMPTZ not null,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);