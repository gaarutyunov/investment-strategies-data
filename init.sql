CREATE DATABASE investment_strategies;

CREATE TABLE instruments
(
    id                  BIGSERIAL UNIQUE NOT NULL PRIMARY KEY,
    figi                TEXT UNIQUE      NOT NULL,
    ticker              TEXT UNIQUE      NOT NULL,
    isin                TEXT UNIQUE      NOT NULL,
    name                TEXT,
    min_price_increment FLOAT,
    lot                 INT,
    currency            TEXT,
    type                TEXT
);

CREATE UNIQUE INDEX figi_idx ON instruments (figi);
CREATE UNIQUE INDEX ticker_idx ON instruments (ticker);
CREATE UNIQUE INDEX isin_idx ON instruments (isin);

CREATE TABLE candles
(
    id            BIGSERIAL UNIQUE NOT NULL PRIMARY KEY,
    instrument_id BIGINT REFERENCES instruments (id),
    open          FLOAT,
    close         FLOAT,
    high          FLOAT,
    low           FLOAT,
    volume        FLOAT,
    time          TIMESTAMPTZ
);

CREATE VIEW equity_history AS
SELECT i.ticker     AS ticker,
       DATE(c.time) AS trade_date,
       c.open       AS open,
       c.high       AS high,
       c.low        AS low,
       c.close      AS close,
       c.volume     AS volume,
       0.0          AS dividend
FROM instruments AS i
         INNER JOIN candles c ON i.id = c.instrument_id;