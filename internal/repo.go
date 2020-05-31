package internal

import (
	"context"
	"fmt"
	sdk "github.com/TinkoffCreditSystems/invest-openapi-go-sdk"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"strings"
)

const (
	insertInstrument = `INSERT INTO instruments (figi, ticker, isin, name, min_price_increment, lot, currency, type)
						VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
						ON CONFLICT (figi) DO UPDATE SET min_price_increment = $5
						RETURNING id`
	insertCandles = `INSERT INTO candles (instrument_id, open, close, high, low, volume, time)
					 VALUES `
	selectInstrument = `SELECT id, figi, ticker, isin, name, min_price_increment, lot, currency, type FROM instruments WHERE ticker = $1`
)

func InsertCandles(equity *Equity, candles *[]sdk.Candle, date EquityDates, conn *pgxpool.Pool, cancel context.CancelFunc) {
	str := insertCandles
	var vals []interface{}

	last := 0

	for _, candle := range *candles {
		str += fmt.Sprintf("(%v, %v, %v, %v, %v, %v, %v),",
			FormatArg(last+1),
			FormatArg(last+2),
			FormatArg(last+3),
			FormatArg(last+4),
			FormatArg(last+5),
			FormatArg(last+6),
			FormatArg(last+7))
		vals = append(vals,
			equity.Id,
			candle.OpenPrice,
			candle.ClosePrice,
			candle.HighPrice,
			candle.LowPrice,
			candle.Volume,
			candle.TS)
		last += 7
	}

	str = strings.TrimRight(str, ",")

	if _, err := conn.Exec(context.Background(), str, vals...); err != nil {
		cancel()
		log.Fatalf("Error inserting candles for %v %v\n", equity.Ticker, err)
	} else {
		log.Printf("Got candles from %v to %v for %v\n", date.From, date.To, equity.Ticker)
	}
}

func ScanInstrument(equity *Equity, conn *pgxpool.Pool) error {
	return conn.QueryRow(
		context.Background(),
		selectInstrument,
		equity.Ticker).
		Scan(
			&equity.Id,
			&equity.Instrument.FIGI,
			&equity.Instrument.Ticker,
			&equity.Instrument.ISIN,
			&equity.Instrument.Name,
			&equity.Instrument.MinPriceIncrement,
			&equity.Instrument.Lot,
			&equity.Instrument.Currency,
			&equity.Instrument.Type)
}

func InsertInstrument(equity *Equity, conn *pgxpool.Pool) error {
	return conn.QueryRow(
		context.Background(),
		insertInstrument,
		equity.Instrument.FIGI,
		equity.Instrument.Ticker,
		equity.Instrument.ISIN,
		equity.Instrument.Name,
		equity.Instrument.MinPriceIncrement,
		equity.Instrument.Lot,
		equity.Instrument.Currency,
		equity.Instrument.Type).
		Scan(&equity.Id)
}
