package main

import (
	"context"
	"fmt"
	"github.com/TinkoffCreditSystems/invest-openapi-go-sdk"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/spf13/viper"
	"go.uber.org/ratelimit"
	"log"
	"strings"
	"time"
)

const (
	insertInstrument = `INSERT INTO instruments (figi, ticker, isin, name, min_price_increment, lot, currency, type)
						VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING RETURNING id`
	insertCandles = `INSERT INTO candles (instrument_id, open, close, high, low, volume, time)
					 VALUES `
	selectInstrument = `SELECT id, figi, ticker, isin, name, min_price_increment, lot, currency, type FROM instruments WHERE ticker = $1`
)

func main() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()

	if err != nil {
		log.Fatalf("Unable to parse config: %v\n", err)
	}

	conn, err := pgxpool.Connect(context.Background(), viper.GetString("db.conn"))

	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}

	defer conn.Close()

	df, err := Parse()

	if err != nil {
		log.Fatalf("Unable to parse data: %v\n", err)
	}

	equities, err := SelectEquities(df)

	if err != nil {
		log.Fatalf("Error while selecting data: %v\n", err)
	}

	client := sdk.NewSandboxRestClient(viper.GetString("api.token"))

	rl := ratelimit.New(2) // per second

	for _, equity := range *equities {
		rl.Take()

		equity := equity

		found := make(chan Equity)
		notFound := make(chan Equity)

		go fetchEquity(&equity, conn, client, found, notFound)

		select {
		case e := <-found:
			log.Printf("Got instrument %v\n", e.Ticker)

			for _, date := range e.Dates {
				rl.Take()

				date := date

				go fetchCandles(&equity, date, conn, client)
			}
		case e := <-notFound:
			log.Printf("Could not find %v\n", e.Ticker)
		}
	}
}

func fetchCandles(equity *Equity, date EquityDates, conn *pgxpool.Pool, client *sdk.SandboxRestClient) {
	ctx, cancel := getContext()

	if equity.Instrument != nil {
		if candles, err := client.Candles(
			ctx,
			date.From,
			date.To,
			sdk.CandleInterval1Day,
			equity.Instrument.FIGI); err != nil {
			cancel()
			log.Fatalf("Error getting candles for %v %v\n", equity.Ticker, err)
		} else if len(candles) > 0 {
			str := insertCandles
			var vals []interface{}

			last := 0

			for _, candle := range candles {
				str += fmt.Sprintf("(%v, %v, %v, %v, %v, %v, %v),",
					formatArg(last + 1),
					formatArg(last + 2),
					formatArg(last + 3),
					formatArg(last + 4),
					formatArg(last + 5),
					formatArg(last + 6),
					formatArg(last + 7))
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
		} else {
			log.Printf("No candles from %v to %v for %v\n", date.From, date.To, equity.Ticker)
		}
	}
}

func formatArg(n int) string {
	return fmt.Sprintf("$%v", n)
}

func fetchEquity(equity *Equity, conn *pgxpool.Pool, client *sdk.SandboxRestClient, found chan Equity, notFound chan Equity) {
	ctx, cancel := getContext()

	equity.Instrument = &sdk.Instrument{}

	if err := conn.QueryRow(
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
			&equity.Instrument.Type); err != nil {
		if instruments, err := client.InstrumentByTicker(ctx, equity.Ticker); err != nil {
			cancel()
			log.Fatalf("Error getting %v %v\n", equity.Ticker, err)
		} else if len(instruments) > 0 {
			equity.Instrument = &instruments[0]

			if err := conn.QueryRow(
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
				Scan(&equity.Id); err != nil {
				cancel()
				log.Fatalf("Error inserting %v %v\n", equity.Ticker, err)
			} else {
				found <- *equity
			}
		} else {
			notFound <- *equity
		}
	} else {
		log.Printf("Got instrument %v\n", equity.Ticker)
		found <- *equity
	}
}

func getContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Second*5)
}
