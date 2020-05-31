package internal

import (
	"github.com/TinkoffCreditSystems/invest-openapi-go-sdk"
	. "github.com/ahmetb/go-linq/v3"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/ratelimit"
	"log"
	"time"
)

/**
Fetch and save equity
 */
func FetchEquity(equity *Equity, conn *pgxpool.Pool, client *sdk.SandboxRestClient, found chan Equity, notFound chan Equity) {
	ctx, cancel := GetContext()

	equity.Instrument = &sdk.Instrument{}

	if err := ScanInstrument(equity, conn); err != nil {
		if instruments, err := client.InstrumentByTicker(ctx, equity.Ticker); err != nil {
			cancel()
			log.Fatalf("Error getting %v %v\n", equity.Ticker, err)
		} else if len(instruments) > 0 {
			equity.Instrument = &instruments[0]

			if err := InsertInstrument(equity, conn); err != nil {
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

/**
Fetch candles in OHLCV format
 */
func FetchCandles(equity *Equity, date EquityDates, conn *pgxpool.Pool, client *sdk.SandboxRestClient) {
	ctx, cancel := GetContext()

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
			InsertCandles(equity, &candles, date, conn, cancel)
		} else {
			log.Printf("No candles from %v to %v for %v\n", date.From, date.To, equity.Ticker)
		}
	}
}

/**
Fetch equity instrument by ticker and candles
 */
func FetchEquities(equities *[]Equity, conn *pgxpool.Pool, client *sdk.SandboxRestClient) {
	rl := ratelimit.New(2) // per second

	for _, equity := range *equities {
		rl.Take()

		equity := equity

		found := make(chan Equity)
		notFound := make(chan Equity)

		go FetchEquity(&equity, conn, client, found, notFound)

		select {
		case e := <-found:
			log.Printf("Got instrument %v\n", e.Ticker)

			for _, date := range e.Dates {
				rl.Take()

				date := date

				go FetchCandles(&equity, date, conn, client)
			}
		case e := <-notFound:
			log.Printf("Could not find %v\n", e.Ticker)
		}
	}
}

/**
Fetch index members from index.xlsx file
 */
func FetchIndexMembers(conn *pgxpool.Pool, client *sdk.SandboxRestClient) {
	df, err := Parse()

	if err != nil {
		log.Fatalf("Unable to parse data: %v\n", err)
	}

	equities, err := GetIndexMembers(df)

	if err != nil {
		log.Fatalf("Error while selecting data: %v\n", err)
	}

	FetchEquities(equities, conn, client)
}

/**
Fetch all available stocks
 */
func FetchAll(conn *pgxpool.Pool, client *sdk.SandboxRestClient, from time.Time, to time.Time) {
	ctx, cancel := GetContext()
	rl := ratelimit.New(2) // per second

	if instruments, err := client.Stocks(ctx); err != nil {
		cancel()
		log.Fatalln("Error fetching stocks")
	} else {
		dates := GetDates(from, to)
		var ruInstruments []sdk.Instrument

		From(instruments).Where(func(i interface{}) bool {
			return i.(sdk.Instrument).Currency == sdk.RUB
		}).ToSlice(&ruInstruments)

		for _, instrument := range ruInstruments {
			equity := &Equity{
				Instrument: &instrument,
				Dates: dates,
				Ticker: instrument.Ticker,
			}

			if err := InsertInstrument(equity, conn); err != nil {
				cancel()
				log.Fatalf("Error inserting %v %v\n", equity.Ticker, err)
			} else {
				for _, date := range equity.Dates {
					rl.Take()

					date := date

					go FetchCandles(equity, date, conn, client)
				}
			}
		}
	}
}


