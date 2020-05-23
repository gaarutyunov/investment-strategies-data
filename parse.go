package main

import (
	"fmt"
	"github.com/360EntSecGroup-Skylar/excelize"
	sdk "github.com/TinkoffCreditSystems/invest-openapi-go-sdk"
	. "github.com/ahmetb/go-linq/v3"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"io/ioutil"
	"log"
	"math"
	"os"
	"regexp"
	"strings"
	"time"
)

type Index struct {
	From   time.Time
	To     time.Time
	Stocks []string
}

type Equity struct {
	Id     *int64
	Ticker string
	Dates []EquityDates
	Instrument *sdk.Instrument
}

type EquityDates struct {
	From time.Time
	To   time.Time
}

const (
	excelLayout   = "01-02-06"
	ZiplineLayout = "2006-01-02"
	outFileName   = "index.csv"
	inFileName    = "index.xlsx"
)

func Parse() (*dataframe.DataFrame, error) {
	if fileExists(outFileName) {
		content, _ := ioutil.ReadFile(outFileName)
		ioContent := strings.NewReader(string(content))

		df := dataframe.ReadCSV(ioContent, dataframe.HasHeader(true))

		return &df, nil
	} else {
		return doParse(inFileName)
	}
}

func doParse(filename string) (*dataframe.DataFrame, error) {
	start := time.Now()

	xlsx, err := excelize.OpenFile(filename)

	if err != nil {
		return nil, err
	}

	var rows [][]string

	rows = append(rows, []string{"date", "tickers"})

	for _, name := range xlsx.GetSheetMap() {
		from := xlsx.GetCellValue(name, "C2")
		to := xlsx.GetCellValue(name, "D2")
		var stocks []string
		parsing := true
		var ticker string
		i := 1
		reg, err := regexp.Compile("[^A-Z]")

		if err != nil {
			return nil, err
		}

		for parsing {
			if i > 4 && len(strings.TrimSpace(ticker)) == 0 {
				parsing = false
				break
			}
			ticker = xlsx.GetCellValue(name, fmt.Sprint("B", i))
			ticker = reg.ReplaceAllString(ticker, "")

			if len(strings.TrimSpace(ticker)) > 0 && ticker != "C" {
				stocks = append(stocks, ticker)
			}

			i++
		}

		index := Index{
			From:   parseDate(from),
			To:     parseDate(to),
			Stocks: stocks,
		}

		days := index.To.Sub(index.From).Hours() / 24

		for i := 0; i <= int(days); i++ {
			rows = append(rows, []string{
				index.From.AddDate(0, 0, i).Format(ZiplineLayout),
				strings.Join(index.Stocks, ";"),
			})
		}
	}

	df := dataframe.LoadRecords(rows)
	df = df.Arrange(dataframe.Sort("date"))

	if f, err := os.Create(outFileName); err != nil {
		return nil, err
	} else if err := df.WriteCSV(f, dataframe.WriteHeader(true)); err != nil {
		return nil, err
	} else {
		elapsed := time.Since(start)
		log.Printf("Parsed data in %s", elapsed)

		return &df, nil
	}
}

func parseDate(date string) time.Time {
	if t, err := time.Parse(excelLayout, date); err != nil {
		return time.Now()
	} else {
		return t
	}
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func SelectEquities(df *dataframe.DataFrame) (*[]Equity, error) {
	tickers := df.Col("tickers")

	var distinct []string

	From(strings.Split(strings.Join(tickers.Records(), ";"), ";")).
		Distinct().
		ToSlice(&distinct)

	var equities []Equity

	for _, s := range distinct {
		var containTicker []bool

		From(tickers.Records()).Select(func(i interface{}) interface{} {
			return strings.Contains(i.(string), s)
		}).ToSlice(&containTicker)

		contains := df.
			Mutate(series.New(containTicker, series.Bool, "contains")).
			Filter(dataframe.F{
				Colname:    "contains",
				Comparator: series.Eq,
				Comparando: true,
			}).
			Arrange(dataframe.Sort("date"))

		dates := contains.Col("date").Records()

		from, err := time.Parse(ZiplineLayout, dates[0])
		if err != nil {
			return nil, err
		}

		to, err := time.Parse(ZiplineLayout, dates[len(dates)-1])
		if err != nil {
			return nil, err
		}

		yearsDiff := math.Floor(to.Sub(from).Hours() / 24 / 365)
		lastDate := from
		var equityDates []EquityDates

		if yearsDiff > 0 {
			for i := 0; i < int(yearsDiff); i++ {
				newDate := lastDate.AddDate(0, 0, 365)

				equityDates = append(equityDates, EquityDates{
					From: lastDate,
					To:   newDate,
				})

				lastDate = newDate.AddDate(0, 0, 1)
			}

			if lastDate.Before(to) {
				equityDates = append(equityDates, EquityDates{
					From: lastDate,
					To:   to,
				})
			}
		} else {
			equityDates = append(equityDates, EquityDates{
				From: lastDate,
				To:   to,
			})
		}

		equities = append(equities, Equity{
			Dates:  equityDates,
			Ticker: s,
		})
	}

	return &equities, nil
}
