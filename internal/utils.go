package internal

import (
	"context"
	"fmt"
	"math"
	"os"
	"time"
)

/**
Tinkoff only supports one year candle data. We split dates in chunks to fetch them later.
 */
func GetDates(from time.Time, to time.Time) []EquityDates {
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

	return equityDates
}

func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func FormatArg(n int) string {
	return fmt.Sprintf("$%v", n)
}

func GetContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Second*5)
}

