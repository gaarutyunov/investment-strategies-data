package main

import (
	"context"
	sdk "github.com/TinkoffCreditSystems/invest-openapi-go-sdk"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/spf13/viper"
	"investment-strategies-data/internal"
	"log"
	"time"
)

func main() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()

	if err != nil {
		log.Fatalf("Unable to parse config: %v\n", err)
	}

	config, err := pgxpool.ParseConfig(viper.GetString("db.conn"))

	if err != nil {
		log.Fatalf("Unable to parse config: %v\n", err)
	}

	//config.ConnConfig.Logger = internal.NewLogger(logrus.New())

	conn, err := pgxpool.ConnectConfig(context.Background(), config)

	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}

	defer conn.Close()

	client := sdk.NewSandboxRestClient(viper.GetString("api.token"))

	internal.FetchAll(conn, client, time.Date(2008, 1, 3, 0, 0 ,0, 0, time.UTC), time.Date(2010, 5, 30, 0, 0, 0, 0, time.UTC))
}

