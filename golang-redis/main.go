package main

import (
	"context"
	"log/slog"
	"multiregion-pubsub/app"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	"github.com/pressly/goose"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

func main() {
	// deadline to ensure the server is starting quickly, any longer than 3 seconds there is likely a deadlock
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := godotenv.Load(); err != nil {
		slog.Info("no .env file found, using OS environment only")
	}

	databaseConnURI := os.Getenv("DATABASE_CONN_URI")
	redisNodeURIs := strings.Split(os.Getenv("REDIS_NODE_URIS"), ",")
	awsRegion := os.Getenv("AWS_REGION")
	port := os.Getenv("LISTEN_PORT")

	config := app.InfraConfig{
		DatabaseConnURI: databaseConnURI,
		RedisNodeURIs:   redisNodeURIs,
		AwsRegion:       awsRegion,
	}
	clients := app.NewDatabaseClients(ctx, config)

	doMigrations(clients)

	mux, shutdown := app.NewRouter(clients)
	defer shutdown()

	if port == "" {
		port = "8080"
	}

	slog.Info("starting server", "port", port)

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		slog.Error("failed while serving", "error", err)
	}
}

func doMigrations(clients app.DatabaseClients) {
	riverMigrator, err := rivermigrate.New(riverpgxv5.New(clients.DatabasePool), nil)
	if err != nil {
		slog.Error("failed to create river migrator")
		return
	}
	if _, err := riverMigrator.Migrate(context.Background(), rivermigrate.DirectionUp, nil); err != nil {
		slog.Error("failed to execute river migration", "error", err)
		return
	}

	slog.Info("river migrations are complete")

	db := stdlib.OpenDBFromPool(clients.DatabasePool)
	defer db.Close()

	_ = goose.SetDialect("postgres")
	if err := goose.Up(db, "."); err != nil {
		slog.Error("failed to execute goose migration", "error", err)
		return
	}

	slog.Info("goose migrations are complete")
}
