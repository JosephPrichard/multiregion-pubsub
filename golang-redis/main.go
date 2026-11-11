package main

import (
	"context"
	"log/slog"
	"multiregion-pubsub/app"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
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

	config := app.RouterConfig{
		DatabaseConnURI: databaseConnURI,
		RedisNodeURIs:   redisNodeURIs,
		AwsRegion:       awsRegion,
	}

	mux, shutdown := app.NewRouter(ctx, config)
	defer shutdown()

	if port == "" {
		port = "8080"
	}

	slog.Info("starting server", "port", port)

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		slog.Error("failed while serving", "error", err)
	}
}
