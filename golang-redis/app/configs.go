package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	rdsAuth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/go-redis/redis"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewDatabasePool(ctx context.Context, databaseConnURL string, awsRegion string) *pgxpool.Pool {
	pgPoolConfig, err := pgxpool.ParseConfig(databaseConnURL)
	if err != nil {
		slog.Error("failed to parse database connection URL", "error", err, "databaseConnURL", databaseConnURL)
		os.Exit(1)
	}
	pgPoolConfig.ConnConfig.ConnectTimeout = 10 * time.Second

	// authentication: we need to get a new token for every database connection - once connected, the token is allowed to expire without losing connection to the database
	pgPoolConfig.BeforeConnect = postgresBeforeConnect(ctx, pgPoolConfig.ConnConfig, awsRegion)

	databasePool, err := pgxpool.NewWithConfig(ctx, pgPoolConfig)
	if err != nil {
		slog.Error("failed to create database pool", "error", err, "databaseConnURL", databaseConnURL)
		os.Exit(1)
	}

	slog.Info("created database connection pool", "databaseConnURL", databaseConnURL, "awsRegion", awsRegion)
	return databasePool
}

func NewRedisClient(connURL string) redis.UniversalClient {
	// no TLS config + credentials provider: ElastiCache connectivity is often done without auth: we're relying on firewalls + private subnet to keep our data secure
	client := redis.NewClient(&redis.Options{
		Addr:         connURL,
		PoolSize:     0, // default pool size
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})

	slog.Info("created redis client", "connURL", connURL)
	return client
}

func postgresBeforeConnect(ctx context.Context, connCfg *pgx.ConnConfig, awsRegion string) func(ctx context.Context, cfg *pgx.ConnConfig) error {
	// ignore before connect configuration if awsRegion is not provided (because we are not running this on AWS)
	if awsRegion == "" {
		return nil
	}

	awsConfig, err := awsCfg.LoadDefaultConfig(ctx, awsCfg.WithRegion(awsRegion))
	if err != nil {
		slog.Error("creating postgres before connect: failed to load aws config", "error", err)
		os.Exit(1)
	}

	host := connCfg.Host
	port := connCfg.Port
	user := connCfg.User
	endpoint := fmt.Sprintf("%s:%d", host, port)

	return func(ctx context.Context, cfg *pgx.ConnConfig) error {
		slog.Info("building an RDS auth token", "endpoint", endpoint, "awsRegion", awsRegion, "user", user)

		// does not make a network call so this is safe to do before *each* connection
		token, err := rdsAuth.BuildAuthToken(ctx, endpoint, awsRegion, user, awsConfig.Credentials)
		if err != nil {
			return fmt.Errorf("build postgres auth token: %w", err)
		}
		cfg.Password = token
		return nil
	}
}
