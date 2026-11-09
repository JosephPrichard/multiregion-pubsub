package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/go-redis/redis"
	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"
)

// EventBrokerServer is an http server designed to wrap multi-region pub/sub functionality in a single API
type EventBrokerServer struct {
	redisClients    map[string]redis.UniversalClient
	selfRedisClient redis.UniversalClient
	selfAwsRegion   string
}

func NewEventBrokerServer(redisClients map[string]redis.UniversalClient, selfRedisClient redis.UniversalClient) EventBrokerServer {
	return EventBrokerServer{redisClients: redisClients, selfRedisClient: selfRedisClient}
}

type PublishEventBody struct {
	Channel string          `json:"channel"` // channel is full passthrough to redis, if channel does not exist it will be created
	Payload json.RawMessage `json:"payload"` // don't fully deserialize it, eventBroker is passthrough for pub/sub payloads
}

// Publish is a REST-api wrapper over a multi-region Redis config that publishes to instances in each region in the distributed system
func (s *EventBrokerServer) Publish(_ http.ResponseWriter, r *http.Request) *RestResponse {
	ctx := r.Context()

	body, err := readJSON[PublishEventBody](r)
	if err != nil {
		return &RestResponse{Payload: ServiceResponse{Message: err.Error()}, Status: http.StatusBadRequest}
	}

	var eg errgroup.Group

	for _, redisClient := range s.redisClients {
		eg.Go(func() error {
			// redisClient does not support context propagation, so do not propagate egCtx - if any publish call fails, log and respond with an error
			recvCount, err := redisClient.Publish(body.Channel, body.Payload).Result()
			if err != nil {
				slog.ErrorContext(ctx, "failed to publish an event", err)
				return err
			}
			slog.InfoContext(ctx, "published event successfully", "recvCount", recvCount, "channel", body.Channel)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return &RestResponse{Error: err, Status: http.StatusInternalServerError}
	}

	return &RestResponse{Status: http.StatusOK}
}

type SubscribeEventBody struct {
	Channels []string `json:"channels"`
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// we don't care about origin checking because our event broker service will not be exposed to the internet
		return true
	},
}

// Subscribe is a WebSocket wrapper over channel subscribe that provides an easy-to-understand SSE interface so anyone with an HTTP-client can consume
func (s *EventBrokerServer) Subscribe(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	channelsStr := r.URL.Query().Get("channels")
	channels := strings.Split(channelsStr, ",")

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, InternalServerError.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	closeSignal := make(chan struct{}, 1)

	// reader: drain the reader, forward close signal to writer when done
	conn.SetReadLimit(512)
	go func() {
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				slog.InfoContext(ctx, "subscriber: finished reading", "error", err)
				closeSignal <- struct{}{}
				return
			}
		}
	}()

	pubsub := s.selfRedisClient.Subscribe(channels...)
	defer pubsub.Close()

	// writer: just forward each message we receive directly to the caller.
	subscriberChannel := pubsub.Channel()
	for {
		select {
		case redisMessage, ok := <-subscriberChannel:
			// received server sent close signal
			if !ok {
				slog.InfoContext(ctx, "subscriber: pubsub connection was closed")
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, []byte(redisMessage.Payload)); err != nil {
				slog.ErrorContext(ctx, "subscriber: failed to write message", "error", err)
			}
		// received client sent close signal
		case <-closeSignal:
			slog.InfoContext(ctx, "subscriber: finished writing", "error", ctx.Err())
			return
		}
	}
}

type EventBrokerRouterConfig struct {
	RedisClientURLs map[string]string
	AwsRegion       string
}

func NewEventBrokerRouter(cfg EventBrokerRouterConfig) *http.ServeMux {
	stderrHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(
		NewLogRecordHandler("EventBrokerService", stderrHandler),
	))

	redisClients := make(map[string]redis.UniversalClient)
	for awsRegion, connURL := range cfg.RedisClientURLs {
		redisClients[awsRegion] = NewRedisClient(connURL)
	}
	if redisClients[cfg.AwsRegion] == nil {
		slog.Error("missing redis client configuration for current region - this is certainly a mistake", "redisClients", redisClients, "awsRegion", cfg.AwsRegion)
		os.Exit(1)
	}

	server := NewEventBrokerServer(redisClients, redisClients[cfg.AwsRegion])

	mux := http.NewServeMux()

	mux.HandleFunc("GET /events", server.Subscribe)
	mux.HandleFunc("POST /events", rest(server.Publish))

	return mux
}
