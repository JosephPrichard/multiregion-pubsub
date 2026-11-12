package app

import (
	"context"
	"encoding/json/v2"
	"log/slog"

	"github.com/go-redis/redis"
)

// EventsPublisher is a publisher that uses fanout to distribute messages to multiple pubsub nodes
type EventsPublisher struct {
	redisClients []redis.UniversalClient
}

func NewEventsPublisher(redisClients []redis.UniversalClient) *EventsPublisher {
	return &EventsPublisher{redisClients: redisClients}
}

type InputEvent struct {
	VendingMachineID string `json:"vendingMachineIO"`
	EventKind        string `json:"eventKind"`
	Payload          any    `json:"payload"`
}

// Publish is a REST-api wrapper over a multi-region Redis config that publishes to instances in each region in the distributed system
func (publisher *EventsPublisher) Publish(ctx context.Context, channel string, event InputEvent) {
	for _, redisClient := range publisher.redisClients {
		// redisClient does not support context propagation, so do not propagate egCtx - if any publish call fails, log and respond with an error
		go func() {
			message, err := json.Marshal(event)
			if err != nil {
				slog.ErrorContext(ctx, "failed marshal pubsub event", "error", err)
				return
			}
			strMessage := string(message)

			// error is just logged, this operation is fire and forget - rely on redisClient retries to reduce dropped message rate
			recvCount, err := redisClient.Publish(channel, message).Result()
			if err != nil {
				slog.ErrorContext(ctx, "failed to publish a pubsub event", "error", err, "redisClient", redisClient)
			} else {
				slog.InfoContext(ctx, "sent pubsub event successfully",
					"recvCount", recvCount, "channel", channel, "message", strMessage, "redisClient", redisClient)
			}
		}()
	}
}
