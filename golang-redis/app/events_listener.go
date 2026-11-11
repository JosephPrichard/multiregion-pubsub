package app

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"log/slog"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type EventsListener struct {
	lock             sync.Mutex
	eventSubscribers map[string][]chan OutputEvent

	redisClient redis.UniversalClient
}

const (
	ChannelName      = "vending-machine-events"
	PaymentEventKind = "payment"
)

type OutputEvent struct {
	VendingMachineID string         `json:"vendingMachineIO"`
	EventKind        string         `json:"eventKind"`
	Payload          jsontext.Value `json:"payload"`
}

func NewEventsListener(redisClient redis.UniversalClient) *EventsListener {
	return &EventsListener{eventSubscribers: make(map[string][]chan OutputEvent), redisClient: redisClient}
}

// Listen starts a background task to subscribe to all channels and re-publish them to the subscribers in the listener table
func (listener *EventsListener) Listen(ctx context.Context) {
	for {
		func() {
			pubsub := listener.redisClient.Subscribe(ChannelName)
			defer pubsub.Close()
			recvChan := pubsub.Channel()

			select {
			case message := <-recvChan:
				var event OutputEvent
				if err := json.Unmarshal([]byte(message.Payload), &event); err != nil {
					slog.Error("failed to deserialize pubsub message - skipping", "payload", message.Payload)
					break
				}
				slog.Info("received a pubsub message", "payload", message.Payload)
				listener.publish(event.VendingMachineID, event)
			case <-ctx.Done():
				slog.Info("stopped listening to pubsub messages", "error", ctx.Err())
				return
			}
		}()
		// restart the subscriber automatically with a delay if it fails for any reason
		time.Sleep(time.Second)
	}
}

// publish notify all subscribers to a vendingMachineID event by sending on each mailbox and then closing them
// removing the mailboxes from the table is done atomically to prevent sends on closed channels
func (listener *EventsListener) publish(vendingMachineID string, event OutputEvent) {
	listener.lock.Lock()

	subscribers := listener.eventSubscribers[vendingMachineID]
	for _, subscriber := range subscribers {
		subscriber <- event
		close(subscriber)
	}
	delete(listener.eventSubscribers, vendingMachineID)

	listener.lock.Unlock()
}

// Subscribe subscribes to an event by vendingMachineID by using a buffered channel as a mailbox.
// mailboxes are "send once" and are removed from the table once consumed.
// since they are buffered, we don't need to explicitly remove them (they will not block if no one is on the other side).
func (listener *EventsListener) Subscribe(vendingMachineID string) chan OutputEvent {
	oneShotChan := make(chan OutputEvent, 1)

	listener.lock.Lock()

	subscribers := listener.eventSubscribers[vendingMachineID]
	subscribers = append(subscribers, oneShotChan)
	listener.eventSubscribers[vendingMachineID] = subscribers

	listener.lock.Unlock()

	return oneShotChan
}
