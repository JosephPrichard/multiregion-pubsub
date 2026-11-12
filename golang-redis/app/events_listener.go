package app

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type EventsListener struct {
	lock             sync.Mutex
	eventSubscribers map[string][]Subscriber

	redisClient redis.UniversalClient
}

const (
	EventsChannelName = "vending-machine-events"
	StateChangeEvent  = "state-change"

	SlowSubscriberThreshold = time.Second * 5
)

type OutputEvent struct {
	VendingMachineID string         `json:"vendingMachineIO"`
	EventKind        string         `json:"eventKind"`
	Payload          jsontext.Value `json:"payload"`
}

func (o OutputEvent) AsVendingMachineEvent() *VendingMachine {
	var state VendingMachine
	err := json.Unmarshal(o.Payload, &state)
	if err != nil {
		slog.Error("unable to unmarshal vending machine event", "error", err, "event", o)
		return nil
	}
	return &state
}

type Subscriber struct {
	lastSendTime time.Time
	mailbox      chan OutputEvent
}

func NewEventsListener(redisClient redis.UniversalClient) *EventsListener {
	return &EventsListener{eventSubscribers: make(map[string][]Subscriber), redisClient: redisClient}
}

// Listen starts a background task to subscribe to all channels and re-publish them to the subscribers in the listener table
func (listener *EventsListener) Listen(ctx context.Context) {
	for {
		func() {
			pubsub := listener.redisClient.Subscribe(EventsChannelName)
			defer pubsub.Close()
			recvChan := pubsub.Channel()

			select {
			case message := <-recvChan:
				var event OutputEvent
				if err := json.Unmarshal([]byte(message.Payload), &event); err != nil {
					slog.Error("failed to deserialize pubsub message - skipping", "payload", message.Payload)
					break
				}
				slog.Info("received a pubsub message", "payload", message.Payload, "event", event)
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

	var indicesToRemove []int

	for index, subscriber := range subscribers {
		select {
		case subscriber.mailbox <- event:
		default:
			// don't block in case of back-pressure (slow consumer), just drop
			// if we had to drop AND it has been awhile since the last send was successful, mark this subscriber for removal
			if time.Now().Sub(subscriber.lastSendTime) > SlowSubscriberThreshold {
				indicesToRemove = append(indicesToRemove, index)
			}
		}
		// only close one-shot channels early, other subscribes are either expired or are closed explicitly by client
		if cap(subscriber.mailbox) == 1 {
			indicesToRemove = append(indicesToRemove, index)
		}
	}

	for indexToRemove := range indicesToRemove {
		close(subscribers[indexToRemove].mailbox)
	}
	for indexToRemove := range indicesToRemove {
		subscribers = slices.Delete(subscribers, indexToRemove, 1)
	}

	listener.eventSubscribers[vendingMachineID] = subscribers

	listener.lock.Unlock()
}

// DefaultChannelCapacity this is the buffer size until a consumer is "slow" and starts missing messages
const DefaultChannelCapacity = 16

// Subscribe subscribes to an event broker by vendingMachineID by using a buffered channel as a mailbox
// if the buffer size is one, mailboxes are "send once" and are removed from the table once consumed.
func (listener *EventsListener) Subscribe(vendingMachineID string, isOneshotChannel bool) chan OutputEvent {
	channelCapacity := DefaultChannelCapacity
	if isOneshotChannel {
		channelCapacity = 1
	}

	mailbox := make(chan OutputEvent, channelCapacity)
	subscriber := Subscriber{lastSendTime: time.Now(), mailbox: mailbox}

	listener.lock.Lock()

	subscribers := listener.eventSubscribers[vendingMachineID]
	subscribers = append(subscribers, subscriber)
	listener.eventSubscribers[vendingMachineID] = subscribers

	listener.lock.Unlock()

	return mailbox
}

// Unsubscribe unsubscribes from an event broker by vendingMachineID and channel address
func (listener *EventsListener) Unsubscribe(vendingMachineID string, mailbox chan OutputEvent) {
	listener.lock.Lock()

	subscribers := listener.eventSubscribers[vendingMachineID]

	isEqual := func(s Subscriber) bool { return s.mailbox == mailbox }

	index := slices.IndexFunc(subscribers, isEqual)
	if index >= 0 {
		close(mailbox)
	}
	subscribers = slices.Delete(subscribers, index, 1)

	listener.eventSubscribers[vendingMachineID] = subscribers

	listener.lock.Unlock()
}
