package main

import (
	"sync"
)

type EventsListener struct {
	lock             sync.Mutex
	eventSubscribers map[string][]chan VendingMachineEvent

	eventsBrokerRemote EventsBrokerRemoteClient
}

func NewEventsListener(eventsBrokerRemote EventsBrokerRemoteClient) EventsListener {
	return EventsListener{eventSubscribers: make(map[string][]chan VendingMachineEvent), eventsBrokerRemote: eventsBrokerRemote}
}

// Listen starts a background task to subscribe to all channels and re-publish them to the subscribers in the listener table
func (listener *EventsListener) Listen() error {
	recvChan, err := listener.eventsBrokerRemote.SubscribeVendingMachineEvents()
	if err != nil {
		return err
	}
	for event := range recvChan {
		listener.publish(event.VendingMachineID, event)
	}
	return nil
}

// publish notify all subscribers to a vendingMachineID event by sending on each mailbox and then closing them
// removing the mailboxes from the table is done atomically to prevent sends on closed channels
func (listener *EventsListener) publish(vendingMachineID string, event VendingMachineEvent) {
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
func (listener *EventsListener) Subscribe(vendingMachineID string) chan VendingMachineEvent {
	oneShotChan := make(chan VendingMachineEvent, 1)

	listener.lock.Lock()

	subscribers := listener.eventSubscribers[vendingMachineID]
	subscribers = append(subscribers, oneShotChan)
	listener.eventSubscribers[vendingMachineID] = subscribers

	listener.lock.Unlock()

	return oneShotChan
}
