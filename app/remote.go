package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/gorilla/websocket"
)

type EventBrokerClient struct {
	http               *http.Client
	eventBrokerHost    string
	eventBrokerBaseURL string
}

func NewEventsBrokerClient(httpClient *http.Client, eventBrokerHost string, eventBrokerBaseURL string) *EventBrokerClient {
	return &EventBrokerClient{http: httpClient, eventBrokerHost: eventBrokerHost, eventBrokerBaseURL: eventBrokerBaseURL}
}

const (
	VendingMachineEventsChannelName = "payment-events"
	PaymentEventKind                = "payment"
)

type VendingMachineEvent struct {
	VendingMachineID string `json:"vendingMachineIO"`
	EventKind        string `json:"eventKind"`
}

type PublishEventRemoteBody struct {
	Channel string `json:"channel"`
	Payload any    `json:"payload"`
}

func (client *EventBrokerClient) PublishEvent(ctx context.Context, event VendingMachineEvent) error {
	requestBody, err := json.Marshal(PublishEventRemoteBody{
		Channel: VendingMachineEventsChannelName,
		Payload: event,
	})
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	slog.InfoContext(ctx, "remote client: begin publish event call", "requestBody", string(requestBody))

	endpoint := url.URL{Scheme: "http", Host: client.eventBrokerHost, Path: fmt.Sprintf("%s/publish", client.eventBrokerBaseURL)}

	req, err := http.NewRequest(http.MethodPost, endpoint.String(), bytes.NewReader(requestBody)) // do not propagate cancellation - fire and forget
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	resp, err := client.http.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	slog.InfoContext(ctx, "remote client: finished publish event call", "respBody", string(respBody))
	return nil
}

func (client *EventBrokerClient) SubscribeEvents() (chan VendingMachineEvent, error) {
	endpoint := url.URL{Scheme: "ws", Host: client.eventBrokerHost, Path: fmt.Sprintf("%s/subscribe", client.eventBrokerBaseURL)}

	conn, _, err := websocket.DefaultDialer.Dial(endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("dial websocket: %w", err)
	}

	eventsChan := make(chan VendingMachineEvent)

	// stream messages to the response channel: close when context is called (body is closed)
	go func() {
		defer conn.Close()
		defer close(eventsChan)
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				slog.Info("websocket client: read error", "error", err)
				return
			}
			var event VendingMachineEvent
			if err := json.Unmarshal(message, &event); err != nil {
				slog.Error("websocket client: failed to unmarshal event", "error", err)
				continue
			}
			slog.Info("websocket client: unmarshalled event", "event", event)
			eventsChan <- event
		}
	}()

	return eventsChan, nil
}
