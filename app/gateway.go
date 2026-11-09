package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"
	"uuid"
)

// GatewayServer is exposed to the internet
// the problem it solves is "contrived": suppose we have a VendingMachine service with 4 states: Idle, AwaitingPayment, Dispensing, OrderComplete
// Idle            ---(on purchase)----------> AwaitingPayment
// AwaitingPayment ---(on payment succeed)---> Dispensing
// AwaitingPayment ---(on payment fails)-----> Dispensing
// AwaitingPayment ---(times out)------------> Idle
// Dispensing      ---(after a 15s delay)--------> OrderComplete
// OrderComplete   ---(after a 5s delay)--------> Idle
// the the following REST API implements the above state machine, with some small validations per state transition
type GatewayServer struct {
	vendingMachineRepo VendingMachineRepo
	eventBrokersRemote EventsBrokerRemoteClient
}

func NewGatewayServer(vendingMachineRepo VendingMachineRepo, eventBrokersRemote EventsBrokerRemoteClient) GatewayServer {
	return GatewayServer{
		vendingMachineRepo: vendingMachineRepo,
		eventBrokersRemote: eventBrokersRemote,
	}
}

// CreateVendingMachine create a new idle vending machine or something
func (s *GatewayServer) CreateVendingMachine(_ http.ResponseWriter, r *http.Request) *RestResponse {
	ctx := r.Context()

	vendingMachineID := uuid.NewV7().String()

	vendingMachine, err := s.vendingMachineRepo.InsertVendingMachine(ctx, vendingMachineID)
	if err != nil {
		return &RestResponse{Error: InternalServerError, Status: http.StatusInternalServerError}
	}

	slog.InfoContext(ctx, "created vending machine", "vendingMachine", vendingMachine)
	return &RestResponse{Payload: vendingMachine, Status: http.StatusOK}
}

type RestockVendingMachineProductsBody struct {
	VendingMachineID string    `json:"vendingMachineId"`
	Products         []Product `json:"products"`
}

// RestockVendingMachineProducts restock the vending machine, so I don't run out of Cola
func (s *GatewayServer) RestockVendingMachineProducts(_ http.ResponseWriter, r *http.Request) *RestResponse {
	ctx := r.Context()

	body, err := readJSON[RestockVendingMachineProductsBody](r)
	if err != nil {
		return &RestResponse{Payload: ServiceResponse{Message: err.Error()}, Status: http.StatusBadRequest}
	}

	err = s.vendingMachineRepo.InsertManyVendingMachineProducts(ctx, body.VendingMachineID, body.Products)
	if err == VendingMachineNotFoundError {
		return &RestResponse{Payload: ServiceResponse{Message: VendingMachineNotFoundError.Error()}, Status: http.StatusBadRequest}
	}
	if err != nil {
		return &RestResponse{Error: InternalServerError, Status: http.StatusInternalServerError}
	}

	return &RestResponse{Status: http.StatusOK}
}

// GetVendingMachine just return the vending machine with state and products
func (s *GatewayServer) GetVendingMachine(_ http.ResponseWriter, r *http.Request) *RestResponse {
	ctx := r.Context()

	vendingMachineID := r.PathValue("id")

	vendingMachine, err := s.vendingMachineRepo.GetVendingMachine(ctx, vendingMachineID)
	if err != nil {
		return &RestResponse{Error: InternalServerError, Status: http.StatusInternalServerError}
	}

	slog.InfoContext(ctx, "retrieved vending machine", "vendingMachine", vendingMachine)
	return &RestResponse{Payload: vendingMachine, Status: http.StatusOK}
}

type BuyVendingMachineProductBody struct {
	VendingMachineID string `json:"vendingMachineId"`
	ProductID        string `json:"productID"`
}

type BuyVendingMachineProductResp struct {
	Message        string               `json:"message"`
	VendingMachine *VendingMachine      `json:"vendingMachine"`
	CurrentState   *UpdateMachineResult `json:"currentState"`
}

var BuyVendingMachineProductInvalid = errors.New("cannot buy a product in the current state")

// BuyVendingMachineProduct try to buy a product - VendingMachine must be in idle and product must be available (blocks until payment is complete or timeout)
// THIS is the call that is SUBSCRIBING to the event broker
func (s *GatewayServer) BuyVendingMachineProduct(_ http.ResponseWriter, r *http.Request) *RestResponse {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second*60)
	defer cancel()

	body, err := readJSON[BuyVendingMachineProductBody](r)
	if err != nil {
		return &RestResponse{Payload: ServiceResponse{Message: err.Error()}, Status: http.StatusBadRequest}
	}

	vendingMachineID := body.VendingMachineID

	// try to perform a state transition - map result accordingly
	response := s.doUpdate(ctx, vendingMachineID, AwaitingPaymentState)
	if response != nil {
		return response
	}

	// if successful, block the current thread until we get a signal that the payment has arrived

	// select the new state of the vending machine (with the product we tried to buy removed)
	vendingMachine, err := s.vendingMachineRepo.GetVendingMachine(ctx, vendingMachineID)
	if err != nil {
		return &RestResponse{Error: InternalServerError, Status: http.StatusInternalServerError}
	}

	return &RestResponse{Payload: BuyVendingMachineProductResp{Message: "OK", VendingMachine: &vendingMachine}, Status: http.StatusOK}
}

type DoVendingMachinePaymentBody struct {
	VendingMachineID string `json:"vendingMachineId"`
	ProductID        string `json:"productID"`
}

type DoVendingMachinePaymentResp struct {
	Message        string               `json:"message"`
	VendingMachine *VendingMachine      `json:"vendingMachine"`
	CurrentState   *UpdateMachineResult `json:"currentState"`
}

// DoVendingMachinePayment pretend this is a webhook or whatever - VendingMachine must be awaiting a payment for this to succeed
// THIS is the call that is PUBLISHING to the event broker
func (s *GatewayServer) DoVendingMachinePayment(w http.ResponseWriter, r *http.Request) *RestResponse {
	ctx := r.Context()

	body, err := readJSON[DoVendingMachinePaymentBody](r)
	if err != nil {
		return &RestResponse{Payload: ServiceResponse{Message: err.Error()}, Status: http.StatusBadRequest}
	}

	vendingMachineID := body.VendingMachineID

	// try to perform a state transition - map result accordingly
	response := s.doUpdate(ctx, vendingMachineID, OrderCompleteState)
	if response != nil {
		return response
	}

	go func() {
		event := VendingMachineEvent{VendingMachineID: vendingMachineID, EventKind: PaymentEventKind}
		err := s.eventBrokersRemote.PublishVendingMachineEvent(context.WithoutCancel(ctx), event)
		if err != nil {
			slog.ErrorContext(ctx, "failed to publish payment event to events broker", "error", err)
		}
	}()

	// select the new state of the vending machine (with the product we tried to buy removed)
	vendingMachine, err := s.vendingMachineRepo.GetVendingMachine(ctx, vendingMachineID)
	if err != nil {
		return &RestResponse{Error: InternalServerError, Status: http.StatusInternalServerError}
	}

	return &RestResponse{Payload: vendingMachine, Status: http.StatusInternalServerError}
}

func (s *GatewayServer) doUpdate(ctx context.Context, vendingMachineID string, state string) *RestResponse {
	result, err := s.vendingMachineRepo.UpdateVendingMachine(ctx, vendingMachineID, state)
	if err == VendingMachinStateTransitionInvalid {
		// result is guaranteed to be initialized when `VendingMachinStateTransitionInvalid` is returned
		return &RestResponse{
			Payload: BuyVendingMachineProductResp{Message: BuyVendingMachineProductInvalid.Error(), CurrentState: &result},
			Status:  http.StatusBadRequest,
		}
	}
	if err != nil {
		return &RestResponse{Error: InternalServerError, Status: http.StatusInternalServerError}
	}
	return nil
}

type GateRouterConfig struct {
	DatabaseConnURL    string
	EventBrokerHost    string
	EventBrokerBaseURL string
	AwsRegion          string
}

func NewGatewayRouter(ctx context.Context, cfg GateRouterConfig) *http.ServeMux {
	stderrHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(
		NewLogRecordHandler("GatewayService", stderrHandler),
	))

	databasePool := NewDatabasePool(ctx, cfg.DatabaseConnURL, cfg.AwsRegion)
	httpClient := http.DefaultClient

	vendingMachineRepo := NewVendingMachineRepo(databasePool)
	eventBrokersRemote := NewEventsBrokerRemoteClient(httpClient, cfg.EventBrokerHost, cfg.EventBrokerBaseURL)

	go func() {
		// just set the entire schema every single time we start the app, I am way too lazy to set up Goose
		commandTag, err := databasePool.Exec(ctx, VendingMachineSchema)
		slog.InfoContext(ctx, "executed create schema", "error", err, "commandTag", commandTag)
	}()

	server := NewGatewayServer(vendingMachineRepo, eventBrokersRemote)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /vending-machine", rest(server.CreateVendingMachine))
	mux.HandleFunc("PUT /vending-machine/{id}/products", rest(server.RestockVendingMachineProducts))
	mux.HandleFunc("GET /vending-machine/{id}", rest(server.GetVendingMachine))
	mux.HandleFunc("POST /vending-machine/{id}/products", rest(server.BuyVendingMachineProduct))
	mux.HandleFunc("POST /vending-machine/{id}/payment", rest(server.DoVendingMachinePayment))

	return mux
}
