package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"uuid"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/go-redis/redis"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	vendingMachineService *VendingMachineService
	eventsPublisher       *EventsPublisher
	eventsListener        *EventsListener
}

func NewServer(vendingMachineService *VendingMachineService, eventsPublisher *EventsPublisher, eventsListener *EventsListener) Server {
	return Server{
		vendingMachineService: vendingMachineService,
		eventsPublisher:       eventsPublisher,
		eventsListener:        eventsListener,
	}
}

const (
	OkMesage       = "OK"
	TimeoutMessage = "Request timed out"
)

// CreateVendingMachine create a new idle vending machine or something
func (s *Server) CreateVendingMachine(_ http.ResponseWriter, r *http.Request) Response {
	ctx := r.Context()

	vendingMachineID := uuid.NewV7().String()

	vendingMachine, err := s.vendingMachineService.InsertVendingMachine(ctx, vendingMachineID)
	if err != nil {
		return Response{Payload: InternalServerError, Error: err, Status: http.StatusInternalServerError}
	}

	slog.InfoContext(ctx, "created vending machine", "vendingMachine", vendingMachine)
	return Response{Payload: vendingMachine, Status: http.StatusOK}
}

type RestockVendingMachineProductsBody struct {
	Products []Product `json:"products"`
}

// RestockVendingMachineProducts restock the vending machine, so I don't run out of Cola
func (s *Server) RestockVendingMachineProducts(_ http.ResponseWriter, r *http.Request) Response {
	ctx := r.Context()

	vendingMachineID := r.PathValue("id")

	body, err := readJSON[RestockVendingMachineProductsBody](r)
	if err != nil {
		return Response{Payload: err, Status: http.StatusBadRequest}
	}

	err = s.vendingMachineService.InsertManyVendingMachineProducts(ctx, vendingMachineID, body.Products)
	if errors.Is(err, VendingMachineNotFoundError) {
		return Response{Payload: err, Status: http.StatusNotFound}
	}
	if err != nil {
		return Response{Payload: InternalServerError, Error: err, Status: http.StatusInternalServerError}
	}

	vendingMachine, errorResponse := s.doGet(ctx, vendingMachineID)
	if errorResponse != nil {
		return *errorResponse
	}
	return Response{Payload: UpdateVendingMachineResponse{Message: OkMesage, VendingMachine: &vendingMachine}, Status: http.StatusOK}
}

// GetVendingMachine just return the vending machine with state and products
func (s *Server) GetVendingMachine(_ http.ResponseWriter, r *http.Request) Response {
	ctx := r.Context()

	vendingMachineID := r.PathValue("id")

	vendingMachine, errorResponse := s.doGet(ctx, vendingMachineID)
	if errorResponse != nil {
		return *errorResponse
	}

	slog.InfoContext(ctx, "retrieved vending machine", "vendingMachine", vendingMachine)
	return Response{Payload: vendingMachine, Status: http.StatusOK}
}

type BuyVendingMachineProductBody struct {
	ProductID string `json:"productId"`
}

type UpdateVendingMachineResponse struct {
	Message        string          `json:"message"`
	VendingMachine *VendingMachine `json:"vendingMachine"`
}

type WaitPurchaseResult struct {
	message     string
	error       error
	status      int
	stateUpdate *VendingMachine
}

// BuyVendingMachineProduct try to buy a product - VendingMachine must be in idle and product must be available (blocks until payment is complete or timeout)
// THIS is the call that is SUBSCRIBING to the event broker
func (s *Server) BuyVendingMachineProduct(_ http.ResponseWriter, r *http.Request) Response {
	ctx, cancel := context.WithTimeout(r.Context(), BuyProductTimeout)
	defer cancel()

	vendingMachineID := r.PathValue("id")

	body, err := readJSON[BuyVendingMachineProductBody](r)
	if err != nil {
		return Response{Payload: err, Status: http.StatusBadRequest}
	}

	productID := body.ProductID

	// try to perform a state transition - map result accordingly
	_, response := s.doUpdate(ctx, VendingMachineUpdate{VendingMachineID: vendingMachineID, NextState: AwaitingPaymentState, ProductID: productID})
	if response != nil {
		return *response
	}

	result := s.waitPurchaseResult(ctx, vendingMachineID)

	vendingMachine := result.stateUpdate

	if vendingMachine == nil {
		// select the new state of the vending machine (with the product we tried to buy removed) (without cancellation of course)
		v, errorResponse := s.doGet(context.Background(), vendingMachineID)
		if errorResponse != nil {
			return *errorResponse
		}
		vendingMachine = &v
	}

	return Response{
		Payload: UpdateVendingMachineResponse{Message: result.message, VendingMachine: vendingMachine},
		Error:   result.error,
		Status:  result.status,
	}
}

func (s *Server) waitPurchaseResult(ctx context.Context, vendingMachineID string) WaitPurchaseResult {
	// if successful, block the current thread until we get a signal that the payment has arrived
	slog.InfoContext(ctx, "waiting for vending machine purchase event", "vendingMachineID", vendingMachineID)

	var result WaitPurchaseResult

	oneshotChan := s.eventsListener.Subscribe(vendingMachineID, true)
	select {
	case event := <-oneshotChan:
		slog.InfoContext(ctx, "buy vending machine product: received payment event", "event", event)
		result = WaitPurchaseResult{message: OkMesage, stateUpdate: event.AsVendingMachineEvent(), status: http.StatusOK}
	case <-ctx.Done():
		result = WaitPurchaseResult{message: TimeoutMessage, error: ctx.Err(), status: http.StatusRequestTimeout}
	}

	return mapPurchaseResult(result)
}

func mapPurchaseResult(result WaitPurchaseResult) WaitPurchaseResult {
	// Override a timeout (received a message) with a distinct error case
	if result.stateUpdate != nil && result.stateUpdate.State == PaymentTimeoutState {
		result.message = TimeoutMessage
		result.status = http.StatusRequestTimeout
	}
	return result
}

type DoVendingMachinePaymentBody struct {
	ProductID string `json:"productId"`
}

// DoVendingMachinePayment pretend this is a webhook or whatever - VendingMachine must be awaiting a payment for this to succeed
// THIS is the call that is PUBLISHING to the event broker
func (s *Server) DoVendingMachinePayment(w http.ResponseWriter, r *http.Request) Response {
	ctx := r.Context()

	vendingMachineID := r.PathValue("id")

	body, err := readJSON[DoVendingMachinePaymentBody](r)
	if err != nil {
		return Response{Payload: err, Status: http.StatusBadRequest}
	}

	productID := body.ProductID

	// try to perform a state transition - map result accordingly
	vendingMachine, response := s.doUpdate(ctx, VendingMachineUpdate{VendingMachineID: vendingMachineID, NextState: DispensingState, ProductID: productID})
	if response != nil {
		return *response
	}

	event := InputEvent{VendingMachineID: vendingMachineID, EventKind: StateChangeEvent, Payload: vendingMachine}
	s.eventsPublisher.Publish(ctx, EventsChannelName, event)

	return Response{Payload: UpdateVendingMachineResponse{Message: OkMesage, VendingMachine: vendingMachine}, Status: http.StatusOK}
}

type OutputWsEvent struct {
	VendingMachine *VendingMachine `json:"vendingMachine"`
}

func (s *Server) StreamVendingMachineEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	websocketConn, err := websocket.Accept(w, r, nil)
	if err != nil {
		writeJSON(ctx, w, Response{Payload: InternalServerError, Error: err}, http.StatusInternalServerError)
		return
	}

	vendingMachineID := r.PathValue("id")

	subscriber := s.eventsListener.Subscribe(vendingMachineID, false)

	shutdown := sync.OnceFunc(func() {
		websocketConn.CloseNow()
		s.eventsListener.Unsubscribe(vendingMachineID, subscriber)
	})

	// READER: client to server (just drain the buffer and log for now)
	go func() {
		defer shutdown()
		for {
			_, payload, err := websocketConn.Read(ctx)
			if err != nil {
				slog.WarnContext(ctx, "failed to read websocket message", "error", err)
				return
			}
			slog.InfoContext(ctx, "received websocket message", "payload", string(payload))
		}
	}()

	// WRITER: server to client (send pub/sub events to client)
	go func() {
		defer shutdown()
		for event := range subscriber {
			output := OutputWsEvent{VendingMachine: event.AsVendingMachineEvent()}
			err = wsjson.Write(ctx, websocketConn, output)
			if err != nil {
				slog.ErrorContext(ctx, "failed to write websocket message", "error", err)
				continue
			}
		}
	}()
}

func (s *Server) doUpdate(ctx context.Context, updt VendingMachineUpdate) (*VendingMachine, *Response) {
	vendingMachine, err := s.vendingMachineService.UpdateVendingMachine(ctx, updt)

	if err != nil {
		// ERROR: error mappings for update operation
		if errors.Is(err, VendingMachineNotFoundError) {
			return nil, &Response{Payload: err, Status: http.StatusNotFound}
		}
		payload := UpdateVendingMachineResponse{Message: err.Error(), VendingMachine: vendingMachine}
		if _, isError := errors.AsType[*StateTransitionError](err); isError {
			return nil, &Response{Payload: payload, Status: http.StatusPreconditionFailed}
		}
		if _, isError := errors.AsType[*PaymentError](err); isError {
			return nil, &Response{Payload: payload, Status: http.StatusBadRequest}
		}
		if _, isError := errors.AsType[*UpdatePostConditionError](err); isError {
			return nil, &Response{Payload: payload, Status: http.StatusBadRequest}
		}

		return nil, &Response{Payload: InternalServerError, Error: err, Status: http.StatusInternalServerError}
	}

	return vendingMachine, nil
}

func (s *Server) doGet(ctx context.Context, vendingMachineID string) (VendingMachine, *Response) {
	vendingMachine, err := s.vendingMachineService.GetVendingMachine(ctx, vendingMachineID)
	if errors.Is(err, VendingMachineNotFoundError) {
		return VendingMachine{}, &Response{Payload: err, Status: http.StatusNotFound}
	}
	if err != nil {
		return VendingMachine{}, &Response{Payload: InternalServerError, Error: err, Status: http.StatusInternalServerError}
	}
	return vendingMachine, nil
}

type DatabaseClients struct {
	DatabasePool *pgxpool.Pool
	RedisClients []redis.UniversalClient
}

func NewRouter(clients DatabaseClients) (*http.ServeMux, func()) {
	riverProducerClient := NewRiverProducerClient(clients.DatabasePool)
	vendingMachineService := NewVendingMachineService(clients.DatabasePool, riverProducerClient)
	eventsPublisher := NewEventsPublisher(clients.RedisClients)
	eventsListener := NewEventsListener(clients.RedisClients[0])

	riverConsumerClient := NewRiverConsumerClient(clients.DatabasePool, vendingMachineService, eventsPublisher)
	if err := riverConsumerClient.Start(context.Background()); err != nil {
		slog.Error("failed to start river consumer client", "error", err)
		os.Exit(1)
	}

	eventsListenerCtx, cancelEventsListener := context.WithCancel(context.Background())
	go eventsListener.Listen(eventsListenerCtx)

	server := NewServer(vendingMachineService, eventsPublisher, eventsListener)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /vending-machine", rest(server.CreateVendingMachine))
	mux.HandleFunc("PUT /vending-machine/{id}/products", rest(server.RestockVendingMachineProducts))
	mux.HandleFunc("GET /vending-machine/{id}", rest(server.GetVendingMachine))
	mux.HandleFunc("POST /vending-machine/{id}/purchase", rest(server.BuyVendingMachineProduct))
	mux.HandleFunc("POST /vending-machine/{id}/payment", rest(server.DoVendingMachinePayment))
	mux.HandleFunc("GET /vending-machine/{id}/events", server.StreamVendingMachineEvents)

	shutdown := func() {
		var errs []error

		clients.DatabasePool.Close()
		for _, redisClient := range clients.RedisClients {
			errs = append(errs, redisClient.Close())
		}
		cancelEventsListener()

		errs = append(errs, riverConsumerClient.Stop(context.Background()))

		slog.Info("shutdown server", "errors", errs)
	}
	return mux, shutdown
}
