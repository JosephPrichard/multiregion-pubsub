package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"uuid"

	"github.com/go-redis/redis"
)

type Server struct {
	vendingMachineRepo *VendingMachineRepo
	eventsPublisher    *EventsPublisher
	eventsListener     *EventsListener
}

func NewServer(vendingMachineRepo *VendingMachineRepo, eventsPublisher *EventsPublisher, eventsListener *EventsListener) Server {
	return Server{
		vendingMachineRepo: vendingMachineRepo,
		eventsPublisher:    eventsPublisher,
		eventsListener:     eventsListener,
	}
}

// CreateVendingMachine create a new idle vending machine or something
func (s *Server) CreateVendingMachine(_ http.ResponseWriter, r *http.Request) *RestResponse {
	ctx := r.Context()

	vendingMachineID := uuid.NewV7().String()

	vendingMachine, err := s.vendingMachineRepo.InsertVendingMachine(ctx, vendingMachineID)
	if err != nil {
		return &RestResponse{Payload: InternalServerError, Error: err, Status: http.StatusInternalServerError}
	}

	slog.InfoContext(ctx, "created vending machine", "vendingMachine", vendingMachine)
	return &RestResponse{Payload: vendingMachine, Status: http.StatusOK}
}

type RestockVendingMachineProductsBody struct {
	Products []Product `json:"products"`
}

// RestockVendingMachineProducts restock the vending machine, so I don't run out of Cola
func (s *Server) RestockVendingMachineProducts(_ http.ResponseWriter, r *http.Request) *RestResponse {
	ctx := r.Context()

	vendingMachineID := r.PathValue("id")

	body, err := readJSON[RestockVendingMachineProductsBody](r)
	if err != nil {
		return &RestResponse{Payload: err, Status: http.StatusBadRequest}
	}

	err = s.vendingMachineRepo.InsertManyVendingMachineProducts(ctx, vendingMachineID, body.Products)
	if errors.Is(err, VendingMachineNotFoundError) {
		return &RestResponse{Payload: err, Status: http.StatusNotFound}
	}
	if err != nil {
		return &RestResponse{Payload: InternalServerError, Error: err, Status: http.StatusInternalServerError}
	}

	vendingMachine, errorResponse := s.doGet(ctx, vendingMachineID)
	if errorResponse != nil {
		return errorResponse
	}
	return &RestResponse{Payload: UpdateVendingMachineResponse{Message: "OK", VendingMachine: &vendingMachine}, Status: http.StatusOK}
}

// GetVendingMachine just return the vending machine with state and products
func (s *Server) GetVendingMachine(_ http.ResponseWriter, r *http.Request) *RestResponse {
	ctx := r.Context()

	vendingMachineID := r.PathValue("id")

	vendingMachine, errorResponse := s.doGet(ctx, vendingMachineID)
	if errorResponse != nil {
		return errorResponse
	}

	slog.InfoContext(ctx, "retrieved vending machine", "vendingMachine", vendingMachine)
	return &RestResponse{Payload: vendingMachine, Status: http.StatusOK}
}

type BuyVendingMachineProductBody struct {
	ProductID string `json:"productId"`
}

type UpdateVendingMachineResponse struct {
	Message        string          `json:"message"`
	VendingMachine *VendingMachine `json:"vendingMachine"`
}

type WaitPurchaseResult struct {
	message string
	error   error
	status  int
}

// BuyVendingMachineProduct try to buy a product - VendingMachine must be in idle and product must be available (blocks until payment is complete or timeout)
// THIS is the call that is SUBSCRIBING to the event broker
func (s *Server) BuyVendingMachineProduct(_ http.ResponseWriter, r *http.Request) *RestResponse {
	ctx, cancel := context.WithTimeout(r.Context(), BuyProductTimeout)
	defer cancel()

	vendingMachineID := r.PathValue("id")

	body, err := readJSON[BuyVendingMachineProductBody](r)
	if err != nil {
		return &RestResponse{Payload: err, Status: http.StatusBadRequest}
	}

	productID := body.ProductID

	// try to perform a state transition - map result accordingly
	response := s.doUpdate(ctx, vendingMachineID, AwaitingPaymentState, productID)
	if response != nil {
		return response
	}

	// if successful, block the current thread until we get a signal that the payment has arrived
	slog.InfoContext(ctx, "waiting for vendingMachine purchase event", "vendingMachineID", vendingMachineID)

	var result WaitPurchaseResult

	oneshotChan := s.eventsListener.Subscribe(vendingMachineID)
	select {
	case event := <-oneshotChan:
		slog.InfoContext(ctx, "buy vending machine product: received payment event", "event", event)
		result = WaitPurchaseResult{message: "OK", status: http.StatusOK}
	case <-ctx.Done():
		result = WaitPurchaseResult{message: "Request timed out", error: ctx.Err(), status: http.StatusRequestTimeout}
	}

	// select the new state of the vending machine (with the product we tried to buy removed) (without cancellation of course)
	vendingMachine, errorResponse := s.doGet(context.Background(), vendingMachineID)
	if errorResponse != nil {
		return errorResponse
	}

	return &RestResponse{
		Payload: UpdateVendingMachineResponse{Message: result.message, VendingMachine: &vendingMachine},
		Error:   result.error,
		Status:  result.status,
	}
}

type DoVendingMachinePaymentBody struct {
	ProductID string `json:"productId"`
}

// DoVendingMachinePayment pretend this is a webhook or whatever - VendingMachine must be awaiting a payment for this to succeed
// THIS is the call that is PUBLISHING to the event broker
func (s *Server) DoVendingMachinePayment(w http.ResponseWriter, r *http.Request) *RestResponse {
	ctx := r.Context()

	vendingMachineID := r.PathValue("id")

	body, err := readJSON[DoVendingMachinePaymentBody](r)
	if err != nil {
		return &RestResponse{Payload: err, Status: http.StatusBadRequest}
	}

	productID := body.ProductID

	// try to perform a state transition - map result accordingly
	response := s.doUpdate(ctx, vendingMachineID, DispensingState, productID)
	if response != nil {
		return response
	}

	event := InputEvent{VendingMachineID: vendingMachineID, EventKind: PaymentEventKind}
	s.eventsPublisher.Publish(ctx, ChannelName, event)

	// select the new state of the vending machine (with the product we tried to buy removed)
	vendingMachine, errorResponse := s.doGet(ctx, vendingMachineID)
	if errorResponse != nil {
		return errorResponse
	}

	return &RestResponse{Payload: UpdateVendingMachineResponse{Message: "OK", VendingMachine: &vendingMachine}, Status: http.StatusOK}
}

func (s *Server) doUpdate(ctx context.Context, vendingMachineID string, nextState string, productID string) *RestResponse {
	vendingMachine, err := s.vendingMachineRepo.DoVendingMachineUpdate(ctx, vendingMachineID, nextState, productID)
	if errors.Is(err, VendingMachineNotFoundError) {
		return &RestResponse{Payload: err, Status: http.StatusNotFound}
	}
	if err, ok := errors.AsType[*StateTransitionError](err); ok {
		return &RestResponse{
			Payload: UpdateVendingMachineResponse{Message: err.Error(), VendingMachine: &vendingMachine},
			Status:  http.StatusPreconditionFailed,
		}
	}
	if err, ok := errors.AsType[*PaymentError](err); ok {
		return &RestResponse{
			Payload: UpdateVendingMachineResponse{Message: err.Error(), VendingMachine: &vendingMachine},
			Status:  http.StatusBadRequest,
		}
	}
	if err != nil {
		return &RestResponse{Payload: InternalServerError, Error: err, Status: http.StatusInternalServerError}
	}
	return nil
}

func (s *Server) doGet(ctx context.Context, vendingMachineID string) (VendingMachine, *RestResponse) {
	vendingMachine, err := s.vendingMachineRepo.GetVendingMachine(ctx, vendingMachineID)
	if errors.Is(err, VendingMachineNotFoundError) {
		return VendingMachine{}, &RestResponse{Payload: err, Status: http.StatusNotFound}
	}
	if err != nil {
		return VendingMachine{}, &RestResponse{Payload: InternalServerError, Error: err, Status: http.StatusInternalServerError}
	}
	return vendingMachine, nil
}

type RouterConfig struct {
	DatabaseConnURI string
	RedisNodeURIs   []string // list of all Redis node URIs to publish to, the 1st one should be the node we will subscribe from
	AwsRegion       string
}

func NewRouter(ctx context.Context, cfg RouterConfig) (*http.ServeMux, func()) {
	if len(cfg.RedisNodeURIs) == 0 {
		slog.Error("requires at least one redis node in config", "redisNodeURIs", cfg.RedisNodeURIs)
		os.Exit(1)
	}

	stderrHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(NewLogRecordHandler("VendingMachinesService", stderrHandler)))

	databasePool := NewDatabasePool(ctx, cfg.DatabaseConnURI, cfg.AwsRegion)

	var redisClients []redis.UniversalClient
	for _, redisNodeURI := range cfg.RedisNodeURIs {
		redisClients = append(redisClients, NewRedisClient(redisNodeURI))
	}

	vendingMachineRepo := NewVendingMachineRepo(databasePool)
	eventsPublisher := NewEventsPublisher(redisClients)
	eventsListener := NewEventsListener(redisClients[0])

	// poor man's database migration
	commandTag, err := databasePool.Exec(ctx, VendingMachineSchema)
	slog.InfoContext(ctx, "executed create schema", "error", err, "commandTag", commandTag)

	eventsListenerCtx, cancelEventsListener := context.WithCancel(context.Background())
	go eventsListener.Listen(eventsListenerCtx)

	server := NewServer(vendingMachineRepo, eventsPublisher, eventsListener)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /vending-machine", rest(server.CreateVendingMachine))
	mux.HandleFunc("PUT /vending-machine/{id}/products", rest(server.RestockVendingMachineProducts))
	mux.HandleFunc("GET /vending-machine/{id}", rest(server.GetVendingMachine))
	mux.HandleFunc("POST /vending-machine/{id}/products", rest(server.BuyVendingMachineProduct))
	mux.HandleFunc("POST /vending-machine/{id}/payment", rest(server.DoVendingMachinePayment))

	shutdown := func() {
		slog.Info("shutting down server")

		databasePool.Close()
		for _, redisClient := range redisClients {
			redisClient.Close()
		}
		cancelEventsListener()
	}

	return mux, shutdown
}
