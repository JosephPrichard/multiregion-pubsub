package app

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"runtime/debug"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

func NewRiverProducerClient(database *pgxpool.Pool) *river.Client[pgx.Tx] {
	riverClient, err := river.NewClient(riverpgxv5.New(database), &river.Config{})
	if err != nil {
		slog.Error("failed to create river producer client", "error", err)
		os.Exit(1)
	}
	return riverClient
}

func NewRiverConsumerClient(database *pgxpool.Pool, service *VendingMachineService, publisher *EventsPublisher) *river.Client[pgx.Tx] {
	workers := river.NewWorkers()
	river.AddWorker(workers, NewUpdateVendingMachineWorker(service, publisher))

	riverClient, err := river.NewClient(riverpgxv5.New(database), &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 32},
		},
		Workers:      workers,
		ErrorHandler: &defaultErrorHandler{},
		Plugins: []rivertype.Plugin{
			&riverQueueMiddleware{},
		},
	})
	if err != nil {
		slog.Error("failed to create river consumer client", "error", err)
		os.Exit(1)
	}
	return riverClient
}

type riverQueueMiddleware struct {
	river.MiddlewareDefaults
}

func (m *riverQueueMiddleware) Work(
	ctx context.Context,
	job *rivertype.JobRow,
	doInner func(ctx context.Context) error,
) error {
	ctx = TracedContext(ctx, uuid.NewV7().String())

	slog.InfoContext(ctx, "riverqueue: before working job", "jobID", job.ID, "kind", job.Kind)

	err := doInner(ctx)

	if err != nil {
		slog.ErrorContext(ctx, "riverqueue: job failed", "jobID", job.ID, "error", err)
	} else {
		slog.InfoContext(ctx, "riverqueue: job succeeded", "jobID", job.ID)
	}

	return err
}

type NonRetryableError struct {
	Err error
}

func (error NonRetryableError) Error() string {
	return error.Err.Error()
}

type defaultErrorHandler struct{}

func (*defaultErrorHandler) HandleError(ctx context.Context, job *rivertype.JobRow, err error) *river.ErrorHandlerResult {
	_, isNotRetryable := errors.AsType[NonRetryableError](err)

	slog.ErrorContext(ctx, "river queue event error", "error", err, "job", job, "isNotRetryable", isNotRetryable)

	if isNotRetryable {
		// non-retryable errors are canceled so they don't stay in the queue
		return &river.ErrorHandlerResult{SetCancelled: true}
	}
	return nil
}

func (*defaultErrorHandler) HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *river.ErrorHandlerResult {
	slog.ErrorContext(ctx, "panic: river queue event", "err", panicVal, "riverTrace", trace, "job", job, "stack", string(debug.Stack()))
	return nil
}

type UpdateVendingMachineJob struct {
	VendingMachineID string
	NextState        VendingMachineState
}

func (job UpdateVendingMachineJob) Kind() string {
	return "update_vending_machine"
}

type UpdateVendingMachineWorker struct {
	river.WorkerDefaults[UpdateVendingMachineJob]
	service   *VendingMachineService
	publisher *EventsPublisher
}

func NewUpdateVendingMachineWorker(service *VendingMachineService, publisher *EventsPublisher) *UpdateVendingMachineWorker {
	return &UpdateVendingMachineWorker{service: service, publisher: publisher}
}

func (w *UpdateVendingMachineWorker) Work(ctx context.Context, job *river.Job[UpdateVendingMachineJob]) error {
	slog.InfoContext(ctx, "begin update vending machine job", "job", job.Args)

	mapErr := func(err error) error {
		_, isPaymentError := errors.AsType[*PaymentError](err)
		_, isStateTransitionError := errors.AsType[*StateTransitionError](err)
		isNotRetryable := isPaymentError || isStateTransitionError

		slog.ErrorContext(ctx, "failed to complete update vending machine job", "error", err, "isNotRetryable", isNotRetryable)

		if isNotRetryable {
			return NonRetryableError{Err: err}
		}
		return err
	}

	updt := VendingMachineUpdate{VendingMachineID: job.Args.VendingMachineID, NextState: job.Args.NextState}
	vendingMachine, err := w.service.UpdateVendingMachine(ctx, updt)
	if err != nil {
		return mapErr(err)
	}

	event := InputEvent{VendingMachineID: vendingMachine.ID, EventKind: StateChangeEvent, Payload: vendingMachine}
	w.publisher.Publish(ctx, EventsChannelName, event)

	slog.InfoContext(ctx, "complete update vending machine job", "vendingMachine", vendingMachine)
	return nil
}
