package app

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"golang.org/x/sync/errgroup"
)

type VendingMachineService struct {
	database    *pgxpool.Pool
	riverClient *river.Client[pgx.Tx]
}

func NewVendingMachineService(database *pgxpool.Pool, riverClient *river.Client[pgx.Tx]) *VendingMachineService {
	return &VendingMachineService{database: database, riverClient: riverClient}
}

func (service *VendingMachineService) InsertVendingMachine(ctx context.Context, vendingMachineID string) (VendingMachine, error) {
	vendingMachine := VendingMachine{
		ID:                   vendingMachineID,
		State:                IdleState,
		LastUpdatedTimestamp: time.Now(),
	}

	// language=SQL
	query := `INSERT INTO vending_machines (vending_machine_id, machine_state, last_updated_timestamp) VALUES ($1, $2, $3)`
	_, err := service.database.Exec(ctx, query, vendingMachine.ID, vendingMachine.State, vendingMachine.LastUpdatedTimestamp)
	if err != nil {
		return VendingMachine{}, fmt.Errorf("insert vending machine: %w", err)
	}

	return vendingMachine, nil
}

var VendingMachineNotFoundError = errors.New("vending machine is could not be found")

func (service *VendingMachineService) InsertManyVendingMachineProducts(ctx context.Context, vendingMachineID string, products []Product) error {
	var productCopyFromRows [][]any
	for _, product := range products {
		productCopyFromRows = append(productCopyFromRows, []any{
			uuid.NewV7(),
			vendingMachineID,
			product.Name,
			product.Price,
			product.Qty,
		})
	}

	count, err := service.database.CopyFrom(
		ctx,
		pgx.Identifier{"products"},
		[]string{"product_id", "vending_machine_id", "name", "price", "qty"},
		pgx.CopyFromRows(productCopyFromRows),
	)
	if dbErr := databaseInsertError(err); dbErr == ForeignKeyViolationErr {
		return VendingMachineNotFoundError
	}
	if err != nil {
		slog.ErrorContext(ctx, "failed to insert products", "error", err)
		return err
	}

	slog.InfoContext(ctx, "restocked vending machine", "count", count, "productCopyFromRows", productCopyFromRows)
	return nil
}

func (service *VendingMachineService) getVendingMachineProducts(ctx context.Context, vendingMachineID string) ([]Product, error) {
	// language=SQL
	query := `SELECT p.product_id, p.name, p.price, p.qty
			FROM products p
			INNER JOIN vending_machines v ON v.vending_machine_id = p.vending_machine_id
			WHERE v.vending_machine_id = $1`

	rows, err := service.database.Query(ctx, query, vendingMachineID)
	if err != nil {
		return nil, fmt.Errorf("select vending machine products: %w", err)
	}
	return UnmarshalRows(rows, MapProduct)
}

func (service *VendingMachineService) getVendingMachine(ctx context.Context, vendingMachineID string) (VendingMachine, error) {
	// language=SQL
	query := `SELECT vending_machine_id, machine_state, last_updated_timestamp, last_purchase_product_id
			FROM vending_machines
			WHERE vending_machine_id = $1`

	rows, err := service.database.Query(ctx, query, vendingMachineID)
	if err != nil {
		return VendingMachine{}, fmt.Errorf("select vending machine: %w", err)
	}
	return UnmarshalVendingMachine(rows)
}

func (service *VendingMachineService) GetVendingMachine(ctx context.Context, vendingMachineID string) (VendingMachine, error) {
	eg, egCtx := errgroup.WithContext(ctx)

	var vendingMachine VendingMachine
	var products []Product

	eg.Go(func() (err error) {
		vendingMachine, err = service.getVendingMachine(egCtx, vendingMachineID)
		return err
	})
	eg.Go(func() (err error) {
		products, err = service.getVendingMachineProducts(egCtx, vendingMachineID)
		return err
	})

	if err := eg.Wait(); err != nil {
		return VendingMachine{}, err
	}

	vendingMachine.SetProducts(products)
	return vendingMachine, nil
}

func (service *VendingMachineService) updateVendingMachineTx(ctx context.Context, updt VendingMachineUpdate) (*VendingMachine, error) {
	vendingMachineID := updt.VendingMachineID

	// BEGIN: begin the transaction: `Serializable` is required because we are performing validations: this prevents a "write skew"
	txn, err := service.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, err
	}
	defer func() {
		err := txn.Rollback(context.Background())
		slog.WarnContext(ctx, "rollback update vending machine txn", "error", err)
	}()

	// language=SQL READ: get the previous vending machine state for validations/short-circuiting on error
	query := `SELECT vending_machine_id, machine_state, last_updated_timestamp, last_purchase_product_id
			FROM vending_machines
			WHERE vending_machine_id = $1`

	rows, err := txn.Query(ctx, query, vendingMachineID)
	if err != nil {
		return nil, fmt.Errorf("select vending machine: %w", err)
	}
	vendingMachine, err := UnmarshalVendingMachine(rows)
	if err != nil {
		return nil, err
	}

	// LOGIC: compute the state update based of previous state and incoming updt
	updt = TransformUpdate(vendingMachine, updt)
	nextState := updt.NextState
	productID := updt.ProductID
	nextLastUpdatedTimestamp := time.Now()
	productChangeSet := updt.ProductChangeSet

	slog.InfoContext(ctx, "starting update vending machine operation", "currVendingMachine", vendingMachine, "updt", updt)

	// VALIDATE: do intermediate logic depending on the read state
	err = DoStateTransitionValidations(vendingMachine, nextState, productID)
	if err != nil {
		return &vendingMachine, err
	}

	// language=SQL WRITE: perform the immediate state change update
	updateQuery := `
			WITH u1 AS (
                UPDATE vending_machines
				SET machine_state = $2, last_updated_timestamp = $3, last_purchase_product_id = $4
				WHERE vending_machine_id = $1
			),
			u2 AS (
                UPDATE products
				SET qty = qty + $5
				WHERE product_id = $4
				RETURNING product_id, name, price, qty
			)
			SELECT * FROM u2`
	updateArgs := []any{
		vendingMachineID,
		nextState,
		nextLastUpdatedTimestamp,
		pgtype.Text{String: productID, Valid: productID != ""},
		productChangeSet.Quantity,
	}

	rows, err = txn.Query(ctx, updateQuery, updateArgs...)
	if err != nil {
		return nil, fmt.Errorf("update vending machine state: %w", err)
	}
	nextProductState, err := UnmarshalRows(rows, MapProduct)
	if err != nil {
		return nil, err
	}

	// POST-VALIDATION: if update query produced an invalid product state, return and rollback
	if len(nextProductState) > 0 {
		err = DoPostUpdateValidations(vendingMachine, nextProductState[0])
		if err != nil {
			return &vendingMachine, err
		}
	}

	// mirror the state change on remote database in server memory for immediate consumption
	vendingMachine.State = nextState
	vendingMachine.LastPurchaseProductID = productID

	// SCHEDULE: schedule a state change atomically if required
	nextScheduledState, ok := GetNextScheduledState(vendingMachine.State)
	if ok {
		scheduledAt := nextScheduledState.GetScheduledAt()
		job := &UpdateVendingMachineJob{VendingMachineID: vendingMachineID, NextState: nextScheduledState.NextState}
		insertOpts := &river.InsertOpts{ScheduledAt: scheduledAt}

		jobResult, err := service.riverClient.InsertTx(ctx, txn, *job, insertOpts)
		if err != nil {
			return nil, fmt.Errorf("schedule update job: %w", err)
		}

		slog.InfoContext(ctx, "scheduled update vending machine job", "job", job, "jobResult", jobResult, "insertOpts", insertOpts)
	}

	// END: commit the transaction/rollback
	if err := txn.Commit(ctx); err != nil {
		return nil, err
	}

	slog.InfoContext(ctx, "finished update vending machine operation", "vendingMachine", vendingMachine, "nextProductState", nextProductState, "updateArgs", updateArgs)
	return &vendingMachine, err
}

func (service *VendingMachineService) UpdateVendingMachine(ctx context.Context, updt VendingMachineUpdate) (*VendingMachine, error) {
	vendingMachineID := updt.VendingMachineID
	nextState := updt.NextState

	slog.InfoContext(ctx, "begin vending machine update", "updt", updt)

	if !slices.Contains(VendingMachineStates, nextState) {
		return nil, fmt.Errorf("state is invalid: %s", nextState)
	}

	vendingMachine, err := service.updateVendingMachineTx(ctx, updt)
	if vendingMachine == nil || err != nil {
		return vendingMachine, err
	}

	// getVendingMachineProducts is done outside a transaction, we're not necessarily expecting it to be consistent in regard to the update, just get the latest products
	products, err := service.getVendingMachineProducts(ctx, vendingMachineID)
	if err != nil {
		return nil, err
	}
	vendingMachine.SetProducts(products)

	slog.InfoContext(ctx, "selected products after update vending machine operation", "vendingMachine", vendingMachine)
	return vendingMachine, nil
}
