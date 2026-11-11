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
	"golang.org/x/sync/errgroup"
)

type VendingMachineRepo struct {
	database *pgxpool.Pool
}

func NewVendingMachineRepo(database *pgxpool.Pool) *VendingMachineRepo {
	return &VendingMachineRepo{database: database}
}

//go:embed schema.sql
var VendingMachineSchema string

type VendingMachine struct {
	ID                    string    `json:"id"`
	State                 string    `json:"state"`
	LastUpdatedTimestamp  time.Time `json:"lastUpdatedTimestamp"`
	Products              []Product `json:"products,omitempty"`
	LastPurchaseProductID string    `json:"lastPurchaseProductID,omitempty"`
}

const (
	IdleState            = "idle"
	AwaitingPaymentState = "awaiting_payment"
	PaymentTimeoutState  = "payment_timeout"
	DispensingState      = "dispensing"
)

const (
	BuyProductTimeout  = time.Second * 30
	PostPaymentTimeout = time.Second * 15
)

var VendingMachineStates = []string{IdleState, AwaitingPaymentState, PaymentTimeoutState, DispensingState}

var StateMachine = map[string][]string{
	IdleState:            {AwaitingPaymentState},
	AwaitingPaymentState: {DispensingState, PaymentTimeoutState},
	PaymentTimeoutState:  {IdleState},
	DispensingState:      {IdleState},
}

func IsValidStateTransition(prevState string, nextState string) bool {
	if prevState == nextState {
		return true
	}

	nextStates, ok := StateMachine[prevState]
	if !ok {
		return false
	}

	return slices.Contains(nextStates, nextState)
}

func MapVendingMachine(rows pgx.Rows) (VendingMachine, error) {
	var vendingMachineID string
	var rawState string
	var lastUpdatedTimestamp time.Time
	var pendingProductIDPurchase pgtype.Text

	err := rows.Scan(&vendingMachineID, &rawState, &lastUpdatedTimestamp, &pendingProductIDPurchase)

	state := MapVendingMachineStatus(rawState, lastUpdatedTimestamp)
	return VendingMachine{
		ID:                    vendingMachineID,
		State:                 state,
		LastUpdatedTimestamp:  lastUpdatedTimestamp,
		LastPurchaseProductID: pendingProductIDPurchase.String,
	}, err
}

func UnmarshalVendingMachine(rows pgx.Rows) (VendingMachine, error) {
	vendingMachine, err := UnmarshalOneRow(rows, MapVendingMachine)
	if err == pgx.ErrNoRows {
		return VendingMachine{}, VendingMachineNotFoundError
	} else if err != nil {
		return VendingMachine{}, fmt.Errorf("unmarshal vending machine row: %w", err)
	}
	return vendingMachine, nil
}

func MapVendingMachineStatus(rawState string, lastUpdatedTimestamp time.Time) string {
	durationSinceUpdate := time.Since(lastUpdatedTimestamp)

	// perform state "overrides" based on lastUpdatedTimestamp to compute "real" state based on time differences rather than use timers/events
	var vendingMachineState string
	switch rawState {
	case IdleState:
		vendingMachineState = IdleState
	case AwaitingPaymentState:
		if durationSinceUpdate > BuyProductTimeout+PostPaymentTimeout {
			vendingMachineState = IdleState
		} else if durationSinceUpdate > BuyProductTimeout {
			vendingMachineState = PaymentTimeoutState
		} else {
			vendingMachineState = AwaitingPaymentState
		}
	case DispensingState:
		if durationSinceUpdate > PostPaymentTimeout {
			vendingMachineState = IdleState
		} else {
			vendingMachineState = DispensingState
		}
	default:
		vendingMachineState = "unknown"
	}

	return vendingMachineState
}

type Product struct {
	ProductID string  `json:"productId"`
	Name      string  `json:"name"`
	Price     float64 `json:"price"`
	Qty       int64   `json:"qty"`
}

func MapProduct(rows pgx.Rows) (Product, error) {
	var p Product
	err := rows.Scan(&p.ProductID, &p.Name, &p.Price, &p.Qty)
	return p, err
}

func (repo *VendingMachineRepo) InsertVendingMachine(ctx context.Context, vendingMachineID string) (VendingMachine, error) {
	vendingMachine := VendingMachine{
		ID:                   vendingMachineID,
		State:                IdleState,
		LastUpdatedTimestamp: time.Now(),
	}

	// language=SQL
	query := `INSERT INTO vending_machines (vending_machine_id, machine_state, last_updated_timestamp) VALUES ($1, $2, $3)`
	_, err := repo.database.Exec(ctx, query, vendingMachine.ID, vendingMachine.State, vendingMachine.LastUpdatedTimestamp)
	if err != nil {
		return VendingMachine{}, fmt.Errorf("insert vending machine: %w", err)
	}

	return vendingMachine, nil
}

var VendingMachineNotFoundError = errors.New("vending machine is could not be found")

func (repo *VendingMachineRepo) InsertManyVendingMachineProducts(ctx context.Context, vendingMachineID string, products []Product) error {
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

	count, err := repo.database.CopyFrom(
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

func (repo *VendingMachineRepo) getVendingMachineProducts(ctx context.Context, vendingMachineID string) ([]Product, error) {
	// language=SQL
	query := `SELECT p.product_id, p.name, p.price, p.qty
			FROM products p
			INNER JOIN vending_machines v ON v.vending_machine_id = p.vending_machine_id
			WHERE v.vending_machine_id = $1`

	rows, err := repo.database.Query(ctx, query, vendingMachineID)
	if err != nil {
		return nil, fmt.Errorf("select vending machine products: %w", err)
	}
	products, err := UnmarshalRows(rows, MapProduct)
	if err != nil {
		return nil, fmt.Errorf("unmarshal product row: %w", err)
	}
	return products, nil
}

func (repo *VendingMachineRepo) getVendingMachine(ctx context.Context, vendingMachineID string) (VendingMachine, error) {
	// language=SQL
	query := `SELECT vending_machine_id, machine_state, last_updated_timestamp, last_purchase_product_id
			FROM vending_machines
			WHERE vending_machine_id = $1`

	rows, err := repo.database.Query(ctx, query, vendingMachineID)
	if err != nil {
		return VendingMachine{}, fmt.Errorf("select vending machine: %w", err)
	}
	return UnmarshalVendingMachine(rows)
}

func (repo *VendingMachineRepo) GetVendingMachine(ctx context.Context, vendingMachineID string) (VendingMachine, error) {
	eg, egCtx := errgroup.WithContext(ctx)

	var vendingMachine VendingMachine
	var products []Product

	eg.Go(func() (err error) {
		vendingMachine, err = repo.getVendingMachine(egCtx, vendingMachineID)
		return err
	})
	eg.Go(func() (err error) {
		products, err = repo.getVendingMachineProducts(egCtx, vendingMachineID)
		return err
	})

	if err := eg.Wait(); err != nil {
		return VendingMachine{}, err
	}

	vendingMachine.Products = products

	return vendingMachine, nil
}

type StateTransitionError struct {
	PrevState string
	NextState string
}

func (err *StateTransitionError) Error() string {
	if err == nil {
		return "nil state transition error"
	}
	return fmt.Sprintf("state transition from %s to %s is invalid", err.PrevState, err.NextState)
}

type PaymentError struct {
	ExpectedProductID string
	ProductID         string
}

func (err *PaymentError) Error() string {
	if err == nil {
		return "nil payment error"
	}
	return fmt.Sprintf("expected payment %s got %s", err.ExpectedProductID, err.ProductID)
}

func (repo *VendingMachineRepo) updateVendingMachineTx(ctx context.Context, vendingMachineID string, nextState string, productID string) (VendingMachine, error) {
	txn, err := repo.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return VendingMachine{}, err
	}
	defer func() {
		err := txn.Rollback(context.Background())
		slog.InfoContext(ctx, "rolling back update vending machine txn", "error", err)
	}()

	// language=SQL
	query := `SELECT vending_machine_id, machine_state, last_updated_timestamp, last_purchase_product_id
			FROM vending_machines
			WHERE vending_machine_id = $1`

	rows, err := txn.Query(ctx, query, vendingMachineID)
	if err != nil {
		return VendingMachine{}, fmt.Errorf("select vending machine: %w", err)
	}
	vendingMachine, err := UnmarshalVendingMachine(rows)
	if err != nil {
		return VendingMachine{}, err
	}

	slog.InfoContext(ctx, "retrieved state in update vending machine operation", "vendingMachine", vendingMachine)

	if !IsValidStateTransition(vendingMachine.State, nextState) {
		return vendingMachine, &StateTransitionError{PrevState: vendingMachine.State, NextState: nextState}
	}
	if vendingMachine.State == AwaitingPaymentState && vendingMachine.LastPurchaseProductID != productID {
		return vendingMachine, &PaymentError{ExpectedProductID: vendingMachine.LastPurchaseProductID, ProductID: productID}
	}

	// language=SQL
	updateQuery := `UPDATE vending_machines 
			SET machine_state = $2, last_updated_timestamp = $3, last_purchase_product_id = $4
			WHERE vending_machine_id = $1;`
	updateArgs := []any{vendingMachineID, nextState, time.Now(), pgtype.Text{String: productID, Valid: productID != ""}}

	_, err = txn.Exec(ctx, updateQuery, updateArgs...)
	if err != nil {
		return VendingMachine{}, fmt.Errorf("update vending machine state: %w", err)
	}

	if err := txn.Commit(ctx); err != nil {
		return VendingMachine{}, err
	}

	// apply state change on selected previous state
	vendingMachine.State = nextState
	vendingMachine.LastPurchaseProductID = productID

	slog.InfoContext(ctx, "finished update vending machine operation", "vendingMachine", vendingMachine, "updateArgs", updateArgs)
	return vendingMachine, err
}

func (repo *VendingMachineRepo) DoVendingMachineUpdate(ctx context.Context, vendingMachineID string, nextState string, productID string) (VendingMachine, error) {
	slog.InfoContext(ctx, "begin vending machine update", "vendingMachineID", vendingMachineID, "productID", productID, "nextState", nextState)

	vendingMachine, err := repo.updateVendingMachineTx(ctx, vendingMachineID, nextState, productID)
	if err != nil {
		return vendingMachine, err
	}

	// getVendingMachineProducts is done outside a transaction, we're not necessarily expecting it to be consistent in regard to the update, just get the latest products
	products, err := repo.getVendingMachineProducts(ctx, vendingMachineID)
	if err != nil {
		return VendingMachine{}, err
	}
	vendingMachine.Products = products

	slog.InfoContext(ctx, "selected products after update vending machine operation", "vendingMachine", vendingMachine)
	return vendingMachine, nil
}
