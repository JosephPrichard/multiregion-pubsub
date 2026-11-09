package main

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

type VendingMachineRepo struct {
	database *pgxpool.Pool
}

func NewVendingMachineRepo(database *pgxpool.Pool) VendingMachineRepo {
	return VendingMachineRepo{database: database}
}

var VendingMachineSchema = `
	CREATE TABLE vending_machines (
		vending_machine_id     TEXT PRIMARY KEY,
		machine_state          TEXT NOT NULL,
		last_updated_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	
	CREATE TABLE products (
		product_id         TEXT PRIMARY KEY,
		vending_machine_id TEXT NOT NULL,
		name               TEXT NOT NULL,
		price              NUMERIC NOT NULL,
		qty                INTEGER NOT NULL,
		FOREIGN KEY (vending_machine_id) REFERENCES vending_machines (vending_machine_id)
	);
`

type VendingMachine struct {
	ID                   string    `json:"id"`
	State                string    `json:"state"`
	LastUpdatedTimestamp time.Time `json:"lastUpdatedTimestamp"`
	Products             []Product `json:"products"`
}

const (
	IdleState            = "idle"
	AwaitingPaymentState = "awaiting_payment"
	DispensingState      = "dispensing_state"
	OrderCompleteState   = "order_complete_state"
)

var VendingMachineStates = []string{IdleState, AwaitingPaymentState, DispensingState, OrderCompleteState}

func MapVendingMachine(rows pgx.Rows) (VendingMachine, error) {
	var vendingMachineID string
	var rawState string
	var lastUpdatedTimestamp time.Time

	err := rows.Scan(&vendingMachineID, &rawState, &lastUpdatedTimestamp)

	state := MapVendingMachineStatus(rawState, lastUpdatedTimestamp)
	return VendingMachine{ID: vendingMachineID, State: state, LastUpdatedTimestamp: lastUpdatedTimestamp}, err
}

func MapVendingMachineStatus(rawState string, lastUpdatedTimestamp time.Time) string {
	timeElapsedSinceStateChange := time.Since(lastUpdatedTimestamp)
	// perform state "overrides" based on lastUpdatedTimestamp to compute "real" state based delays
	var vendingMachineState string
	switch rawState {
	case IdleState:
		vendingMachineState = IdleState
	case AwaitingPaymentState:
		vendingMachineState = AwaitingPaymentState
	case DispensingState:
		if timeElapsedSinceStateChange > time.Second*30 {
			vendingMachineState = IdleState
		} else if timeElapsedSinceStateChange > time.Second*15 {
			vendingMachineState = OrderCompleteState
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

func (r *VendingMachineRepo) InsertVendingMachine(ctx context.Context, vendingMachineID string) (VendingMachine, error) {
	rows, err := r.database.Query(ctx,
		"INSERT INTO vending_machines (vending_machine_id, state, last_updated_timestamp) ($1, $2, $3) RETURNING vending_machine_id, machine_state, last_updated_timestamp",
		vendingMachineID,
		IdleState,
		time.Now(),
	)
	if err != nil {
		slog.ErrorContext(ctx, "failed to insert vending machine", "error", err)
		return VendingMachine{}, err
	}
	vendingMachine, err := UnmarshalOneRow(rows, MapVendingMachine)
	if err != nil {
		slog.ErrorContext(ctx, "failed to unmarshal vending machine rows", "error", err)
		return VendingMachine{}, err
	}
	return vendingMachine, nil
}

var VendingMachineNotFoundError = errors.New("vending machine is invalid")

func (r *VendingMachineRepo) InsertManyVendingMachineProducts(ctx context.Context, vendingMachineID string, products []Product) error {
	var productCopyFromRows [][]any
	for _, product := range products {
		productCopyFromRows = append(productCopyFromRows, []any{
			product.ProductID,
			vendingMachineID,
			product.Name,
			product.Price,
			product.Qty,
		})
	}

	n, err := r.database.CopyFrom(
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

	slog.InfoContext(ctx, "restocked vending machine", "count", n, "productCopyFromRows", productCopyFromRows)
	return nil
}

func (r *VendingMachineRepo) GetVendingMachine(ctx context.Context, vendingMachineID string) (VendingMachine, error) {
	eg, egCtx := errgroup.WithContext(ctx)

	var vendingMachine VendingMachine
	var products []Product

	eg.Go(func() (err error) {
		rows, err := r.database.Query(egCtx,
			`SELECT vending_machine_id, machine_state, last_updated_timestamp, 
			FROM vending_machines
			WHERE vending_machine_id = $1`,
			vendingMachineID,
		)
		if err != nil {
			slog.ErrorContext(egCtx, "failed to select vending machine", "error", err)
			return err
		}
		vendingMachine, err = UnmarshalOneRow(rows, MapVendingMachine)
		if err == pgx.ErrNoRows {
			return VendingMachineNotFoundError
		}
		if err != nil {
			slog.ErrorContext(egCtx, "failed to unmarshal vending machine row", "error", err)
			return err
		}
		return nil
	})
	eg.Go(func() (err error) {
		rows, err := r.database.Query(egCtx,
			`SELECT p.product_id, p.name, p.price, p.qty
			FROM products p
			INNER JOIN vending_machines v ON v.vending_machine_id = p.vending_machine_id
			WHERE v.vending_machine_id = $1`,
			vendingMachineID,
		)
		if err != nil {
			slog.ErrorContext(egCtx, "failed to select vending machine products", "error", err)
			return err
		}
		products, err = UnmarshalRows(rows, MapProduct)
		if err != nil {
			slog.ErrorContext(egCtx, "failed to unmarshal vending machine product rows", "error", err)
			return err
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return VendingMachine{}, err
	}

	vendingMachine.Products = products

	return vendingMachine, nil
}

var VendingMachinStateTransitionInvalid = errors.New("vending machine state transition is invalid")
var VendingMachinStateInvalid = errors.New("vending machine state is invalid")

type UpdateMachineResult struct {
	ID                   string    `json:"id"`
	PrevState            string    `json:"state"`
	NextState            string    `json:"-"`
	LastUpdatedTimestamp time.Time `json:"lastUpdatedTimestamp"`
	IsTransitionValid    bool      `json:"-"`
}

func mapTryUpdateVendingMachineResult(rows pgx.Rows) (UpdateMachineResult, error) {
	var vendingMachineID string
	var rawPrevState string
	var rawNextState string
	var lastUpdatedTimestamp time.Time

	err := rows.Scan(&vendingMachineID, &rawPrevState, &rawNextState, &lastUpdatedTimestamp)

	prevState := MapVendingMachineStatus(rawPrevState, lastUpdatedTimestamp)
	nextState := MapVendingMachineStatus(rawNextState, lastUpdatedTimestamp)

	return UpdateMachineResult{
		ID:                   vendingMachineID,
		PrevState:            prevState,
		NextState:            nextState,
		LastUpdatedTimestamp: lastUpdatedTimestamp,
		IsTransitionValid:    true,
	}, err
}

func (r *VendingMachineRepo) UpdateVendingMachine(ctx context.Context, vendingMachineID string, state string) (u UpdateMachineResult, err error) {
	slog.InfoContext(ctx, "begin update vending machine operation", "vendingMachineID", vendingMachineID, "state", state)

	if !slices.Contains(VendingMachineStates, state) {
		return UpdateMachineResult{}, VendingMachinStateInvalid
	}

	// language=SQL
	query := `
		WITH previous_state AS (
			SELECT vending_machine_id, machine_state, last_updated_timestamp
			FROM vending_machines
			WHERE vending_machine_id = $1
		),
		next_state AS (
			UPDATE vending_machines 
			WHERE vending_machine_id = $1
			SET machine_state = $2 
			RETURNING vending_machine_id, machine_state, last_updated_timestamp
		)
		SELECT p.vending_machine_id, p.machine_state as next_state. n.machine_state as prev_state, p.last_updated_timestamp
		FROM previous_state p 
		INNER JOIN next_state n ON n.vending_machine_id = p.vending_machine_id`

	rows, err := r.database.Query(ctx, query, vendingMachineID, state)
	if err != nil {
		slog.ErrorContext(ctx, "failed to update vending machine state", "error", err)
		return u, err
	}

	result, err := UnmarshalOneRow(rows, mapTryUpdateVendingMachineResult)
	if err != nil {
		if err == pgx.ErrNoRows {
			return u, VendingMachineNotFoundError
		}
		return u, err
	}

	slog.InfoContext(ctx, "finished update vending machine operation", "result", result)

	if !result.IsTransitionValid {
		return result, VendingMachinStateTransitionInvalid
	}

	return result, nil
}
