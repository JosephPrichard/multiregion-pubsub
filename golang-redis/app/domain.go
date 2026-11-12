package app

import (
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type VendingMachine struct {
	ID                    string              `json:"id"`
	State                 VendingMachineState `json:"state"`
	LastUpdatedTimestamp  time.Time           `json:"lastUpdatedTimestamp"`
	Products              []Product           `json:"products,omitempty"`
	LastPurchaseProductID string              `json:"lastPurchaseProductId,omitempty"`
	LastPurchasedProduct  *Product            `json:"lastPurchasedProduct,omitempty"`
}

func (m *VendingMachine) SetProducts(products []Product) {
	m.Products = products
	for _, p := range m.Products {
		if p.ProductID == m.LastPurchaseProductID {
			// &p is reference to a copy of the data stored in the array, not the element itself
			m.LastPurchasedProduct = &p
		}
	}
}

type VendingMachineState string

const (
	DefaultState         VendingMachineState = ""
	IdleState            VendingMachineState = "idle"
	AwaitingPaymentState VendingMachineState = "awaiting_payment"
	PaymentTimeoutState  VendingMachineState = "payment_timeout"
	DispensingState      VendingMachineState = "dispensing"
	OrderCompletedState  VendingMachineState = "order_completed"
)

const (
	BuyProductTimeout    = time.Second * 10
	AfterPaymentTimeout  = time.Second * 5
	OrderCompleteTimeout = time.Second * 5
)

type NextScheduledState struct {
	NextState VendingMachineState
	Timeout   time.Duration
}

func (state NextScheduledState) GetScheduledAt() time.Time {
	return time.Now().Add(state.Timeout)
}

var VendingMachineStates = []VendingMachineState{IdleState, AwaitingPaymentState, PaymentTimeoutState, DispensingState, OrderCompletedState}

var StateMachine = map[VendingMachineState][]VendingMachineState{
	IdleState:            {AwaitingPaymentState},
	AwaitingPaymentState: {DispensingState, PaymentTimeoutState},
	PaymentTimeoutState:  {IdleState},
	DispensingState:      {OrderCompletedState},
	OrderCompletedState:  {IdleState},
}

var StateMachineScheduleMap = map[VendingMachineState]NextScheduledState{
	AwaitingPaymentState: {PaymentTimeoutState, BuyProductTimeout},
	PaymentTimeoutState:  {IdleState, AfterPaymentTimeout},
	DispensingState:      {OrderCompletedState, AfterPaymentTimeout},
	OrderCompletedState:  {IdleState, OrderCompleteTimeout},
}

func IsValidStateTransition(prevState VendingMachineState, nextState VendingMachineState) bool {
	if prevState == nextState {
		return true
	}
	nextStates, ok := StateMachine[prevState]
	if !ok {
		return false
	}
	return slices.Contains(nextStates, nextState)
}

func GetNextScheduledState(prevState VendingMachineState) (NextScheduledState, bool) {
	nextScheduledState, ok := StateMachineScheduleMap[prevState]
	return nextScheduledState, ok
}

func MapVendingMachine(rows pgx.Rows) (VendingMachine, error) {
	var vendingMachineID string
	var state VendingMachineState
	var lastUpdatedTimestamp time.Time
	var pendingProductIDPurchase pgtype.Text

	err := rows.Scan(&vendingMachineID, &state, &lastUpdatedTimestamp, &pendingProductIDPurchase)

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

type VendingMachineUpdate struct {
	VendingMachineID string              `json:"vendingMachineID"`
	NextState        VendingMachineState `json:"nextState"`
	ProductID        string              `json:"productID"`
	ProductChangeSet ProductChangeSet    `json:"productChangeSet"`
}

type ProductChangeSet struct {
	Quantity int
}

func TransformUpdate(currState VendingMachine, updt VendingMachineUpdate) VendingMachineUpdate {
	// we need to overwrite the productID only when the nextState is idle, otherwise we can just show the last product
	if currState.State == IdleState {
		updt.ProductID = ""
	}

	// retain existing productID for cases where it won't come as input
	if currState.State == OrderCompletedState || currState.State == PaymentTimeoutState {
		updt.ProductID = currState.LastPurchaseProductID
	}

	// drop product quantity of purchased product (when next state will be post-purchase state)
	if updt.NextState == DispensingState {
		updt.ProductChangeSet.Quantity = -1
	}

	return updt
}

type StateTransitionError struct {
	PrevState VendingMachineState
	NextState VendingMachineState
}

func (err *StateTransitionError) Error() string {
	if err == nil {
		return "nil state transition error"
	}
	return fmt.Sprintf("State transition from %s to %s is invalid", err.PrevState, err.NextState)
}

type PaymentError struct {
	ExpectedProductID string
	ProductID         string
}

func (err *PaymentError) Error() string {
	if err == nil {
		return "nil payment error"
	}
	return fmt.Sprintf("Expected payment %s got %s", err.ExpectedProductID, err.ProductID)
}

func DoStateTransitionValidations(vendingMachine VendingMachine, nextState VendingMachineState, productID string) error {
	isValidTransition := IsValidStateTransition(vendingMachine.State, nextState)
	if !isValidTransition {
		return &StateTransitionError{PrevState: vendingMachine.State, NextState: nextState}
	}

	isPaymentAction := vendingMachine.State == AwaitingPaymentState && nextState == OrderCompletedState
	// AwaitingPaymentState->: we need to validate the productID being set is equal to the one already in the database
	if isPaymentAction && vendingMachine.LastPurchaseProductID != productID {
		return &PaymentError{ExpectedProductID: vendingMachine.LastPurchaseProductID, ProductID: productID}
	}

	return nil
}

type PostConditionErrorKind int

const (
	PurchaseQtyKind PostConditionErrorKind = 0
)

type UpdatePostConditionError struct {
	Kind      PostConditionErrorKind
	ProductID string
}

func (err *UpdatePostConditionError) Error() string {
	if err == nil {
		return "nil update post condition error"
	}
	switch err.Kind {
	case PurchaseQtyKind:
		return fmt.Sprintf("PostCondition: not enough product in stock for productID=%s", err.ProductID)
	default:
		return "unknown update post condition error"
	}
}

func DoPostUpdateValidations(vendingMachine VendingMachine, product Product) error {
	// client is supposed to check these before they call, but this prevents invalid state
	if product.Qty < 0 {
		return &UpdatePostConditionError{ProductID: product.ProductID}
	}
	return nil
}
