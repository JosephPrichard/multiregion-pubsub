package main

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func UnmarshalRows[T any](rows pgx.Rows, mapFn func(pgx.Rows) (T, error)) ([]T, error) {
	defer rows.Close()
	var objects []T
	for rows.Next() {
		object, err := mapFn(rows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	return objects, nil
}

func UnmarshalOneRow[T any](rows pgx.Rows, mapFn func(pgx.Rows) (T, error)) (object T, err error) {
	objects, err := UnmarshalRows(rows, mapFn)
	if err != nil {
		return object, err
	}
	if len(objects) == 1 {
		return objects[0], nil
	}
	return object, pgx.ErrNoRows
}

const (
	ErrPgUniqueViolation      = "23505"
	ErrPgForeignKeyViolation  = "23503"
	ErrPgCheckViolation       = "23506"
	ErrPgSerializationFailure = "40001"
	ErrPgDeadlock             = "40P01"
)

var UniqueViolationErr = errors.New("unique constraint violation")
var ForeignKeyViolationErr = errors.New("foreign key violation")

func databaseInsertError(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return nil
	}
	switch pgErr.Code {
	case ErrPgUniqueViolation:
		return UniqueViolationErr
	case ErrPgForeignKeyViolation, ErrPgCheckViolation:
		return ForeignKeyViolationErr
	default:
		return nil
	}
}
