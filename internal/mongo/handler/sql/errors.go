package sql

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

func IsUndefinedColumn(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	// PostgreSQL: 42703 = undefined_column
	return pgErr.Code == "42703"
}

func IsUndefinedRelation(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	// PostgreSQL: 42P01 = undefined_table
	return pgErr.Code == "42P01"
}

func IsUndefinedSchema(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	// PostgreSQL: 3F000 = invalid_schema_name
	return pgErr.Code == "3F000"
}

func IsDuplicateColumn(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	// PostgreSQL: 42701 = duplicate_column
	return pgErr.Code == "42701"
}

func IsDuplicateObject(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	// PostgreSQL: 42710 = duplicate_object, 42P07 = duplicate_table (often used for indexes/relations too)
	return pgErr.Code == "42710" || pgErr.Code == "42P07"
}

func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// PostgreSQL: 23505 = unique_violation
		if pgErr.Code == "23505" {
			return true
		}
		// MonkDB backends sometimes use custom messages.
		msg := strings.ToLower(pgErr.Message)
		if strings.Contains(msg, "duplicate key") || strings.Contains(msg, "unique") {
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key") || strings.Contains(msg, "unique constraint") || strings.Contains(msg, "unique violation")
}

func IsDuplicateColumnName(err error) bool {
	errStr := ""
	if pgErr, ok := err.(*pgconn.PgError); ok {
		errStr = pgErr.Message
	} else {
		errStr = err.Error()
	}
	if strings.Contains(errStr, "already has a column named") {
		return true
	}
	return IsDuplicateColumn(err)
}
