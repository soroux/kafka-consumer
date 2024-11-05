package db

import (
	"database/sql"
	"fmt"
	"kafka-consumer/config"
)

func OpenDBConnection() (*sql.DB, error) {
	db, err := sql.Open("postgres", config.PgConnStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %w", err)
	}
	return db, nil
}
