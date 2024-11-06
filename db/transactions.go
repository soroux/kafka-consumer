package db

import (
	"fmt"
	"kafka-consumer/config"
	"kafka-consumer/models"
	"log"
	"strconv"
	"sync"
	"time"
)

func getLockForKey(key string) *sync.Mutex {
	lock, _ := keyLocks.LoadOrStore(key, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

var keyLocks sync.Map // key: string, value: *sync.Mutex

// Process a batch of transactions to create or update the transaction_daily table
func UpdateTransactionDaily(transactions []models.Transaction) error {
	db, err := OpenDBConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Prepare statement for updating or inserting records
	stmtUpdate, err := tx.Prepare(`UPDATE transactions_daily SET amount = amount + $1 WHERE date = $2`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer stmtUpdate.Close()

	stmtInsert, err := tx.Prepare(`INSERT INTO transactions_daily (date, amount) VALUES ($1, $2)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmtInsert.Close()

	for _, transaction := range transactions {
		// Parse the created_at string to get just the date
		date, err := time.Parse(time.RFC3339, transaction.CreatedAt)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to parse created_at: %w", err)
		}

		// Format date to "YYYY-MM-DD"
		dateString := date.Format("2006-01-02")
		amount := transaction.Amount
		if transaction.Type == "withdraw" {
			amount = -amount
		}

		keyLock := getLockForKey(dateString)
		keyLock.Lock()

		// First, check if the record exists
		var exists bool
		err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM transactions_daily WHERE date = $1)`, dateString).Scan(&exists)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to check existence: %w", err)
		}

		if exists {
			// Update the existing record
			_, err = stmtUpdate.Exec(transaction.Amount, dateString)
		} else {
			// Insert a new record
			_, err = stmtInsert.Exec(dateString, transaction.Amount)
		}
		keyLock.Unlock()

		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert or update transaction_daily: %w", err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Successfully updated or created entries in transaction_daily")
	return nil
}

// Process a batch of transactions to create or update the transaction_user_daily table
func UpdateTransactionUserDaily(transactions []models.Transaction) error {
	db, err := OpenDBConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Prepare statement for updating or inserting records
	stmtUpdate, err := tx.Prepare(`UPDATE transactions_user_daily SET amount = amount + $1 WHERE user_id = $2 AND date = $3`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer stmtUpdate.Close()

	stmtInsert, err := tx.Prepare(`INSERT INTO transactions_user_daily (user_id, date, amount) VALUES ($1, $2, $3)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmtInsert.Close()

	for _, transaction := range transactions {
		// Parse the created_at string to get just the date
		date, err := time.Parse(time.RFC3339, transaction.CreatedAt)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to parse created_at: %w", err)
		}

		// Format date to "YYYY-MM-DD"
		dateString := date.Format("2006-01-02")
		amount := transaction.Amount
		if transaction.Type == "withdraw" {
			amount = -amount
		}
		keyLock := getLockForKey(dateString + "=" + strconv.Itoa(transaction.UserID))
		keyLock.Lock()

		// Check if the record exists
		var exists bool
		err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM transactions_user_daily WHERE user_id = $1 AND date = $2)`, transaction.UserID, dateString).Scan(&exists)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to check existence: %w", err)
		}

		if exists {
			// Update the existing record
			_, err = stmtUpdate.Exec(amount, transaction.UserID, dateString)
		} else {
			// Insert a new record
			_, err = stmtInsert.Exec(transaction.UserID, dateString, amount)
		}
		keyLock.Unlock()

		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert or update transaction_user_daily: %w", err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Successfully updated or created entries in transaction_user_daily")
	return nil
}

// Choose the function based on consumer group
func GetProcessFunc(groupID string) func([]models.Transaction) error {
	switch groupID {
	case config.TransactionDailyGroupID:
		return UpdateTransactionDaily
	case config.TransactionUserDailyGroupID:
		return UpdateTransactionUserDaily
	default:
		return nil
	}
}
