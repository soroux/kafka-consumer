package utils

import (
	"github.com/cenkalti/backoff/v4"
	"kafka-consumer/config"
	"kafka-consumer/models"
)

func ProcessWithRetry(transactions []models.Transaction, processFunc func([]models.Transaction) error) error {
	retryPolicy := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), config.MaxRetries)
	return backoff.Retry(func() error {
		return processFunc(transactions)
	}, retryPolicy)
}
