package utils

import (
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"kafka-consumer/models"
	"log"
)

func ParseBatchTransactionPayload(messages []kafka.Message) []models.Transaction {
	var transactions []models.Transaction
	for _, msg := range messages {
		var transaction models.Transaction
		if err := json.Unmarshal(msg.Value, &transaction); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			continue
		}
		transactions = append(transactions, transaction)
	}
	return transactions
}
