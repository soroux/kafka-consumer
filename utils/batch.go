package utils

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
)

func ReadBatchMessages(ctx context.Context, reader *kafka.Reader, batchSize int) []kafka.Message {
	messages := make([]kafka.Message, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if err == context.Canceled {
				return messages
			}
			log.Printf("Error reading message: %v", err)
			continue
		}
		messages = append(messages, msg)
	}
	return messages
}
