package kafka

import (
	"context"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"kafka-consumer/config"
	"log"
)

func SendToDLQ(messages []kafka.Message, groupID string) {
	dlqWriter := kafka.Writer{
		Addr:  kafka.TCP(config.KafkaBroker),
		Topic: groupID + "_dlq",
	}
	defer dlqWriter.Close()

	for _, msg := range messages {
		dlqMessage := kafka.Message{
			Key:   msg.Key,   // You might want to keep the same key
			Value: msg.Value, // The original message
		}
		if err := dlqWriter.WriteMessages(context.Background(), dlqMessage); err != nil {
			log.Printf("Failed to send message to DLQ: %v", err)
		}
	}
}
