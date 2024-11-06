package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"kafka-consumer/config"
	"kafka-consumer/db"
	"kafka-consumer/utils"
	"log"
	"sync"
)

var mu sync.Mutex // mutex to prevent race conditions in database access

func StartConsumerGroup(ctx context.Context, groupID string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{config.KafkaBroker},
		Topic:   config.Topic,
		GroupID: groupID,
	})
	defer reader.Close()

	numWorkers := 4
	var workerWG sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			consumeMessages(ctx, reader, groupID)
		}()
	}
	workerWG.Wait()
}

func consumeMessages(ctx context.Context, reader *kafka.Reader, groupID string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			messages := utils.ReadBatchMessages(ctx, reader, config.BatchSize)
			if len(messages) > 0 {
				transactions := utils.ParseBatchTransactionPayload(messages)
				err := utils.ProcessWithRetry(transactions, db.GetProcessFunc(groupID))
				if err != nil {
					log.Printf("Processing failed, sending to DLQ: %v", err)
					SendToDLQ(messages, groupID)
				}
			}
		}
	}
}
