package main

import (
	"context"
	"kafka-consumer/config"
	"kafka-consumer/kafka"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down gracefully...")
		cancel()
	}()

	// Add consumer groups to the wait group
	groups := []string{config.TransactionDailyGroupID, config.TransactionUserDailyGroupID}
	for _, groupID := range groups {
		wg.Add(1)
		go func(gid string) {
			defer wg.Done()
			kafka.StartConsumerGroup(ctx, gid)
		}(groupID)
	}

	wg.Wait()
}
