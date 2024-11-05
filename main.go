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
		cancel()
	}()

	wg.Add(2)

	go func() {
		defer wg.Done()
		kafka.StartConsumerGroup(ctx, config.TransactionDailyGroupID)
	}()

	go func() {
		defer wg.Done()
		kafka.StartConsumerGroup(ctx, config.TransactionUserDailyGroupID)
	}()

	wg.Wait()
}

func handleShutdown(cancel context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down gracefully...")
	cancel()
}
