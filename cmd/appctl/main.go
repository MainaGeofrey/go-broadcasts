package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"broadcasts/config"
	"broadcasts/messages"
	"broadcasts/messenger"
	"broadcasts/pkg/database/mysql"
	"broadcasts/pkg/logger"
	"broadcasts/pkg/rabbitmq"
)

func main() {
	// Load environment variables
	config.LoadEnvFile()

	// Initialize the custom logger
	if err := logger.Init(); err != nil {
		fmt.Printf("Error initializing logger: %v\n", err)
		os.Exit(1)
	}

	if logger.Logger == nil {
		fmt.Println("Logger is not initialized correctly")
		os.Exit(1)
	}

	// Initialize the database connection
	if err := mysql.Init(); err != nil {
		logger.Logger.Printf("Error initializing database: %v", err)
		os.Exit(1)
	}
	defer mysql.Close()

	// Extract RabbitMQ configuration from environment variables
	rabbitUser := config.GetEnv("RABBIT_USER", "guest")
	rabbitPass := config.GetEnv("RABBIT_PASS", "guest")
	rabbitHost := config.GetEnv("RABBIT_HOST", "localhost")
	rabbitPort := config.GetEnv("RABBIT_PORT", "5672")

	// Build RabbitMQ URL
	rabbitmqUrl := fmt.Sprintf("amqp://%s:%s@%s:%s/", rabbitUser, rabbitPass, rabbitHost, rabbitPort)

	// Initialize RabbitMQ connection manager
	rabbitManager, err := rabbitmq.NewConnectionManager(rabbitmqUrl, logger.Logger, 5*time.Minute, context.Background())
	if err != nil {
		logger.Logger.Printf("Error initializing RabbitMQ connection: %v", err)
		os.Exit(1)
	}
	defer rabbitManager.Close()

	// Create RabbitMQ connection
	rabbitConn := rabbitManager.GetConnection()

	// Create RabbitMQ channel
	channel, err := rabbitConn.Channel()
	if err != nil {
		logger.Logger.Printf("Failed to create RabbitMQ channel: %v", err)
		os.Exit(1)
	}
	defer channel.Close()

	// Declare necessary RabbitMQ queues
	broadcastQueue := "broadcasts"
	responseQueue := "broadcasts_responses"
	queues := []string{broadcastQueue, responseQueue}

	for _, queue := range queues {
		_, err := channel.QueueDeclare(
			queue, // Queue name
			true,  // Durable
			false, // Delete when unused
			false, // Exclusive
			false, // No-wait
			nil,   // Arguments
		)
		if err != nil {
			logger.Logger.Printf("Failed to declare RabbitMQ queue %s: %v", queue, err)
			os.Exit(1)
		}
	}

	// Create the BroadcastChecker and MessengerService with the RabbitMQ connection and channel
	bc, err := messages.NewBroadcastChecker(logger.Logger, mysql.DB, channel, broadcastQueue)
	if err != nil {
		logger.Logger.Printf("Error creating BroadcastChecker: %v", err)
		os.Exit(1)
	}

	testPhone := config.GetEnv("SMS_TEST_PHONE", "")
	appEnv := config.GetEnv("APP_ENV", "development")

	ms, err := messenger.NewMessengerService(
		logger.Logger,
		mysql.DB,
		rabbitConn, // Pass RabbitMQ connection here
		broadcastQueue,
		responseQueue,
		testPhone,
		appEnv,
	)
	if err != nil {
		logger.Logger.Printf("Error creating MessengerService: %v", err)
		os.Exit(1)
	}

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to signal completion
	done := make(chan bool)

	// Worker pool sizes
	fetchWorkers := 1
	processWorkers := 5

	// WaitGroup to wait for all workers to finish
	var wg sync.WaitGroup
	wg.Add(fetchWorkers + processWorkers)

	// Start fetch workers
	for i := 0; i < fetchWorkers; i++ {
		go bc.Run(ctx, &wg)
	}

	for i := 0; i < fetchWorkers; i++ {
	go bc.ProcessBroadcasts(ctx, &wg)
	}

	// Start process workers
	for i := 0; i < processWorkers; i++ {
		go ms.ConsumeMessages(ctx)
	}

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(done)
	}()

	// Handle graceful shutdown
	<-done
	logger.Logger.Println("Application finished.")
}
