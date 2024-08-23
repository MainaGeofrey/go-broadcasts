package main

import (
	"broadcasts/channels"
	"broadcasts/config"
	"broadcasts/messages"
	"broadcasts/messenger"
	"broadcasts/pkg/database/mysql"
	"broadcasts/pkg/logger"
	"broadcasts/pkg/rabbitmq"
	"broadcasts/pkg/redis"
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

func main() {
	// Load environment variables
	config.LoadEnvFile()

	// Initialize the custom logger with a rate limit of 5 seconds
	if err := logger.Init(time.Second * 5); err != nil {
		fmt.Printf("Error initializing logger: %v\n", err)
		os.Exit(1)
	}

	if logger.Logger == nil {
		fmt.Println("Logger is not initialized correctly")
		os.Exit(1)
	}

	// Ensure to stop the logger before application exit to flush any remaining logs to log file
	defer logger.Logger.Stop()

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
	responseQueue := "broadcasts_status_update"
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

	redisHost := config.GetEnv("REDIS_HOST", "localhost")
	redisPort := config.GetEnv("REDIS_PORT", "6379")
	redisPass := config.GetEnv("REDIS_PASS", "")
	redisDB := config.GetEnv("REDIS_DB", "0")

	// Initialize Redis options
	redisOptions, err := redis.NewRedisOptions(redisHost, redisPort, redisPass, redisDB)
	if err != nil {
		logger.Logger.Printf("Error creating Redis options: %v", err)
		os.Exit(1)
	}

	// Initialize Redis ConnectionManager
	redisManager, err := redis.NewConnectionManagerFromOptions(redisOptions, logger.Logger, 5*time.Minute, context.Background())
	if err != nil {
		logger.Logger.Printf("Error initializing Redis connection: %v", err)
		os.Exit(1)
	}
	defer redisManager.Close()

	// Get the Redis client from the manager
	redisClient := redisManager.GetClient()


		testKey := config.GetEnv("SDP_TOKEN_KEY","")
		testValue := "sdp-requires-token"

		// Set a value in Redis
		err = redisClient.Set(context.Background(), testKey, testValue, 10*time.Second).Err()
		if err != nil {
			logger.Logger.Printf("Error setting value in Redis: %v", err)
		} else {
			logger.Logger.Printf("Successfully set value in Redis: %s=%s", testKey, testValue)
		}
/*
		// Get the value from Redis
		value, err := redisClient.Get(context.Background(), testKey).Result()
		if err != nil {
			logger.Logger.Printf("Error getting value from Redis: %v", err)
		} else {
			logger.Logger.Printf("Retrieved value from Redis: %s=%s", testKey, value)
		}
	*/

	channelsFetcher := channels.NewChannelsFetcher(mysql.DB, logger.Logger, redisClient)

	// Fetch channels and cache them in Redis
	status := 1 // Replace with the actual status you want to filter by
	err = channelsFetcher.FetchAndCacheChannels(status)
	if err != nil {
		logger.Logger.Printf("Error fetching and caching channels: %v", err)
	} else {
		logger.Logger.Println("Channels fetched and cached successfully.")
	}

	logger.Logger.Println("Application connections initialized.........[rabbit,redis,rabbit]..........")

	// Create the BroadcastChecker and MessengerService with the RabbitMQ connection and channel

	/*PRODUCER */
	bc, err := messages.BroadcastCheckerProcess(logger.Logger, mysql.DB, channel, broadcastQueue, channelsFetcher)
	if err != nil {
		logger.Logger.Printf("Error creating BroadcastChecker: %v", err)
		os.Exit(1)
	}

	testPhone := config.GetEnv("SMS_TEST_PHONE", "")
	appEnv := config.GetEnv("APP_ENV", "development")
	sdpUserName := config.GetEnv("SDP_USERNAME", "default_username")
	sdpResponseUrl := config.GetEnv("SDP_RESPONSE_URL", "")
	/*CONSUMER */
	ms, err := messenger.NewMessengerService(
		logger.Logger,
		mysql.DB,
		rabbitConn, // Pass RabbitMQ connection here
		broadcastQueue,
		responseQueue,
		testPhone,
		appEnv,
		sdpUserName,
		sdpResponseUrl,
		redisClient,
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
	producerWorkers := 5
	consumerWorkers := 5

	// WaitGroup to wait for all workers to finish
	var wg sync.WaitGroup
	wg.Add(fetchWorkers + producerWorkers + consumerWorkers)

	// Start fetch workers
	for i := 0; i < fetchWorkers; i++ {
		go bc.Run(ctx, &wg)
	}

	for i := 0; i < fetchWorkers; i++ {
		go bc.ProcessBroadcasts(ctx, &wg)
	}

	// Start process workers
	for i := 0; i < consumerWorkers; i++ {
		go ms.ConsumeMessages(ctx, &wg) // Pass the WaitGroup
	}

	// Start status update consumer
	//go ms.ConsumeStatusUpdates(ctx)

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(done)
	}()

	// Handle graceful shutdown
	<-done
	logger.Logger.Println("Application finished.")
}
