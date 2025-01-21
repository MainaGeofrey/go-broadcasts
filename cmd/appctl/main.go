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

	rabbitConn := rabbitManager.GetConnection()

	channel, err := rabbitConn.Channel()
	if err != nil {
		logger.Logger.Printf("Failed to create RabbitMQ channel: %v", err)
		os.Exit(1)
	}
	defer channel.Close()

	queues := []string{"broadcasts", "broadcasts_status_update"}
	for _, queue := range queues {
		_, err := channel.QueueDeclare(queue, true, false, false, false, nil)
		if err != nil {
			logger.Logger.Printf("Failed to declare RabbitMQ queue %s: %v", queue, err)
			os.Exit(1)
		}
	}

	redisOptions, err := redis.NewRedisOptions(
		config.GetEnv("REDIS_HOST", "localhost"),
		config.GetEnv("REDIS_PORT", "6379"),
		config.GetEnv("REDIS_PASS", ""),
		config.GetEnv("REDIS_DB", "0"),
	)
	if err != nil {
		logger.Logger.Printf("Error creating Redis options: %v", err)
		os.Exit(1)
	}

	redisManager, err := redis.NewConnectionManagerFromOptions(redisOptions, logger.Logger, 5*time.Minute, context.Background())
	if err != nil {
		logger.Logger.Printf("Error initializing Redis connection: %v", err)
		os.Exit(1)
	}
	defer redisManager.Close()

	redisClient := redisManager.GetClient()

	channelsFetcher := channels.NewChannelsFetcher(mysql.DB, logger.Logger, redisClient)

	err = channelsFetcher.FetchAndCacheChannels(1)
	if err != nil {
		logger.Logger.Printf("Error fetching and caching channels: %v", err)
	} else {
		logger.Logger.Println("Channels fetched and cached successfully.")
	}

	logger.Logger.Println("Application connections initialized.........[rabbit, redis, rabbit]..........")

	bc, err := messages.BroadcastCheckerProcess(logger.Logger, mysql.DB, channel, "broadcasts", channelsFetcher)
	if err != nil {
		logger.Logger.Printf("Error creating BroadcastChecker: %v", err)
		os.Exit(1)
	}

	ms, err := messenger.NewMessengerService(
		logger.Logger,
		mysql.DB,
		rabbitConn,
		"broadcasts",
		"broadcasts_status_update",
		config.GetEnv("SMS_TEST_PHONE", ""),
		config.GetEnv("APP_ENV", "development"),
		config.GetEnv("SDP_USERNAME", "default_username"),
		config.GetEnv("SDP_RESPONSE_URL", ""),
		redisClient,
	)
	if err != nil {
		logger.Logger.Printf("Error creating MessengerService: %v", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a WaitGroup for worker synchronization
	var workerDone sync.WaitGroup

	// Start fetch and process workers
	// Add two workers to the WaitGroup
	workerDone.Add(2)

	go func() {
		defer workerDone.Done()
		bc.Run(ctx, &workerDone)
	}()

	go func() {
		defer workerDone.Done()
		ms.ConsumeMessages(ctx, &workerDone)
	}()

	// Wait for workers to finish
	workerDone.Wait()
	logger.Logger.Println("Application finished.")
}
