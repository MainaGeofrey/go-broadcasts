package redis

import (
	"context"
	"fmt"
	"time"
	"broadcasts/pkg/logger"
	"github.com/redis/go-redis/v9"
	"strconv"
)

// Default connection duration
const defaultConnectionDuration = 5 * time.Minute


type ConnectionManager struct {
	options         *redis.Options
	logger          *logger.CustomLogger
	client          *redis.Client
	duration        time.Duration
	activityChannel chan struct{}
	closeChannel    chan struct{}
}


func NewConnectionManager(options *redis.Options, logger *logger.CustomLogger, duration time.Duration, ctx context.Context) (*ConnectionManager, error) {
	if duration <= 0 {
		duration = defaultConnectionDuration
	}

	cm := &ConnectionManager{
		options:         options,
		logger:          logger,
		duration:        duration,
		activityChannel: make(chan struct{}, 1),
		closeChannel:    make(chan struct{}, 1),
	}

	if err := cm.createConnection(ctx); err != nil {
		return nil, err
	}

	go cm.keepConnectionAlive(ctx)

	return cm, nil
}

// createConnection establishes a new Redis connection with retry logic
func (cm *ConnectionManager) createConnection(ctx context.Context) error {
	var err error 
	for i := 0; i < 3; i++ { // Retry up to 3 times
		client := redis.NewClient(cm.options)
		if err = client.Ping(ctx).Err(); err == nil {
			cm.client = client
			cm.logger.Printf("Connection established to Redis at %s:%d", cm.options.Addr, cm.options.DB)
			return nil
		}
		cm.logger.Printf("Failed to connect to Redis at %s:%d (attempt %d): %v", cm.options.Addr, cm.options.DB, i+1, err)
		time.Sleep(2 * time.Second) // Wait before retrying
	}
	return fmt.Errorf("could not connect to Redis after multiple attempts: %w", err)
}

// keepConnectionAlive maintains the connection and checks its health
func (cm *ConnectionManager) keepConnectionAlive(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second) // Check connection every 10 seconds
	defer ticker.Stop()

	lastActivity := time.Now()

	for {
		select {
		case <-ctx.Done():
			cm.Close()
			return
		case <-cm.closeChannel:
			cm.Close()
			return
		case <-ticker.C:
			if time.Since(lastActivity) > cm.duration {
				cm.logger.Println("Connection idle for too long, closing connection...")
				cm.Close()
			} else if !cm.IsHealthy(ctx) {
				cm.logger.Println("Connection lost, attempting to reconnect...")
				if err := cm.createConnection(ctx); err != nil {
					cm.logger.Printf("Failed to reconnect: %v", err)
				}
			}
		}
		// Update last activity time when there is activity
		select {
		case <-cm.activityChannel:
			lastActivity = time.Now()
		default:
			// No activity; proceed to next tick
		}
	}
}

// NotifyActivity should be called to indicate activity on the connection
func (cm *ConnectionManager) NotifyActivity() {
	cm.activityChannel <- struct{}{}
}

// Close closes the Redis connection
func (cm *ConnectionManager) Close() {
	if cm.client != nil {
		cm.client.Close()
		cm.logger.Println("Connection closed")
	}
}

// GetClient returns the active Redis client
func (cm *ConnectionManager) GetClient() *redis.Client {
	return cm.client
}

// IsHealthy checks if the Redis connection is healthy
func (cm *ConnectionManager) IsHealthy(ctx context.Context) bool {
	if cm.client == nil {
		return false
	}
	return cm.client.Ping(ctx).Err() == nil
}

// NewRedisOptions constructs redis.Options from the provided configuration
func NewRedisOptions(host, port, password, db string) (*redis.Options, error) {
	redisDB, err := strconv.Atoi(db)
	if err != nil {
		return nil, fmt.Errorf("error converting Redis DB to integer: %w", err)
	}

	options := &redis.Options{
		Addr:     fmt.Sprintf("%s:%s", host, port),
		Password: password,
		DB:       redisDB,
	}

	return options, nil
}

// NewConnectionManagerFromOptions creates a new ConnectionManager from the given options
func NewConnectionManagerFromOptions(options *redis.Options, logger *logger.CustomLogger, duration time.Duration, ctx context.Context) (*ConnectionManager, error) {
	if duration <= 0 {
		duration = defaultConnectionDuration
	}

	return NewConnectionManager(options, logger, duration, ctx)
}
