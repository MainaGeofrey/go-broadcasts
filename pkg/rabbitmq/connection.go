package rabbitmq

import (
	"broadcasts/pkg/logger"
	"context"
	"github.com/rabbitmq/amqp091-go"
	"os"
	"time"
)

const defaultConnectionDuration = 5 * time.Minute

type ConnectionManager struct {
	url             string
	logger          *logger.CustomLogger
	connection      *amqp091.Connection
	duration        time.Duration
	activityChannel chan struct{}
	closeChannel    chan struct{}
}

func NewConnectionManager(url string, logger *logger.CustomLogger, duration time.Duration, ctx context.Context) (*ConnectionManager, error) {
	if duration <= 0 {
		duration = defaultConnectionDuration
	}

	cm := &ConnectionManager{
		url:             url,
		logger:          logger,
		duration:        duration,
		activityChannel: make(chan struct{}, 1),
		closeChannel:    make(chan struct{}, 1),
	}

	if err := cm.createConnection(); err != nil {
		return nil, err
	}

	go cm.keepConnectionAlive(ctx)
	go cm.listenForClose()

	return cm, nil
}

// createConnection establishes a new RabbitMQ connection with retry logic
func (cm *ConnectionManager) createConnection() error {
	var err error
	for i := 0; i < 3; i++ { // Retry up to 3 times
		conn, err := amqp091.Dial(cm.url)
		if err == nil {
			cm.connection = conn
			cm.logger.Printf("Connection established to RabbitMQ at %s", cm.url)
			return nil
		}
		cm.logger.Printf("Failed to connect to RabbitMQ at %s (attempt %d): %v", cm.url, i+1, err)
		time.Sleep(2 * time.Second) // Wait before retrying
	}
	return err
}

// listenForClose listens for connection close notifications
func (cm *ConnectionManager) listenForClose() {
	closeErr := make(chan *amqp091.Error)
	cm.connection.NotifyClose(closeErr)

	for err := range closeErr {
		cm.logger.Printf("Connection closed: %v", err)
		cm.reconnect()
	}
}

func (cm *ConnectionManager) reconnect() {
	if err := cm.createConnection(); err != nil {
		cm.logger.Printf("Reconnection failed: %v", err)
	}
}

// keepConnectionAlive maintains the connection and checks its health
func (cm *ConnectionManager) keepConnectionAlive(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second) // Set shorter interval for quicker detection
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			cm.Close()
			return
		case <-cm.closeChannel:
			cm.Close()
			return
		case <-ticker.C:
			if !cm.IsHealthy() {
				//cm.logger.Println("Connection lost, attempting to reconnect...")
				//cm.reconnect()

				cm.logger.Fatalf("RabbitMq connection error detected after a health check")
				cm.logger.Fatalf("Exiting application without attempting to reconnect.........")
				os.Exit(1)
			}
		}

		select {
		case <-cm.activityChannel:
			lastActivity := time.Now() // Update lastActivity only when activity is detected
			cm.logger.Printf("Last activity updated at: %v", lastActivity)
		default:
			// No activity; proceed to next tick
		}
	}
}

func (cm *ConnectionManager) NotifyActivity() {
	cm.activityChannel <- struct{}{}
}

func (cm *ConnectionManager) Close() {
	if cm.connection != nil {
		cm.connection.Close()
		cm.logger.Println("Connection closed")
	}
}

func (cm *ConnectionManager) GetConnection() *amqp091.Connection {
	return cm.connection
}

func (cm *ConnectionManager) IsHealthy() bool {
	if cm.connection == nil {
		return false
	}
	return !cm.connection.IsClosed()
}
