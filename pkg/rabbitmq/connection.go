package rabbitmq

import (
    "github.com/rabbitmq/amqp091-go"
    "time"
    "broadcasts/pkg/logger"
    "context"
)

const defaultConnectionDuration = 5 * time.Minute

// ConnectionManager manages RabbitMQ connections
type ConnectionManager struct {
    url           string
    logger        *logger.CustomLogger
    connection    *amqp091.Connection
    duration      time.Duration
    closeChannel  chan struct{}
}

// NewConnectionManager creates a new ConnectionManager
func NewConnectionManager(url string, logger *logger.CustomLogger, duration time.Duration, ctx context.Context) (*ConnectionManager, error) {
    if duration <= 0 {
        duration = defaultConnectionDuration
    }

    cm := &ConnectionManager{
        url:          url,
        logger:       logger,
        duration:     duration,
        closeChannel: make(chan struct{}, 1),
    }

    if err := cm.createConnection(); err != nil {
        return nil, err
    }

    go cm.keepConnectionAlive(ctx)

    return cm, nil
}

// createConnection establishes a new RabbitMQ connection with retry logic
func (cm *ConnectionManager) createConnection() error {
    var err error
    for i := 0; i < 5; i++ { // Retry up to 5 times
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

// keepConnectionAlive maintains the connection for the specified duration
func (cm *ConnectionManager) keepConnectionAlive(ctx context.Context) {
    timer := time.NewTimer(cm.duration)
    defer timer.Stop()

    select {
    case <-timer.C:
        cm.Close()
    case <-ctx.Done():
        cm.Close()
    case <-cm.closeChannel:
        cm.Close()
    }
}

// Close closes the RabbitMQ connection
func (cm *ConnectionManager) Close() {
    if cm.connection != nil {
        cm.connection.Close()
        cm.logger.Println("Connection closed")
    }
}

// GetConnection returns the active RabbitMQ connection
func (cm *ConnectionManager) GetConnection() *amqp091.Connection {
    return cm.connection
}

// IsHealthy checks if the RabbitMQ connection is healthy
func (cm *ConnectionManager) IsHealthy() bool {
    if cm.connection == nil {
        return false
    }
    return cm.connection.IsClosed() == false
}
