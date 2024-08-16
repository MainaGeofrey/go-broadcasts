package rabbitmq

import (
    "github.com/rabbitmq/amqp091-go"
    "sync"
	"broadcasts/pkg/logger"
)

type ConnectionPool struct {
    mu        sync.Mutex
    pool      []*amqp091.Connection
    maxSize   int
    url       string
    logger    *logger.CustomLogger
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(url string, maxSize int, logger *logger.CustomLogger) (*ConnectionPool, error) {
    pool := &ConnectionPool{
        maxSize: maxSize,
        url:     url,
        logger:  logger,
    }
    if err := pool.init(); err != nil {
        return nil, err
    }
    return pool, nil
}

// init initializes the connection pool
func (p *ConnectionPool) init() error {
    for i := 0; i < p.maxSize; i++ {
        conn, err := amqp091.Dial(p.url)
        if err != nil {
            p.logger.Printf("Failed to connect to RabbitMQ: %v", err)
            return err
        }
        p.pool = append(p.pool, conn)
        p.logger.Printf("Connection %d created and added to the pool", i+1)
    }
    return nil
}

// GetConnection retrieves a connection from the pool
func (p *ConnectionPool) GetConnection() (*amqp091.Connection, error) {
    p.mu.Lock()
    defer p.mu.Unlock()

    if len(p.pool) == 0 {
        p.logger.Printf("Error retrieving connection from pool: no available connections")
        return nil, nil // Return nil error
    }

    conn := p.pool[len(p.pool)-1]
    p.pool = p.pool[:len(p.pool)-1]
    p.logger.Printf("Connection retrieved from the pool")
    return conn, nil
}

// ReleaseConnection returns a connection back to the pool
func (p *ConnectionPool) ReleaseConnection(conn *amqp091.Connection) {
    p.mu.Lock()
    defer p.mu.Unlock()

    if len(p.pool) < p.maxSize {
        p.pool = append(p.pool, conn)
        p.logger.Printf("Connection returned to the pool")
    } else {
        conn.Close() // Close the connection if the pool is full
        p.logger.Printf("Connection pool is full. Connection closed")
    }
}
