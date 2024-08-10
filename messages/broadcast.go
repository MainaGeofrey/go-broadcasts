package messages

import (
	"broadcasts/pkg/logger"
	"context"
	"database/sql"
	"encoding/json"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

type BroadcastChecker struct {
	logger         *logger.CustomLogger
	broadcastRepo  *BroadcastRepository
	broadcastChan  chan map[string]interface{}
	rabbitChannel  *amqp091.Channel
}

// NewBroadcastChecker creates a new BroadcastChecker with RabbitMQ support
func NewBroadcastChecker(logger *logger.CustomLogger, db *sql.DB, rabbitConn *amqp091.Connection) (*BroadcastChecker, error) {
	channel, err := rabbitConn.Channel()
	if err != nil {
		logger.Printf("Failed to create RabbitMQ channel: %v", err)
		return nil, err
	}

	return &BroadcastChecker{
		logger:        logger,
		broadcastRepo: NewBroadcastRepository(db, logger),
		broadcastChan: make(chan map[string]interface{}, 100),
		rabbitChannel: channel,
	}, nil
}

// ProcessBroadcasts processes broadcasts and sends them to RabbitMQ
func (bc *BroadcastChecker) ProcessBroadcasts(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case broadcast := <-bc.broadcastChan:
			bc.logger.Printf("Processing broadcast: %v", broadcast)
			// Convert broadcast to a suitable format for RabbitMQ
			message, err := json.Marshal(broadcast)
			if err != nil {
				bc.logger.Printf("Failed to marshal broadcast message: %v", err)
				continue
			}

			err = bc.rabbitChannel.Publish(
				"",          // Exchange
				"broadcasts", // Routing key or queue name
				false,       // Mandatory
				false,       // Immediate
				amqp091.Publishing{
					ContentType: "application/json",
					Body:        message,
				},
			)
			if err != nil {
				bc.logger.Printf("Failed to publish message to RabbitMQ: %v", err)
			}
		}
	}
}

// Run fetches broadcasts and sends them to the processing channel
func (bc *BroadcastChecker) Run(ctx context.Context, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()

		limit := 1 // Number of records to fetch per batch

		for {
			select {
			case <-ctx.Done():
				close(bc.broadcastChan)
				return
			default:
				broadcast, err := bc.broadcastRepo.FetchAndUpdateBroadcast(STATUS_NOT_FETCHED, STATUS_PROCESSING)
				if err != nil {
					bc.logger.Printf("Error fetching or updating broadcast: %v", err)
					// time.Sleep(1 * time.Second) // Optional delay
					continue
				}
				if broadcast != nil {
					broadcastID, ok := broadcast["broadcast_id"].(string)
					if !ok {
						bc.logger.Printf("broadcast_id is missing or not a string")
						continue
					}

					offset := 0 // Reset offset for each new broadcast

					// Fetch related broadcast lists
					for {
						broadcastLists, err := bc.broadcastRepo.FetchBroadcastListsByBroadcastID(broadcastID, limit, offset)
						if err != nil {
							bc.logger.Printf("Error fetching broadcast lists: %v", err)
							break
						}

						if len(broadcastLists) == 0 {
							bc.logger.Printf("No more broadcast lists found for ID: %v", broadcastID)
							break
						}

					//	bc.logger.Printf("Processing broadcast: %v", broadcast)
					//	bc.logger.Printf("Associated broadcast lists: %v", broadcastLists)

						for _, bList := range broadcastLists {
							bc.logger.Printf("Broadcast list: %v", bList)
						}

						bc.broadcastChan <- broadcast
						offset += limit // Move to the next batch
					}
				} else {
					// time.Sleep(1 * time.Second) // Optional delay
				}
			}
		}
	}()
}
