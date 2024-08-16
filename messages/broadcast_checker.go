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
	queueName      string
}

func BroadcastChecker(logger *logger.CustomLogger, db *sql.DB, rabbitChannel *amqp091.Channel, queueName string) (*BroadcastChecker, error) {
	return &BroadcastChecker{
		logger:        logger,
		broadcastRepo: NewBroadcastRepository(db, logger),
		broadcastChan: make(chan map[string]interface{}, 100),
		rabbitChannel: rabbitChannel,
		queueName:     queueName,
	}, nil
}

func (bc *BroadcastChecker) ProcessBroadcasts(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case broadcast := <-bc.broadcastChan:
			bc.logger.Printf("Processing broadcast: %v", broadcast)
			message, err := json.Marshal(broadcast)
			if err != nil {
				bc.logger.Printf("Failed to marshal broadcast message: %v", err)
				continue
			}

			err = bc.rabbitChannel.Publish(
				"",          // Exchange
				bc.queueName, // Use the queue name
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

func (bc *BroadcastChecker) Run(ctx context.Context, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()

		limit := 1

		for {
			select {
			case <-ctx.Done():
				close(bc.broadcastChan)
				return
			default:
				broadcast, err := bc.broadcastRepo.FetchAndUpdateBroadcast(STATUS_NOT_FETCHED, STATUS_PROCESSING)
				if err != nil {
					bc.logger.Printf("Error fetching or updating broadcast: %v", err)
					continue
				}
				if broadcast != nil {
					broadcastID, ok := broadcast["broadcast_id"].(int)
					if !ok {
						bc.logger.Printf("broadcast_id is missing or not a string")
						continue
					}
					clientID, ok := broadcast["client_id"].(int)
					if !ok {
						bc.logger.Printf("client_id is missing or not a string")
						continue
					}
/*Fetch all People in the List to process 
*/


					offset := 0
					for {
						broadcastLists, err := bc.broadcastRepo.FetchBroadcastListsByBroadcastID(broadcastID,clientID, limit, offset)
						if err != nil {
							bc.logger.Printf("Error fetching broadcast lists: %v", err)
							break
						}

						if len(broadcastLists) == 0 {
							bc.logger.Printf("No more broadcast lists found for ID: %v", broadcastID)
							break
						}

						for _, bList := range broadcastLists {
							bList["parent_broadcast"] = broadcast
							bc.logger.Printf("Broadcast list with parent details: %v", bList)
							bc.broadcastChan <- bList
						}

						offset += limit
					}
				}
			}
		}
	}()
}
