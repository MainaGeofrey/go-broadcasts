package messages

import (
	"broadcasts/pkg/logger"
	"context"
	"database/sql"
	"sync"
)

type BroadcastChecker struct {
	logger        *logger.CustomLogger
	broadcastRepo *BroadcastRepository
	broadcastChan chan map[string]interface{}
}

func NewBroadcastChecker(logger *logger.CustomLogger, db *sql.DB) *BroadcastChecker {
	return &BroadcastChecker{
		logger:        logger,
		broadcastRepo: NewBroadcastRepository(db, logger),
		broadcastChan: make(chan map[string]interface{}, 100),
	}
}

func (bc *BroadcastChecker) ProcessBroadcasts(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case broadcast := <-bc.broadcastChan:
			bc.logger.Printf("Processing broadcast: %v", broadcast)
			// Add RabbitMQ integration and further processing here
		}
	}
}

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
				///	time.Sleep(1 * time.Second)
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

				
						bc.logger.Printf("Processing broadcast: %v", broadcast)
						bc.logger.Printf("Associated broadcast lists: %v", broadcastLists)

						for _, bList := range broadcastLists {
							bc.logger.Printf("Broadcast list: %v", bList)
						}

						bc.broadcastChan <- broadcast
						offset += limit // Move to the next batch
					}
				} else {
				//	time.Sleep(1 * time.Second) 
				}
			}
		}
	}()
}
