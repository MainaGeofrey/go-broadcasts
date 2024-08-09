package messages

import (
	"broadcasts/pkg/logger"
	"context"
	"database/sql"
	"sync"
	"time"
)

type BroadcastChecker struct {
	logger        *logger.CustomLogger
	broadcastRepo *BroadcastRepository
	broadcastChan chan map[string]interface{}
}

func NewBroadcastChecker(logger *logger.CustomLogger, db *sql.DB) *BroadcastChecker {
	return &BroadcastChecker{
		logger:        logger,
		broadcastRepo: NewBroadcastRepository(db),
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
		for {
			select {
			case <-ctx.Done():
				close(bc.broadcastChan)
				return
			default:
				broadcast, err := bc.broadcastRepo.FetchAndUpdateBroadcast(STATUS_NOT_FETCHED, STATUS_PROCESSING)
				if err != nil {
					bc.logger.Printf("Error fetching or updating broadcast: %v", err)
					time.Sleep(1 * time.Second) // Adjust sleep time as needed
					continue
				}
				if broadcast != nil {
					bc.broadcastChan <- broadcast
				}
				time.Sleep(1 * time.Second) // Adjust sleep time as needed
			}
		}
	}()
}
