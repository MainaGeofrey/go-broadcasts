package messages

import (
	"broadcasts/channels"
	"broadcasts/pkg/logger"
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

const bufferSize = 100

type BroadcastChecker struct {
	logger          *logger.CustomLogger
	broadcastRepo   *BroadcastRepository
	broadcastChan   chan map[string]interface{}
	rabbitChannel   *amqp091.Channel
	queueName       string
	channelsFetcher *channels.ChannelsFetcher
}

func BroadcastCheckerProcess(logger *logger.CustomLogger, db *sql.DB, rabbitChannel *amqp091.Channel, queueName string, channelsFetcher *channels.ChannelsFetcher) (*BroadcastChecker, error) {
	return &BroadcastChecker{
		logger:          logger,
		broadcastRepo:   NewBroadcastRepository(db, logger),
		broadcastChan:   make(chan map[string]interface{}, bufferSize),
		rabbitChannel:   rabbitChannel,
		queueName:       queueName,
		channelsFetcher: channelsFetcher,
	}, nil
}

func (bc *BroadcastChecker) ProcessBroadcasts(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case broadcast, ok := <-bc.broadcastChan:
			if !ok {
				// Channel is closed and empty
				bc.logger.Println("Broadcast channel is closed")
				return
			}

			if len(broadcast) == 0 {
				bc.logger.Println("Received empty broadcast, skipping")
				continue
			}

			bc.logger.Printf("Processing broadcast: %v", broadcast)
			message, err := json.Marshal(broadcast)
			if err != nil {
				bc.logger.Printf("Failed to marshal broadcast message: %v", err)
				continue
			}

			err = bc.rabbitChannel.Publish(
				"",           // Exchange
				bc.queueName, // Use the queue name
				false,        // Mandatory
				false,        // Immediate
				amqp091.Publishing{
					ContentType: "application/json",
					Body:        message,
				},
			)
			if err != nil {
				bc.logger.Printf("Failed to publish message to RabbitMQ: %v", err)
			}
		default:
			// handle cases where no broadcasts are available
			continue
		}
	}
}

func (bc *BroadcastChecker) Run(ctx context.Context, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()

		limit := bufferSize

		for {
			select {
			case <-ctx.Done():
				close(bc.broadcastChan)
				return
			default:
				broadcast, err := bc.broadcastRepo.FetchAndUpdateBroadcast(STATUS_NOT_FETCHED, STATUS_PROCESSING)
				if err != nil {
					bc.logger.Printf("Error fetching or updating broadcast: %v", err)
					close(bc.broadcastChan)
					return
				}

				if broadcast != nil {
					broadcastID, ok := broadcast["broadcast_id"].(int)
					if !ok {
						bc.logger.Printf("broadcast_id is missing or not an int")
						continue
					}
					clientID, ok := broadcast["client_id"].(int)
					if !ok {
						bc.logger.Printf("client_id is missing or not an int")
						continue
					}

					projectID, ok := broadcast["project_id"].(int)
					if !ok {
						bc.logger.Printf("project id is missing or not an int")
						continue
					}

					campaignChannel, ok := broadcast["campaign_channel"].(string)
					if !ok {
						bc.logger.Printf("campaign_channel ID is missing or not a string")
						continue
					}

					campaignChannelID, err := strconv.Atoi(campaignChannel)
					if err != nil {
						bc.logger.Printf("campaign_channel ID is a string but not a valid integer: %v", campaignChannel)
						continue
					}

					campaignChannels, err := bc.channelsFetcher.GetCachedChannel(campaignChannelID, clientID, projectID)
					if err != nil {
						bc.logger.Printf("Error retrieving cached channel: %v", err)
					} else if campaignChannels == nil {
						bc.logger.Println("No channel found in Redis.")
					} else {

						broadcast["campaign_channel"] = campaignChannels
						bc.logger.Printf("Updated broadcast with channel from Redis: %v", broadcast)
					}

					offset := 0
					for {
						broadcastLists, err := bc.broadcastRepo.FetchBroadcastListsByBroadcastID(broadcastID, clientID, limit, offset)
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
