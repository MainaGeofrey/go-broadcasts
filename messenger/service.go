package messenger

import (
	"broadcasts/pkg/logger"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)


type MessengerService struct {
	logger              *logger.CustomLogger
	messengerRepo       *MessengerRepository
	rabbitChannel       *amqp091.Channel
	broadcastsQueue     string
	broadcastsRespQueue string
	testPhone           string // For testing purposes
	appEnv              string // Application environment (e.g., "production")
}

// NewMessengerService creates a new MessengerService instance.
func NewMessengerService(logger *logger.CustomLogger, db *sql.DB, rabbitConn *amqp091.Connection, broadcastsQueue, broadcastsRespQueue string, testPhone, appEnv string) (*MessengerService, error) {
	channel, err := rabbitConn.Channel()
	if err != nil {
		logger.Printf("Failed to create RabbitMQ channel: %v", err)
		return nil, err
	}

	return &MessengerService{
		logger:              logger,
		messengerRepo:       NewMessengerRepository(db, logger),
		rabbitChannel:       channel,
		broadcastsQueue:     broadcastsQueue,
		broadcastsRespQueue: broadcastsRespQueue,
		testPhone:           testPhone,
		appEnv:              appEnv,
	}, nil
}

// ConsumeMessages starts consuming messages from RabbitMQ and processing them.
func (ms *MessengerService) ConsumeMessages(ctx context.Context) {
	msgs, err := ms.rabbitChannel.Consume(
		ms.broadcastsQueue, // Queue name
		"",                 // Consumer tag
		false,              // Auto-ack
		false,              // Exclusive
		false,              // No-local
		false,              // No-wait
		nil,                // Args
	)
	if err != nil {
		ms.logger.Printf("Failed to register a consumer: %v", err)
		return
	}
	ms.logger.Println("Started consuming messages from queue:", ms.broadcastsQueue)

	for {
		select {
		case <-ctx.Done():
			ms.logger.Println("Context canceled, stopping message consumption")
			return
		case d := <-msgs:
			ms.logger.Printf("Received message: %s", d.Body)
			ms.processMessage(ctx, d)
		}
	}
}


func (ms *MessengerService) processMessage(ctx context.Context, d amqp091.Delivery) {
	var broadcastList map[string]interface{}
	ms.logger.Println("Processing message...")

	// Acknowledge the message immediately, (message is not redelivered, even if the processing fails.)
	d.Ack(false)

	if err := json.Unmarshal(d.Body, &broadcastList); err != nil {
		ms.logger.Printf("Failed to unmarshal message: %v", err)

		return
	}

	ms.logger.Printf("Broadcast list details: %v", broadcastList)

	parentBroadcast, ok := broadcastList["parent_broadcast"].(map[string]interface{})
	if !ok {
		ms.logger.Printf("Invalid or missing parent_broadcast")

		return
	}

	broadcastID, ok := parentBroadcast["broadcast_id"].(string)
	if !ok {
		ms.logger.Printf("Invalid or missing broadcast_id")

		return
	}

	id, ok := broadcastList["list_id"].(string)
	if !ok {
		ms.logger.Printf("ID is missing or not a string")

		return
	}

	success := ms.SendSMS(ctx, broadcastList)
	var status int
	if success {
		status = STATUS_SUCCESS
	} else {
		status = STATUS_ERROR
	}

	err := ms.messengerRepo.UpdateBroadcastListProcessedStatus(broadcastID, id, status)
	if err != nil {
		ms.logger.Printf("Failed to update broadcast list status: %v", err)

		return
	}

	ms.logger.Printf("Message processed successfully: %v", broadcastList)
}


func (ms *MessengerService) SendSMS(ctx context.Context, broadcastList map[string]interface{}) bool {
	broadcast, ok := broadcastList["parent_broadcast"].(map[string]interface{})
	if !ok {
		ms.logger.Printf("Failed to extract parent broadcast configuration")
		return false
	}

	channelConfig, ok := broadcast["campaign_channel"].(map[string]interface{})
	if !ok {
		ms.logger.Printf("Failed to extract campaign channel configuration")
		return false
	}

	paramsInterface, ok := channelConfig["parameters"]
	if !ok {
		ms.logger.Printf("Failed to extract parameters")
		return false
	}

	paramsStr, ok := paramsInterface.(string)
	if !ok {
		ms.logger.Printf("Parameters are not in the expected format")
		return false
	}

	var parameters []map[string]string
	err := json.Unmarshal([]byte(paramsStr), &parameters)
	if err != nil {
		ms.logger.Printf("Failed to unmarshal parameters: %v", err)
		return false
	}

	message := &Message{
		MobileNumber:   broadcastList["msisdn"].(string),
		MessageContent: broadcastList["message_content"].(string),
	}

	if ms.appEnv == "development" {
		message.MobileNumber = ms.testPhone
	}

	payload := make(map[string]string)
	for _, param := range parameters {
		for key, value := range param {
			switch key {
			case "mobile":
				payload[key] = message.MobileNumber
			case "message":
				payload[key] = message.MessageContent
			default:
				payload[key] = value
			}
		}
	}

	ms.logger.Printf("Sending SMS to %s with payload: %v", message.MobileNumber, payload)

	reqBody, err := json.Marshal(payload)
	if err != nil {
		ms.logger.Printf("Failed to marshal request payload: %v", err)
		return false
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, channelConfig["url"].(string), strings.NewReader(string(reqBody)))
	if err != nil {
		ms.logger.Printf("Failed to create new request: %v", err)
		return false
	}

	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		ms.logger.Printf("Failed to send request: %v", err)
		return false
	}
	defer resp.Body.Close()

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		ms.logger.Printf("Failed to decode response: %v", err)
		return false
	}

	return apiResp.Success == "true"
}

type Message struct {
	MobileNumber   string
	MessageContent string
}

type APIResponse struct {
	Success string `json:"success"`
}
