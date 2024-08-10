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

// MessengerService handles message processing and communication with RabbitMQ.
type MessengerService struct {
	logger            *logger.CustomLogger
	messengerRepo     *MessengerRepository
	rabbitChannel     *amqp091.Channel
	broadcastsQueue   string
	broadcastsRespQueue string
	testPhone         string // For testing purposes
	appEnv            string // Application environment (e.g., "production")
}

// NewMessengerService creates a new MessengerService instance.
func NewMessengerService(logger *logger.CustomLogger, db *sql.DB, rabbitConn *amqp091.Connection, broadcastsQueue, broadcastsRespQueue string, testPhone, appEnv string) (*MessengerService, error) {
	channel, err := rabbitConn.Channel()
	if err != nil {
		logger.Printf("Failed to create RabbitMQ channel: %v", err)
		return nil, err
	}

	return &MessengerService{
		logger:            logger,
		messengerRepo:     NewMessengerRepository(db, logger),
		rabbitChannel:     channel,
		broadcastsQueue:   broadcastsQueue,
		broadcastsRespQueue: broadcastsRespQueue,
		testPhone:         testPhone,
		appEnv:            appEnv,
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

	for {
		select {
		case <-ctx.Done():
			return
		case d := <-msgs:
			ms.processMessage(ctx, d)
		}
	}
}

// processMessage processes a single message from RabbitMQ.
func (ms *MessengerService) processMessage(ctx context.Context, d amqp091.Delivery) {
	var broadcastList map[string]interface{}
	if err := json.Unmarshal(d.Body, &broadcastList); err != nil {
		ms.logger.Printf("Failed to unmarshal message: %v", err)
		d.Nack(false, true) // Negative acknowledgment, requeue message
		return
	}

	broadcastID, ok := broadcastList["broadcast_id"].(string)
	if !ok {
		ms.logger.Printf("Broadcast ID is missing or not a string")
		d.Nack(false, true) // Negative acknowledgment, requeue message
		return
	}

	id, ok := broadcastList["id"].(string)
	if !ok {
		ms.logger.Printf("ID is missing or not a string")
		d.Nack(false, true) // Negative acknowledgment, requeue message
		return
	}

	// Send SMS and determine status
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
		d.Nack(false, true) // Negative acknowledgment, requeue message
		return
	}

	ms.logger.Printf("Message processed successfully: %v", broadcastList)
	d.Ack(false) // Acknowledge that message is processed
}

// SendSMS sends an SMS based on the message data and channel configuration.
func (ms *MessengerService) SendSMS(ctx context.Context, broadcastList map[string]interface{}) bool {
	// Extract configuration and message details
	config, ok := broadcastList["parent_broadcast"].(map[string]interface{})
	if !ok {
		ms.logger.Printf("Failed to extract parent broadcast configuration")
		return false
	}

	channelConfig, ok := config["campaign_channel"].(map[string]interface{})
	if !ok {
		ms.logger.Printf("Failed to extract campaign channel configuration")
		return false
	}

	parameters, ok := channelConfig["parameters"].([]interface{})
	if !ok {
		ms.logger.Printf("Failed to extract parameters")
		return false
	}

	message := &Message{
		MobileNumber:   broadcastList["msisdn"].(string),
		MessageContent: broadcastList["message_content"].(string),
	}

	// Build SMS payload
	payload := make(map[string]string)
	for _, param := range parameters {
		p, ok := param.(map[string]string)
		if !ok {
			ms.logger.Printf("Invalid parameter format")
			return false
		}
		for key, value := range p {
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


// Message represents the message to be sent via SMS.
type Message struct {
	MobileNumber   string
	MessageContent string
}

// APIResponse represents the response from the SMS API.
type APIResponse struct {
	Success string `json:"success"`
}
