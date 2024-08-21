package messenger

import (
	"broadcasts/pkg/logger"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

// MessengerService handles message processing and communication with RabbitMQ.
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

// ConsumeMessages starts consuming messages from RabbitMQ and processing them asynchronously.
func (ms *MessengerService) ConsumeMessages(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

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

			// Process the message asynchronously using a goroutine
			wg.Add(1)
			go ms.processMessage(ctx, d, wg)
		}
	}
}

// processMessage processes a message from the broadcasts queue asynchronously.
func (ms *MessengerService) processMessage(ctx context.Context, d amqp091.Delivery, wg *sync.WaitGroup) {
	defer wg.Done()

	var broadcastList map[string]interface{}
	ms.logger.Println("Processing message...")

	// Acknowledge the message immediately (message is not redelivered, even if the processing fails.)
	d.Ack(false)

	if err := json.Unmarshal(d.Body, &broadcastList); err != nil {
		ms.logger.Printf("Failed to unmarshal message: %v", err)
		return
	}

	ms.logger.Printf("Broadcast list details: %v", broadcastList)


	outboundResult := make(chan string, 1)
	smsResult := make(chan error, 1)


	go ms.sendSMS(ctx, smsResult, broadcastList)
	go ms.createOutbound(outboundResult, broadcastList)


	select {
	case outboundID := <-outboundResult:
		ms.logger.Printf("Outbound record created with ID: %s", outboundID)
	case err := <-smsResult:
		if err != nil {
			ms.logger.Printf("Error in SendSMS: %v", err)
			return
		}
		ms.logger.Println("SendSMS completed successfully")
	case <-ctx.Done():
		ms.logger.Println("Context canceled, stopping processing")
		return
	}

	return
	/*// Acknowledge the message after successful processing
	if err := d.Ack(false); err != nil {
		ms.logger.Printf("Failed to acknowledge message: %v", err)
	}*/

	// Create a status update message
	statusUpdate := map[string]interface{}{
		"broadcast_id": broadcastList["parent_broadcast"].(map[string]interface{})["broadcast_id"].(string),
		"list_id":      broadcastList["list_id"].(string),
		"status":       STATUS_SUCCESS, 
	}

	statusUpdateBody, err := json.Marshal(statusUpdate)
	if err != nil {
		ms.logger.Printf("Failed to marshal status update: %v", err)
		return
	}

	// Publish the status update to the response queue
	if err := ms.rabbitChannel.Publish(
		"",                     // Exchange
		ms.broadcastsRespQueue, // Routing key (queue name)
		false,                  // Mandatory
		false,                  // Immediate
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        statusUpdateBody,
		},
	); err != nil {
		ms.logger.Printf("Failed to publish status update: %v", err)
		return
	}

	ms.logger.Printf("Message processed successfully: %v", broadcastList)
}

// SendSMS sends an SMS message using the specified parameters.
func (ms *MessengerService) sendSMS(ctx context.Context, result chan<- error, broadcastList map[string]interface{}) {
	defer close(result)

	broadcast, ok := broadcastList["parent_broadcast"].(map[string]interface{})
	if !ok {
		ms.logger.Printf("Failed to extract parent broadcast configuration")
		result <- fmt.Errorf("failed to extract parent broadcast configuration")
		return
	}

	channelConfig, ok := broadcast["campaign_channel"].(map[string]interface{})
	if !ok {
		ms.logger.Printf("Failed to extract campaign channel configuration")
		result <- fmt.Errorf("failed to extract campaign channel configuration")
		return
	}

	paramsInterface, ok := channelConfig["parameters"]
	if !ok {
		ms.logger.Printf("Failed to extract parameters")
		result <- fmt.Errorf("failed to extract parameters")
		return
	}

	paramsStr, ok := paramsInterface.(string)
	if !ok {
		ms.logger.Printf("Parameters are not in the expected format")
		result <- fmt.Errorf("parameters are not in the expected format")
		return
	}

	var parameters []map[string]string
	if err := json.Unmarshal([]byte(paramsStr), &parameters); err != nil {
		ms.logger.Printf("Failed to unmarshal parameters: %v", err)
		result <- err
		return
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
		result <- err
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, channelConfig["url"].(string), strings.NewReader(string(reqBody)))
	if err != nil {
		ms.logger.Printf("Failed to create new request: %v", err)
		result <- err
		return
	}

	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		ms.logger.Printf("Failed to send request: %v", err)
		result <- err
		return
	}
	defer resp.Body.Close()

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		ms.logger.Printf("Failed to decode response: %v", err)
		result <- err
		return
	}

	if apiResp.Success != "true" {
		result <- fmt.Errorf("SMS sending failed")
		return
	}

	result <- nil
}

// createOutbound creates an outbound record in the database and returns the outbound ID.
func (ms *MessengerService) createOutbound(result chan<- string, broadcastList map[string]interface{}) {
	defer close(result)

	outboundID, err := ms.messengerRepo.CreateOutbound(broadcastList)
	if err != nil {
		ms.logger.Printf("Failed to insert outbound entry: %v", err)
		result <- ""
		return
	}

	// Convert outboundID from int64 to string
	outboundIDStr := strconv.FormatInt(outboundID, 10)
	ms.logger.Printf("Outbound record created with ID: %s", outboundIDStr)
	result <- outboundIDStr
}

func (ms *MessengerService) ConsumeStatusUpdates(ctx context.Context) {
	msgs, err := ms.rabbitChannel.Consume(
		ms.broadcastsRespQueue, // Queue name
		"",                     // Consumer tag
		true,                   // Auto-ack
		false,                  // Exclusive
		false,                  // No-local
		false,                  // No-wait
		nil,                    // Args
	)
	if err != nil {
		ms.logger.Printf("Failed to register a consumer for status updates: %v", err)
		return
	}
	ms.logger.Println("Started consuming status updates from queue:", ms.broadcastsRespQueue)

	for {
		select {
		case <-ctx.Done():
			ms.logger.Println("Context canceled, stopping status update consumption")
			return
		case d := <-msgs:
			ms.logger.Printf("Received status update: %s", d.Body)
			// Process the status update message
			ms.processStatusUpdate(d)
		}
	}
}

// processStatusUpdate processes a status update message from the status updates queue.
func (ms *MessengerService) processStatusUpdate(d amqp091.Delivery) {
	var statusUpdate map[string]interface{}
	if err := json.Unmarshal(d.Body, &statusUpdate); err != nil {
		ms.logger.Printf("Failed to unmarshal status update message: %v", err)
		return
	}

	ms.logger.Printf("Processing status update: %v", statusUpdate)

	// Use type assertion to convert float64 to int
	broadcastID, id := statusUpdate["broadcast_id"].(string), statusUpdate["list_id"].(string)

	// Type assertion for status value
	statusFloat, ok := statusUpdate["status"].(float64)
	if !ok {
		ms.logger.Printf("Failed to assert status as float64")
		return
	}
	status := int(statusFloat)

	err := ms.messengerRepo.UpdateBroadcastListProcessedStatus(broadcastID, id, status)
	if err != nil {
		ms.logger.Printf("Failed to update broadcast list status: %v", err)
		return
	}

	ms.logger.Println("Status update processed successfully")
}

type Message struct {
	MobileNumber   string
	MessageContent string
}

// APIResponse represents the response from the SMS API.
type APIResponse struct {
	Success string `json:"success"`
}
