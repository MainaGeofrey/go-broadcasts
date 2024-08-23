package messenger

import (
	"broadcasts/pkg/logger"
	"broadcasts/pkg/sms"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"net/http"
	"strconv"
	"strings"
	"sync"
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
	sdpUserName         string
	sdpResponseUrl      string
	Redis               *redis.Client
}

// NewMessengerService creates a new MessengerService instance.
func NewMessengerService(logger *logger.CustomLogger, db *sql.DB, rabbitConn *amqp091.Connection, broadcastsQueue, broadcastsRespQueue string, testPhone, appEnv string, sdpUserName string, sdpResponseUrl string, redisClient *redis.Client) (*MessengerService, error) {
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
		sdpUserName:         sdpUserName,
		sdpResponseUrl:      sdpResponseUrl,
		Redis:               redisClient,
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

	outboundID, err := ms.createOutboundSync(broadcastList)
	if err != nil {
		ms.logger.Printf("Error in createOutbound: %v", err)
		return
	}
	ms.logger.Printf("Create outbound result received: %s", outboundID)

	// Send SMS
	if err := ms.sendSMS(ctx, broadcastList, outboundID); err != nil {
		ms.logger.Printf("Error in sendSMS: %v", err)
		return
	}

	outboundIDInt, err := strconv.ParseInt(outboundID, 10, 64)
	if err != nil {
		ms.logger.Printf("Error converting outboundID to int64: %v", err)
		return
	}

	go func() {
		if err := ms.messengerRepo.UpdateOutboundStatus(outboundIDInt, STATUS_SENT); err != nil {
			ms.logger.Printf("Error updating outbound status: %v", err)
		}
		ms.logger.Printf("Outbound status updated to %d", STATUS_SENT)
	}()

	ms.logger.Println("SendSMS completed successfully")

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

func (ms *MessengerService) sendSMS(ctx context.Context, broadcastList map[string]interface{}, outboundID string) error {
	broadcast, ok := broadcastList["parent_broadcast"].(map[string]interface{})
	if !ok {
		ms.logger.Printf("Failed to extract parent broadcast configuration")
		return fmt.Errorf("failed to extract parent broadcast configuration")
	}

	channelConfig, ok := broadcast["campaign_channel"].(map[string]interface{})
	if !ok {
		ms.logger.Printf("Failed to extract campaign channel configuration")
		return fmt.Errorf("failed to extract campaign channel configuration")
	}

	paramsInterface, ok := channelConfig["Parameters"]
	if !ok {
		ms.logger.Printf("Failed to extract parameters")
		return fmt.Errorf("failed to extract parameters")
	}

	paramsStr, ok := paramsInterface.(string)
	if !ok {
		ms.logger.Printf("Parameters are not in the expected format")
		return fmt.Errorf("parameters are not in the expected format")
	}

	var parameters []map[string]string
	if err := json.Unmarshal([]byte(paramsStr), &parameters); err != nil {
		ms.logger.Printf("Failed to unmarshal parameters: %v", err)
		return err
	}

	message := &Message{
		MobileNumber:   broadcastList["msisdn"].(string),
		MessageContent: broadcastList["message_content"].(string),
	}

	if ms.appEnv == "development" {
		message.MobileNumber = ms.testPhone
	}

	/*	senderType, ok := channelConfig["SenderType"]
		if !ok {
			ms.logger.Printf("Failed to extract sender type")
			result <- fmt.Errorf("failed to extract sender type")
			return
		}*/
	senderType := "sdp"

	switch senderType {
	case "api":
		ms.sendToApi(ctx, channelConfig["URL"].(string), parameters, message)
	case "sdp":
		ms.sendToSDP(message.MobileNumber, "senderID", message.MessageContent, outboundID)
	default:
		ms.logger.Printf("Unknown sender type: %s", senderType)
		return fmt.Errorf("unknown sender type: %s", senderType)
	}

	return nil
}

func (ms *MessengerService) sendToSDP(msisdn, senderId, message, outboundID string) {
	sdpService := sms.Sdp{
		Username:    ms.sdpUserName,
		Redis:       ms.Redis,
		ResponseUrl: ms.sdpResponseUrl,
		Log:         logger.Logger,
	}

	var packageId uint16 = 4605
	success := sdpService.SendSms(msisdn, senderId, message, outboundID, packageId)
	if success {
		logger.Logger.Println("SMS sent successfully")
	} else {
		logger.Logger.Println("Failed to send SMS")
	}
}

func (ms *MessengerService) sendToApi(ctx context.Context, url string, parameters []map[string]string, message *Message) {
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
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(reqBody)))
	if err != nil {
		ms.logger.Printf("Failed to create new request: %v", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		ms.logger.Printf("Failed to send request: %v", err)
		return
	}
	defer resp.Body.Close()

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		ms.logger.Printf("Failed to decode response: %v", err)
		return
	}

	if apiResp.Success != "true" {
		ms.logger.Printf("SMS sending failed")
		return
	}
}

func (ms *MessengerService) createOutboundSync(broadcastList map[string]interface{}) (string, error) {
	outboundID, err := ms.messengerRepo.CreateOutbound(broadcastList)
	if err != nil {
		return "", err
	}

	return strconv.FormatInt(outboundID, 10), nil
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
