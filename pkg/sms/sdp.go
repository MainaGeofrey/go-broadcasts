package sms

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"broadcasts/pkg/logger"        // Import your custom logger package
	"github.com/redis/go-redis/v9" // Correct Redis client import
)

type Sdp struct {
	Username    string
	Redis       *redis.Client // Use go-redis client
	ResponseUrl string
	Log         *logger.CustomLogger // Add the custom logger field
}

type data struct {
	UserName          string `json:"userName"`
	Channel           string `json:"channel"`
	Oa                string `json:"oa"`
	Msisdn            string `json:"msisdn"`
	Message           string `json:"message"`
	UniqueID          string `json:"uniqueId"`
	ActionResponseURL string `json:"actionResponseURL"`
}

type sdpPayload struct {
	TimeStamp int64  `json:"timeStamp"`
	DataSet   []data `json:"dataSet"`
}

func (s Sdp) SendSms(msisdn, senderId, message, uniqueId string) bool {
	d := data{
		UserName:           os.Getenv("SDP_USERNAME"),
		Channel:           "sms",
		Oa:                senderId,
		Msisdn:            msisdn,
		Message:           message,
		UniqueID:          uniqueId,
		ActionResponseURL: os.Getenv("SDP_DN_URL"),
	}

	p := sdpPayload{
		TimeStamp: time.Now().Unix(),
		DataSet:   []data{d},
	}
	s.Log.Printf("MessagePusher|OutboundID: %s, Payload: %s",uniqueId, d)
	payloadBytes, err := json.Marshal(p)
	if err != nil {
		s.Log.Printf("essagePusher|OutboundID: %s,Failed to marshal payload: %v",uniqueId, err)
		return false
	}

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // Ignore expired SSL certificates
	}

	client := &http.Client{Timeout: 15 * time.Second, Transport: transCfg}

	s.Log.Printf("MessagePusher| Payload: %s | Url %s ", string(payloadBytes),os.Getenv("SDP_SEND_URL"))
 	s.Log.Printf("MessagePusher| Redis key fetched sdp keyxXXXX: %v",os.Getenv("SDP_TOKEN_KEY"))
	// Retrieve the token from Redis
	ctx := context.Background()
	token, err := s.Redis.Get(ctx, os.Getenv("SDP_TOKEN_KEY")).Result()
	if err != nil {
		s.Log.Fatalf("MessagePusher| Failed to get token from Redis: %v", err)

		s.Log.Fatalf("MessagePusher| Redis ERROR, missing sdp key")
		s.Log.Fatalf("MessagePusher| Exiting application.........")
		os.Exit(1)
		return false
	}

	request, err := http.NewRequest(http.MethodPost, os.Getenv("SDP_SEND_URL"), bytes.NewBuffer(payloadBytes))
	if err != nil {
		s.Log.Fatalf("MessagePusher| Http request creation failed: %v", err)
		return false
	}
	request.Header.Set("X-Requested-With", "XMLHttpRequest")
	request.Header.Set("X-Authorization", fmt.Sprintf("Bearer %s", token))

	response, err := client.Do(request)
	if err != nil {
		s.Log.Printf("MessagePusher|Doing actual http request: %v", err)
		return false
	}
	defer response.Body.Close()

	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		s.Log.Printf("MessagePusher|Failed to read response body: %v", err)
		return false
	}

	s.Log.Printf("MessagePusher|Status: %s", response.Status)
	s.Log.Printf("MessagePusher|Response Body: %s", string(bodyBytes))

	if response.StatusCode != http.StatusOK {
		return false
	}
	return true
}
