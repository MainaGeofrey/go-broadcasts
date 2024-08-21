package sms

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
	"context"
	
	"github.com/redis/go-redis/v9" // Correct Redis client import
	"broadcasts/pkg/logger" // Import your custom logger package
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
	PackageID         uint16 `json:"package_id"`
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

func (s Sdp) SendSms(msisdn, senderId, message, uniqueId string, packageId uint16) bool {
	d := data{
		UserName:          s.Username,
		Channel:           "sms",
		PackageID:         packageId,
		Oa:                senderId,
		Msisdn:            msisdn,
		Message:           message,
		UniqueID:          uniqueId,
		ActionResponseURL: s.ResponseUrl,
	}

	p := sdpPayload{
		TimeStamp: time.Now().Unix(),
		DataSet:   []data{d},
	}

	payloadBytes, err := json.Marshal(p)
	if err != nil {
		s.Log.Printf("Failed to marshal payload: %v", err)
		return false
	}

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // Ignore expired SSL certificates
	}

	client := &http.Client{Timeout: 15 * time.Second, Transport: transCfg}

	s.Log.Printf("Payload: %s", string(payloadBytes))

	// Retrieve the token from Redis
	ctx := context.Background()
	token, err := s.Redis.Get(ctx, os.Getenv("SDP_TOKEN_KEY")).Result()
	if err != nil {
		s.Log.Printf("Failed to get token from Redis: %v", err)
		return false
	}

	request, err := http.NewRequest(http.MethodPost, os.Getenv("SDP_SEND_URL"), bytes.NewBuffer(payloadBytes))
	if err != nil {
		s.Log.Printf("Http request creation failed: %v", err)
		return false
	}
	request.Header.Set("X-Requested-With", "XMLHttpRequest")
	request.Header.Set("X-Authorization", fmt.Sprintf("Bearer %s", token))

	response, err := client.Do(request)
	if err != nil {
		s.Log.Printf("Doing actual http request: %v", err)
		return false
	}
	defer response.Body.Close()

	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		s.Log.Printf("Failed to read response body: %v", err)
		return false
	}

	s.Log.Printf("Status: %s", response.Status)
	s.Log.Printf("Response Body: %s", string(bodyBytes))

	if response.StatusCode != http.StatusOK {
		return false
	}
	return true
}
