package channels

import (
	"broadcasts/pkg/logger"
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"database/sql"
)

// ChannelsFetcher is responsible for fetching channels and caching them in Redis.
type ChannelsFetcher struct {
	Repo  *ChannelsRepository
	Log   *logger.CustomLogger
	Redis *redis.Client
}

// NewChannelsFetcher creates a new instance of ChannelsFetcher.
func NewChannelsFetcher(db *sql.DB, log *logger.CustomLogger, redisClient *redis.Client) *ChannelsFetcher {
	return &ChannelsFetcher{
		Repo:  NewChannelsRepository(db, log),
		Log:   log,
		Redis: redisClient,
	}
}

// FetchAndCacheChannels fetches channels from the repository and caches them in Redis indefinitely.
func (cf *ChannelsFetcher) FetchAndCacheChannels(status int) error {
	channels, err := cf.Repo.Fetch(status)
	if err != nil {
		return fmt.Errorf("error fetching channels: %v", err)
	}

	for _, channel := range channels {
		id, clientID, projectID := channel.ID, channel.ClientID, channel.ProjectID
		key := fmt.Sprintf("%d_%d_%d", id, clientID, projectID)

		channelJSON, err := json.Marshal(channel)
		if err != nil {
			cf.Log.Printf("Error marshaling channel to JSON: %v", err)
			continue
		}

		// Cache the channel in Redis without an expiry
		err = cf.Redis.Set(context.Background(), key, channelJSON, 0).Err() //no expiry
		if err != nil {
			cf.Log.Printf("Error setting channel in Redis: %v", err)
		} else {
			cf.Log.Printf("Channel cached in Redis with key: %s", key)
		}
	}

	return nil
}


// GetCachedChannel retrieves a cached channel from Redis based on id, client_id, and project_id.
func (cf *ChannelsFetcher) GetCachedChannel(id, clientID, projectID int) (map[string]interface{}, error) {
	key := fmt.Sprintf("%d_%d_%d", id, clientID, projectID)

	// Get the cached channel from Redis
	val, err := cf.Redis.Get(context.Background(), key).Result()
	if err != nil {
		if err == redis.Nil {
			cf.Log.Printf("No channel found in Redis with key: %s", key)
			return nil, nil
		}
		cf.Log.Printf("Error getting channel from Redis: %v", err)
		return nil, err
	}

	// Deserialize the JSON value into a map
	var channel map[string]interface{}
	if err := json.Unmarshal([]byte(val), &channel); err != nil {
		cf.Log.Printf("Error unmarshaling channel from JSON: %v", err)
		return nil, err
	}

	return channel, nil
}
