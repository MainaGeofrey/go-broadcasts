package messages

import (
	"broadcasts/channels"
	"broadcasts/pkg/logger"
	"database/sql"
	"fmt"
	"strconv"
)

type BroadcastRepository struct {
	db              *sql.DB
	logger          *logger.CustomLogger
	channelsFetcher *channels.ChannelsFetcher
}

// NewBroadcastRepository creates a new instance of BroadcastRepository.
func NewBroadcastRepository(db *sql.DB, logger *logger.CustomLogger, channelsFetcher *channels.ChannelsFetcher) *BroadcastRepository {
	return &BroadcastRepository{
		db:              db,
		logger:          logger,
		channelsFetcher: channelsFetcher,
	}
}

// Fetch retrieves a broadcast from the database based on the status and sent time.
func (r *BroadcastRepository) Fetch(status int) (map[string]interface{}, error) {
	broadcast := make(map[string]interface{})
	query := `SELECT b.broadcast_id, b.project_id, b.campaign_channel, b.sent_time, b.message_content, b.source_list,
                     b.status, b.original_filename, b.generated_filename, b.credits_used, b.client_id,segment_id, campaign_channel
              FROM broadcasts b 
              WHERE b.status = ? AND b.sent_time <= NOW()
              LIMIT 1`

	rows, err := r.db.Query(query, status)
	if err != nil {
		r.logger.Printf("Error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		var campaignChannel, sentTime, messageContent, sourceList, originalFilename, generatedFilename string
		var status, segment_id, projectID, broadcastID, clientID int
		var creditsUsed int
		var campaign_channel string

		if err := rows.Scan(&broadcastID, &projectID, &campaignChannel, &sentTime, &messageContent, &sourceList,
			&status, &originalFilename, &generatedFilename, &creditsUsed, &clientID, &segment_id, &campaign_channel); err != nil {
			r.logger.Printf("Error scanning row: %v", err)
			return nil, err
		}

		broadcast["broadcast_id"] = broadcastID
		broadcast["project_id"] = projectID
		broadcast["campaign_channel"] = campaign_channel
		broadcast["sent_time"] = sentTime
		broadcast["message_content"] = messageContent
		broadcast["source_list"] = sourceList
		broadcast["status"] = status
		broadcast["original_filename"] = originalFilename
		broadcast["generated_filename"] = generatedFilename
		broadcast["credits_used"] = creditsUsed
		broadcast["client_id"] = clientID
		broadcast["segment_id"] = segment_id
	} else {
		return nil, nil
	}

	return broadcast, nil
}

// Update updates the status of a broadcast based on its broadcast_id.
func (r *BroadcastRepository) Update(broadcastID int, status int) error {
	query := `UPDATE broadcasts
              SET status = ?
              WHERE broadcast_id = ?`
	_, err := r.db.Exec(query, status, broadcastID)
	if err != nil {
		r.logger.Printf("Error updating broadcast STATUS status: %v", err)
		return err
	}
	return nil
}

func (r *BroadcastRepository) FetchAndUpdateBroadcast(status int, newStatus int, errorState int) (map[string]interface{}, int, error) {
	broadcast, err := r.Fetch(status)
	if err != nil {
		return nil, 0, err
	}
	if broadcast == nil {
		return nil, 0, nil
	}

	broadcastID, ok := broadcast["broadcast_id"].(int)
	if !ok {
		r.logger.Printf("broadcast_id is missing or not an int")
		return nil, 0, fmt.Errorf("broadcast_id is missing or not an int")
	}

	updatedBroadcast, err := r.updateBroadcastChannel(broadcast)
	if err != nil {
		r.logger.Printf("Failed to update campaign channel for broadcast ID: %v, error: %v", broadcast["broadcast_id"], err)

		updateErr := r.Update(broadcastID, errorState)
		if updateErr != nil {
			r.logger.Printf("Failed to update broadcast status to 0: %v", updateErr)
			return nil, 0, updateErr
		}

		return nil, errorState, err
	}

	err = r.Update(broadcastID, newStatus)
	if err != nil {
		return nil, 0, err
	}

	return updatedBroadcast, newStatus, nil
}

func (r *BroadcastRepository) updateBroadcastChannel(broadcast map[string]interface{}) (map[string]interface{}, error) {
	projectID, ok := broadcast["project_id"].(int)
	if !ok {
		return nil, fmt.Errorf("project_id is missing or not an int in broadcast: %v", broadcast)
	}

	campaignChannel, ok := broadcast["campaign_channel"].(string)
	if !ok {
		return nil, fmt.Errorf("campaign_channel is missing or not a string in broadcast: %v", broadcast)
	}

	clientID, ok := broadcast["client_id"].(int)
	if !ok {
		return nil, fmt.Errorf("client_id is missing or not an int in broadcast: %v", broadcast)
	}

	campaignChannelID, err := strconv.Atoi(campaignChannel)
	if err != nil {
		return nil, fmt.Errorf("campaign_channel is a string but not a valid integer: %v, broadcast: %v", campaignChannel, broadcast)
	}

	campaignChannels, err := r.channelsFetcher.GetCachedChannel(campaignChannelID, clientID, projectID)
	if err != nil {
		return nil, fmt.Errorf("error retrieving cached channel from Redis for campaignChannelID: %d, clientID: %d, projectID: %d. Error: %v", campaignChannelID, clientID, projectID, err)
	} else if campaignChannels == nil {
		return nil, fmt.Errorf("no channel found in Redis for campaignChannelID: %d, clientID: %d, projectID: %d", campaignChannelID, clientID, projectID)
	}

	broadcast["campaign_channel"] = campaignChannels
	r.logger.Printf("Updated broadcast with channel from Redis: %v", broadcast)

	return broadcast, nil
}

// FetchBroadcastListsByBroadcastID retrieves broadcast lists for a given broadcast_id with pagination.
func (r *BroadcastRepository) FetchBroadcastListsByBroadcastID(broadcastID int, clientID int, limit, offset int) ([]map[string]interface{}, error) {
	var broadcastLists []map[string]interface{}

	//r.logger.Printf("Fetching broadcast lists for ID: %v with limit: %d and offset: %d", broadcastID, limit, offset)
	/* Add check to remove blacklist */
	query := `SELECT list_id, message_content, msg_length, msg_pages, msisdn
              FROM broadcast_lists
              WHERE broadcast_id = ? and msisdn not in (select distinct msisdn from blacklist where status in (0,1)  and client_id =? )
              LIMIT ? OFFSET ?`

	rows, err := r.db.Query(query, broadcastID, clientID, limit, offset)
	if err != nil {
		r.logger.Printf("Error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	if rows == nil {
		r.logger.Printf("No rows returned for ID: %v", broadcastID)
	}

	for rows.Next() {
		var listID, messageContent, msgLength, msgPages, msisdn string
		if err := rows.Scan(&listID, &messageContent, &msgLength, &msgPages, &msisdn); err != nil {
			r.logger.Printf("Error scanning row: %v", err)
			return nil, err
		}

		broadcastList := map[string]interface{}{
			"list_id":         listID,
			"message_content": messageContent,
			"msg_length":      msgLength,
			"msg_pages":       msgPages,
			"msisdn":          msisdn,
		}
		broadcastLists = append(broadcastLists, broadcastList)
	}

	if len(broadcastLists) == 0 {
		r.logger.Printf("No broadcast lists found for ID: %v", broadcastID)
	}

	return broadcastLists, nil
}
