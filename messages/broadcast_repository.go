package messages

import (
	"broadcasts/pkg/logger"
	"database/sql"
)

type BroadcastRepository struct {
	db     *sql.DB
	logger *logger.CustomLogger
}

// NewBroadcastRepository creates a new instance of BroadcastRepository.
func NewBroadcastRepository(db *sql.DB, logger *logger.CustomLogger) *BroadcastRepository {
	return &BroadcastRepository{
		db:     db,
		logger: logger,
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
		var campaignChannel, sentTime, messageContent, sourceList, originalFilename, generatedFilename, clientID string
		var status, segment_id,projectID,broadcastID, clientID int
		var creditsUsed int
		var campaign_channel string

		if err := rows.Scan(&broadcastID, &projectID, &campaignChannel, &sentTime, &messageContent, &sourceList,
			&status, &originalFilename, &generatedFilename, &creditsUsed, &clientID,
			&campaign_channel,&segment_id); err != nil {
			r.logger.Printf("Error scanning row: %v", err)
			return nil, err
		}

		broadcast["broadcast_id"] = broadcastID
		broadcast["project_id"] = projectID
		broadcast["campaign_channel"] =campaign_channel
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
		return nil, nil // No matching records found
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
		r.logger.Printf("Error updating broadcast status: %v", err)
		return err
	}
	return nil
}

// FetchAndUpdateBroadcast retrieves and updates a broadcast based on status.
func (r *BroadcastRepository) FetchAndUpdateBroadcast(status int, status int) (map[string]interface{}, error) {
	broadcast, err := r.Fetch(status)
	if err != nil {
		return nil, err
	}
	if broadcast == nil {
		return nil, nil // No broadcast found
	}

	broadcastID, ok := broadcast["broadcast_id"].(int)
	if !ok {
		r.logger.Printf("broadcast_id is missing or not a number")
		return nil, err
	}

	err = r.Update(broadcastID, status)
	if err != nil {
		return nil, err
	}

	return broadcast, nil
}

// FetchBroadcastListsByBroadcastID retrieves broadcast lists for a given broadcast_id with pagination.
func (r *BroadcastRepository) FetchBroadcastListsByBroadcastID(broadcastID int, clientID int, limit, offset int) ([]map[string]interface{}, error) {
	var broadcastLists []map[string]interface{}

	//r.logger.Printf("Fetching broadcast lists for ID: %v with limit: %d and offset: %d", broadcastID, limit, offset)
/* Add check to remove blacklist */
	query := `SELECT list_id, message_content, msg_length, msg_pages, msisdn
              FROM broadcast_lists
              WHERE broadcast_id = ? and and msisdn not in (select distinct msisdn from blacklist where status in (0,1)  and client_id =? )
              LIMIT ? OFFSET ?`

	rows, err := r.db.Query(query, broadcastID, clientID,limit, offset)
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
