package messages

import (
	"database/sql"
	"fmt"
)

type BroadcastRepository struct {
	db *sql.DB
}

// NewBroadcastRepository creates a new instance of BroadcastRepository.
func NewBroadcastRepository(db *sql.DB) *BroadcastRepository {
	return &BroadcastRepository{db: db}
}

// Fetch retrieves a broadcast from the database based on the status and sent time.
func (r *BroadcastRepository) Fetch(status int) (map[string]interface{}, error) {
	broadcast := make(map[string]interface{})
	query := `SELECT broadcast_id, project_id, campaign_channel, sent_time, message_content, source_list,
                     status, original_filename, generated_filename, credits_used,
                     created_by, client_id, updated_by, created_at
              FROM broadcasts
              WHERE status = ? AND sent_time <= NOW()
              LIMIT 1`

	rows, err := r.db.Query(query, status)
	if err != nil {
		return nil, fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var broadcastID, projectID, campaignChannel, sentTime, messageContent, sourceList,
			status, originalFilename, generatedFilename string
		var creditsUsed, createdBy, clientID, updatedBy int
		var createdAt string

		if err := rows.Scan(&broadcastID, &projectID, &campaignChannel, &sentTime, &messageContent, &sourceList,
			&status, &originalFilename, &generatedFilename, &creditsUsed,
			&createdBy, &clientID, &updatedBy, &createdAt); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		broadcast["broadcast_id"] = broadcastID
		broadcast["project_id"] = projectID
		broadcast["campaign_channel"] = campaignChannel
		broadcast["sent_time"] = sentTime
		broadcast["message_content"] = messageContent
		broadcast["source_list"] = sourceList
		broadcast["status"] = status
		broadcast["original_filename"] = originalFilename
		broadcast["generated_filename"] = generatedFilename
		broadcast["credits_used"] = creditsUsed
		broadcast["created_by"] = createdBy
		broadcast["client_id"] = clientID
		broadcast["updated_by"] = updatedBy
		broadcast["created_at"] = createdAt

		return broadcast, nil
	}

	return nil, nil // No broadcast found
}

// Update updates the status of a broadcast based on its broadcast_id.
func (r *BroadcastRepository) Update(broadcastID string, newStatus int) error {
	query := `UPDATE broadcasts
              SET status = ?
              WHERE broadcast_id = ?`
	_, err := r.db.Exec(query, newStatus, broadcastID)
	if err != nil {
		return fmt.Errorf("error updating broadcast status: %v", err)
	}
	return nil
}

func (r *BroadcastRepository) FetchAndUpdateBroadcast(status int, newStatus int) (map[string]interface{}, error) {
	broadcast, err := r.Fetch(status)
	if err != nil {
		return nil, err
	}
	if broadcast == nil {
		return nil, nil // No broadcast found
	}

	broadcastID, ok := broadcast["broadcast_id"].(string)
	if !ok {
		return nil, fmt.Errorf("broadcast_id is missing or not a string")
	}

	err = r.Update(broadcastID, newStatus)
	if err != nil {
		return nil, err
	}

	return broadcast, nil
}