package messenger

import (
	"broadcasts/pkg/logger"
	"database/sql"
	"errors"
	"github.com/google/uuid"
)

type MessengerRepository struct {
	db     *sql.DB
	logger *logger.CustomLogger
}

func NewMessengerRepository(db *sql.DB, logger *logger.CustomLogger) *MessengerRepository {
	return &MessengerRepository{
		db:     db,
		logger: logger,
	}
}

// UpdateBroadcastListProcessedStatus updates the processed status of a broadcast list item
// only if the new status is higher than the current status.
func (r *MessengerRepository) UpdateBroadcastListProcessedStatus(broadcastID, id string, newStatus int) error {
	// Fetch the current status
	var currentStatus int
	err := r.db.QueryRow(`
		SELECT processed
		FROM broadcast_lists
		WHERE list_id = ?`, id).Scan(&currentStatus)
	if err != nil {
		if err == sql.ErrNoRows {
			// No rows found; consider it an error or handle as needed
			r.logger.Printf("No broadcast list found with broadcast_id: %s and id: %s", broadcastID, id)
			return err
		}
		r.logger.Printf("Failed to fetch current status: %v", err)
		return err
	}

	// Update only if the new status is higher than the current status
	if newStatus > currentStatus {
		_, err := r.db.Exec(`
			UPDATE broadcast_lists
			SET processed = ?
			WHERE list_id = ?`, newStatus, id)
		if err != nil {
			r.logger.Printf("Failed to update broadcast list status: %v", err)
			return err
		}
	} else {
		r.logger.Printf("No status update needed. Current status: %d, New status: %d", currentStatus, newStatus)
	}

	return nil
}

func (r *MessengerRepository) CreateOutbound(broadcastList map[string]interface{}) (int64, error) {
	parentBroadcast, ok := broadcastList["parent_broadcast"].(map[string]interface{})
	if !ok {
		r.logger.Printf("Invalid or missing parent_broadcast")
		return 0, errors.New("invalid or missing parent_broadcast")
	}

	//r.logger.Printf("Broadcast: %v", parentBroadcast)

	broadcastID, ok := parentBroadcast["broadcast_id"]
	if !ok {
		r.logger.Printf("Invalid or missing broadcast_id")
		return 0, errors.New("invalid or missing broadcast_id")
	}

	projectID, ok := parentBroadcast["project_id"]
	if !ok {
		r.logger.Printf("Invalid or missing broadcast_id")
		return 0, errors.New("invalid or missing broadcast_id")
	}

	clientID, ok := parentBroadcast["client_id"]
	if !ok {
		r.logger.Printf("Invalid or missing client_id")
		return 0, errors.New("invalid or missing client_id")
	}


	campaignChannel, ok := parentBroadcast["campaign_channel"].(map[string]interface{})
	if !ok {
		r.logger.Printf("Invalid or missing campaign_channel")
		return 0, errors.New("invalid or missing campaign_channel")
	}
	channelID, ok := campaignChannel["ID"] 
	if !ok {
		r.logger.Printf("Invalid or missing channel id")
		return 0, errors.New("invalid or missing channel id")
	}

	mobileNumber, ok := broadcastList["msisdn"]
	if !ok {
		r.logger.Printf("Invalid or missing mobile_number")
		return 0, errors.New("invalid or missing mobile_number")
	}

	content, ok := broadcastList["message_content"]
	if !ok {
		r.logger.Printf("Invalid or missing message_content")
		return 0, errors.New("invalid or missing message_content")
	}

	broadcastListId, ok := broadcastList["list_id"]
	if !ok {
		r.logger.Printf("Invalid or missing list id")
		return 0, errors.New("invalid or list id")
	}

	length, ok := broadcastList["msg_length"]
	if !ok {
		r.logger.Printf("Invalid or missing length")
		return 0, errors.New("invalid or missing length")
	}

	uuid := uuid.New().String()
	status := STATUS_PROCESSING

	query := `
		INSERT INTO outbound_old (uuid, client_id, project_id, broadcast_id,broadcast_list_id, channel_id, mobile_number, content, length, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	result, err := r.db.Exec(query, uuid, clientID, projectID, broadcastID, broadcastListId, channelID, mobileNumber, content, length, status)
	if err != nil {
		r.logger.Printf("Failed to insert outbound record: %v", err)
		return 0, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		r.logger.Printf("Failed to retrieve last insert ID: %v", err)
		return 0, err
	}

	r.logger.Printf("Outbound record created with ID: %d", id)
	return id, nil
}



func (r *MessengerRepository) UpdateOutboundStatus(id int64, newStatus int) error {

	query := `UPDATE outbound_old SET status = ? WHERE id = ?`


	_, err := r.db.Exec(query, newStatus, id)
	if err != nil {
		r.logger.Printf("Failed to update status for outbound record with ID %d: %v", id, err)
		return err
	}

	r.logger.Printf("Outbound record with ID %d updated successfully to status %d", id, newStatus)
	return nil
}
