package messenger

import (
	"broadcasts/pkg/logger"
	"database/sql"
	"errors"
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

	r.logger.Printf("Broadcast:VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV %v", parentBroadcast)

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


	campaignChannel, ok := parentBroadcast["campaign_channel"]
	if !ok {
		r.logger.Printf("Invalid or missing campaign_channel")
		return 0, errors.New("invalid or missing campaign_channel")
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
	query :=`
		INSERT ignore INTO outbound (message_id, sourceAddress, MSISDN, client_id,project_id, message_source, messageRoute,lastSend, firstSend,priority,status, statusMessage, created_at, updatedBy, dateModified,message_content) VALUES (?,?,?,?,?, 'BROADCAST', 'BULK', now(), now(),1, 32, 'SentToNetwork',now(),1,now(),?)
`
	result, err := r.db.Exec(query,broadcastID,campaignChannel,mobileNumber, clientID, projectID,content)
	if err != nil {
		r.logger.Printf("Failed to insert outbound record: %v", err)
		return 0, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		r.logger.Printf("Failed to retrieve last insert ID: %v", err)
		return 0, err
	}
if(id==0){
  r.logger.Printf("Failed to retrieve last insert ID: %v", err)
                return 0, errors.New("value is zero, an error occurred")
}
	r.logger.Printf("Outbound record created with ID: %d", id)
	return id, nil
}



func (r *MessengerRepository) UpdateOutboundStatus(id int64, newStatus int) error {

	query := `UPDATE outbound SET status = ? WHERE id = ?`


	_, err := r.db.Exec(query, newStatus, id)
	if err != nil {
		r.logger.Printf("Failed to update status for outbound record with ID %d: %v", id, err)
		return err
	}

	r.logger.Printf("Outbound record with ID %d updated successfully to status %d", id, newStatus)
	return nil
}
