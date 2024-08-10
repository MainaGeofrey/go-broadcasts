package messenger

import (
	"broadcasts/pkg/logger"
	"database/sql"
)

// MessengerRepository interacts with the database for messenger-related operations.
type MessengerRepository struct {
	db     *sql.DB
	logger *logger.CustomLogger
}

// NewMessengerRepository creates a new instance of MessengerRepository.
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
		WHERE broadcast_id = ? AND id = ?`, broadcastID, id).Scan(&currentStatus)
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
			WHERE broadcast_id = ? AND id = ?`, newStatus, broadcastID, id)
		if err != nil {
			r.logger.Printf("Failed to update broadcast list status: %v", err)
			return err
		}
	} else {
		r.logger.Printf("No status update needed. Current status: %d, New status: %d", currentStatus, newStatus)
	}

	return nil
}
