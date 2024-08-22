package channels

import (
	"broadcasts/pkg/logger"
	"database/sql"
)

// Channel represents the structure of a channel record.
type Channel struct {
	ID                  int
	ClientID            int
	ProjectID           int
	ChannelName         string
	ChannelDescription  string
	URL                 string
	Parameters          string
	Status              int
}

// ChannelsRepository represents a repository for managing channels.
type ChannelsRepository struct {
	db     *sql.DB
	logger *logger.CustomLogger
}

// NewChannelsRepository creates a new instance of ChannelsRepository.
func NewChannelsRepository(db *sql.DB, logger *logger.CustomLogger) *ChannelsRepository {
	return &ChannelsRepository{
		db:     db,
		logger: logger,
	}
}

// Fetch retrieves all channels from the database based on the provided status.
// Returns a slice of Channel structs or an empty slice if no channels are found.
func (r *ChannelsRepository) Fetch(status int) ([]Channel, error) {
	var channels []Channel
	query := `SELECT id, client_id, project_id, channel_name, channel_description, url, parameters, status
              FROM campaign_channels 
              WHERE status = ?`

	rows, err := r.db.Query(query, status)
	if err != nil {
		r.logger.Printf("Error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id, clientID, projectID, status int
			channelName, channelDescription, url, parameters sql.NullString
		)

		if err := rows.Scan(&id, &clientID, &projectID, &channelName, &channelDescription, &url, &parameters, &status); err != nil {
			r.logger.Printf("Error scanning row: %v", err)
			return nil, err
		}

		channel := Channel{
			ID:                 id,
			ClientID:           clientID,
			ProjectID:          projectID,
			ChannelName:        channelName.String,
			ChannelDescription: channelDescription.String,
			URL:                url.String,
			Parameters:         parameters.String,
			Status:             status,
		}

		channels = append(channels, channel)
	}

	if err := rows.Err(); err != nil {
		r.logger.Printf("Error after scanning rows: %v", err)
		return nil, err
	}

	return channels, nil
}
