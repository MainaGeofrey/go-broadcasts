package channels

import (
	"broadcasts/pkg/logger"
	"database/sql"
)

type ChannelsRepository struct {
	db     *sql.DB
	logger *logger.CustomLogger
}

func NewChannelsRepository(db *sql.DB, logger *logger.CustomLogger) *ChannelsRepository {
	return &ChannelsRepository{
		db:     db,
		logger: logger,
	}
}


func (r *ChannelsRepository) Fetch(status int) (map[string]interface{}, error) {
	channel := make(map[string]interface{})
	query := `SELECT id, client_id, project_id, channel_name, channel_description, url, parameters,status
              FROM campaign_channels 
              WHERE status = ? 
              LIMIT 1`

	rows, err := r.db.Query(query, status)
	if err != nil {
		r.logger.Printf("Error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		var id, clientID, projectID, status int
		var uuid, channelName, channelDescription, url, parameters, createdBy, updatedBy sql.NullString
		var createdAt, updatedAt sql.NullTime

		if err := rows.Scan(&id, &uuid, &clientID, &projectID, &channelName, &channelDescription, &url,
			&parameters, &status, &createdBy, &updatedBy, &createdAt, &updatedAt); err != nil {
			r.logger.Printf("Error scanning row: %v", err)
			return nil, err
		}

		channel["id"] = id
		channel["client_id"] = clientID
		channel["project_id"] = projectID
		channel["channel_name"] = channelName.String
		channel["channel_description"] = channelDescription.String
		channel["url"] = url.String
		channel["parameters"] = parameters.String
		channel["status"] = status
	} else {
		return nil, nil 
	}

	return channel, nil
}
