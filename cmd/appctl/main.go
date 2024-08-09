package main

import (
	"fmt"
	"os"

	"broadcasts/config"
	"broadcasts/pkg/database/mysql"
	"broadcasts/pkg/logger"
)

func main() {
	config.LoadEnvFile()

	if err := logger.Init(); err != nil {
		fmt.Printf("Error initializing logger: %v\n", err)
		os.Exit(1)
	}

	if logger.Logger == nil {
		fmt.Println("Logger is not initialized correctly")
		os.Exit(1)
	}

	if err := mysql.Init(); err != nil {
		logger.Logger.Printf("Error initializing database: %v", err)
		os.Exit(1)
	}

	// In defer we leave Close to make sure that if some panic occurs, the connection will return to the pool.
	defer mysql.Close()

	// Fetch data from the broadcasts table
	logger.Logger.Println("Fetching data from broadcasts table")
	fetchDataFromTable("broadcasts")

	// Fetch data from the broadcast_list table
	logger.Logger.Println("Fetching data from broadcast_lists table")
	fetchDataFromTable("broadcast_lists")
}

// fetchDataFromTable queries the specified table and logs the results
func fetchDataFromTable(tableName string) {
	logger.Logger.Printf("Fetching data from table: %s", tableName)
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := mysql.Query(query)
	if err != nil {
		logger.Logger.Printf("Error querying database (%s): %v", tableName, err)
		return
	}
	defer rows.Close()

	// Fetch column names
	columns, err := rows.Columns()
	if err != nil {
		logger.Logger.Printf("Error fetching columns for table %s: %v", tableName, err)
		return
	}

	// Prepare a slice to hold the row data
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Process each row
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			logger.Logger.Printf("Error scanning row for table %s: %v", tableName, err)
			continue
		}

		// Log the row data
		rowData := make([]interface{}, len(columns))
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				rowData[i] = string(b)
			} else {
				rowData[i] = v
			}
		}
		logger.Logger.Printf("Data from table %s: %v", tableName, rowData)
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		logger.Logger.Printf("Error iterating over rows for table %s: %v", tableName, err)
	}
}
