package mysql

import (
	"database/sql"
	"fmt"
	"time"

	"broadcasts/config"
	"broadcasts/pkg/logger"

	_ "github.com/go-sql-driver/mysql"
)

var (
	// DB is the global database connection pool
	DB *sql.DB
	// ErrDB indicates if the database connection has encountered an error
	ErrDB error
)

// Init initializes the database connection
func Init() error {
	if logger.Logger == nil {
		return fmt.Errorf("logger is not initialized")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		config.GetEnv("DB_USERNAME", ""),
		config.GetEnv("DB_PASSWORD", ""),
		config.GetEnv("DB_HOST", "127.0.0.1"),
		config.GetEnv("DB_DATABASE", ""),
	)

	// Open a connection to the database
	DB, ErrDB = sql.Open("mysql", dsn)
	if ErrDB != nil {
		logger.Logger.Printf("error connecting to database: %v", ErrDB)
		return fmt.Errorf("error connecting to database: %w", ErrDB)
	}

	// Set connection pool settings
	DB.SetMaxOpenConns(10)
	DB.SetMaxIdleConns(10)
	DB.SetConnMaxLifetime(30 * time.Minute)

	// Test the connection
	if ErrDB = DB.Ping(); ErrDB != nil {
		logger.Logger.Printf("error pinging database: %v", ErrDB)
		return fmt.Errorf("error pinging database: %w", ErrDB)
	}

	logger.Logger.Println("Database connection initialized successfully")
	return nil
}

// Close closes the database connection
func Close() {
	if err := DB.Close(); err != nil {
		logger.Logger.Printf("error closing database: %v", err)
	} else {
		logger.Logger.Println("Database connection closed successfully")
	}
}

// Query executes a SQL query and logs its execution time
func Query(query string, args ...interface{}) (*sql.Rows, error) {

	logger.Logger.Println(query)
	rows, err := DB.Query(query, args...)
	if err != nil {
		logger.Logger.Printf("error executing query: %v", err)
		return nil, err
	}



	return rows, nil
}

// Exec executes a SQL statement and logs its execution time
func Exec(query string, args ...interface{}) (sql.Result, error) {

	logger.Logger.Println(query)
	result, err := DB.Exec(query, args...)
	if err != nil {
		logger.Logger.Printf("error executing statement: %v", err)
		return nil, err
	}

	return result, nil
}
