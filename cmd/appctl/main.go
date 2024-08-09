package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	
	"broadcasts/config"
	"broadcasts/pkg/logger"
"broadcasts/pkg/database/mysql"
	"broadcasts/messages" // Update this import path according to your actual project structure
)

func main() {
	// Load environment variables
	config.LoadEnvFile()

	// Initialize the custom logger
	if err := logger.Init(); err != nil {
		fmt.Printf("Error initializing logger: %v\n", err)
		os.Exit(1)
	}

	if logger.Logger == nil {
		fmt.Println("Logger is not initialized correctly")
		os.Exit(1)
	}

	// Initialize the database connection
	if err := mysql.Init(); err != nil {
		logger.Logger.Printf("Error initializing database: %v", err)
		os.Exit(1)
	}
	defer mysql.Close()

	// Create the BroadcastChecker
	bc := messages.NewBroadcastChecker(logger.Logger, mysql.DB)

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to signal completion
	done := make(chan bool)

	// Worker pool sizes
	fetchWorkers := 1
	processWorkers := 5

	// WaitGroup to wait for all workers to finish
	var wg sync.WaitGroup
	wg.Add(fetchWorkers + processWorkers)

	// Start fetch workers
	for i := 0; i < fetchWorkers; i++ {
		go bc.Run(ctx, &wg)
	}

	// Start process workers
	for i := 0; i < processWorkers; i++ {
		go bc.ProcessBroadcasts(ctx, &wg)
	}

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(done)
	}()

	// Handle graceful shutdown
	<-done
	logger.Logger.Println("Application finished.")
}
