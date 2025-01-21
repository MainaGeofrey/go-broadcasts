package logger

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type CustomLogger struct {
	*log.Logger
	fatalLogger *log.Logger
	logChannel  chan string
	wg          sync.WaitGroup
	lastLogged  map[string]time.Time
	rateLimit   time.Duration
	done        chan struct{}
}

var Logger *CustomLogger

func Init(rateLimit time.Duration) error {
	rootDir, err := getProjectRoot()
	if err != nil {
		return err
	}

	logDir := filepath.Join(rootDir, "storage", "logs")

	if err := os.MkdirAll(logDir, os.ModePerm); err != nil {
		return err
	}

	// Create application log file
	logFilePath := filepath.Join(logDir, "application.log")
	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	// Create fatal log file
	fatalLogFilePath := filepath.Join(logDir, "fatal.log")
	fatalLogFile, err := os.OpenFile(fatalLogFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	logger := &CustomLogger{
		Logger:      log.New(logFile, "", log.Lmsgprefix),
		fatalLogger: log.New(fatalLogFile, "", log.Lmsgprefix),
		logChannel:  make(chan string, 100), // Buffer size can be adjusted
		lastLogged:  make(map[string]time.Time),
		rateLimit:   rateLimit,
		done:        make(chan struct{}),
	}

	Logger = logger
	logger.startLoggingWorker()
	return nil
}

func (cl *CustomLogger) startLoggingWorker() {
	cl.wg.Add(1)
	go func() {
		defer cl.wg.Done()
		for {
			select {
			case msg := <-cl.logChannel:
				timestamp := time.Now().Format("2006/01/02 15:04:05.000000")
				cl.Logger.Output(2, fmt.Sprintf("%s %s", timestamp, msg))
			case <-cl.done:
				return
			}
		}
	}()
}

func (cl *CustomLogger) Stop() {
	close(cl.done)
	cl.wg.Wait()
}

func (cl *CustomLogger) Printf(format string, args ...interface{}) {
	cl.logChannel <- fmt.Sprintf(format, args...)
}

func (cl *CustomLogger) Print(v ...interface{}) {
	cl.logChannel <- fmt.Sprint(v...)
}

func (cl *CustomLogger) Println(v ...interface{}) {
	cl.logChannel <- fmt.Sprintln(v...)
}

func (cl *CustomLogger) Fatalf(format string, args ...interface{}) {
	cl.logToFatal(fmt.Sprintf(format, args...))
	os.Exit(1)
}

func (cl *CustomLogger) Fatalln(v ...interface{}) {
	cl.logToFatal(fmt.Sprintln(v...))
	os.Exit(1)
}

func (cl *CustomLogger) Fatal(v ...interface{}) {
	cl.logToFatal(fmt.Sprint(v...))
	os.Exit(1)
}

// logToFatal handles logging of fatal errors to the fatal log file
func (cl *CustomLogger) logToFatal(message string) {
	timestamp := time.Now().Format("2006/01/02 15:04:05.000000")
	cl.fatalLogger.Output(2, fmt.Sprintf("%s %s", timestamp, message))
}

func getProjectRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		if fileExists(filepath.Join(cwd, "go.mod")) {
			return cwd, nil
		}

		parent := filepath.Dir(cwd)
		if parent == cwd {
			break
		}
		cwd = parent
	}

	return "", os.ErrNotExist
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
