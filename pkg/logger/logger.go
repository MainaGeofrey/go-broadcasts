package logger

import (
    "log"
    "os"
    "path/filepath"
)

type CustomLogger struct {
    *log.Logger
}


var Logger *CustomLogger


func Init() error {
    rootDir, err := getProjectRoot()
    if err != nil {
        return err
    }

    logDir := filepath.Join(rootDir, "storage", "logs")

    if err := os.MkdirAll(logDir, os.ModePerm); err != nil {
        return err
    }

    logFilePath := filepath.Join(logDir, "application.log")
    logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
    if err != nil {
        return err
    }

    Logger = &CustomLogger{log.New(logFile, "", log.LstdFlags)}
    return nil
}

func getProjectRoot() (string, error) {
    // Start with the current working directory
    cwd, err := os.Getwd()
    if err != nil {
        return "", err
    }

    // Traverse upwards until you find a known root file or directory
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

// fileExists checks if a file exists at the given path
func fileExists(path string) bool {
    _, err := os.Stat(path)
    return !os.IsNotExist(err)
}
