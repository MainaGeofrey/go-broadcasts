package logger

import (
    "fmt"
    "log"
    "os"
    "path/filepath"
    "time"
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

    Logger = &CustomLogger{log.New(logFile, "", log.Lmsgprefix)}
    return nil
}

func (cl *CustomLogger) Printf(format string, args ...interface{}) {
    logMsg := fmt.Sprintf(format, args...)
    timestamp := time.Now().Format("2006/01/02 15:04:05.000000")
    cl.Logger.Output(2, fmt.Sprintf("%s %s", timestamp, logMsg))
}

func (cl *CustomLogger) Print(v ...interface{}) {
    cl.Printf(fmt.Sprint(v...))
}

func (cl *CustomLogger) Println(v ...interface{}) {
    cl.Printf(fmt.Sprintln(v...))
}

func (cl *CustomLogger) Fatalf(format string, args ...interface{}) {
    cl.Printf(format, args...)
    os.Exit(1)
}

func (cl *CustomLogger) Fatalln(v ...interface{}) {
    cl.Println(v...)
    os.Exit(1)
}

func (cl *CustomLogger) Fatal(v ...interface{}) {
    cl.Print(v...)
    os.Exit(1)
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
