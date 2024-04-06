package log

import (
	"log"
	"runtime"
	"strings"
	"sync"

	"github.com/codingexplorations/data-lake/pkg/config"
)

var Reset = "\033[0m"
var Red = "\033[31m"
var Yellow = "\033[33m"
var Cyan = "\033[36m"

type Logger interface {
	Error(msg string)
	Warn(msg string)
	Info(msg string)
	Debug(msg string)
}

var loggerLock = &sync.Mutex{}

var loggerInstance Logger

func GetLogger() (Logger, error) {
	if loggerInstance == nil {
		loggerLock.Lock()
		defer loggerLock.Unlock()
		if loggerInstance == nil {
			config := config.GetConfig()
			switch config.LoggerType {
			case "SERVICE":
				serviceLog, err := NewSqsLog()
				if err != nil {
					log.Println("failed to create service log instance")
					return nil, err
				}
				loggerInstance = serviceLog
			case "CONSOLE":
			default:
				loggerInstance = NewConsoleLog()
			}
		}
	}

	return loggerInstance, nil
}

func getCaller(skip int) (string, int32) {
	_, file, line, ok := runtime.Caller(skip)

	if ok {
		callerSplit := strings.Split(file, "/")

		// get the last two elements in the file path on the / split - Go splice range functionality
		lastTwoFilePaths := callerSplit[len(callerSplit)-2:]

		shortFile := strings.Join(lastTwoFilePaths, "/")

		return shortFile, int32(line)
	}

	return "", int32(line)
}
