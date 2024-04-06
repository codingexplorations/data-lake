package log

import (
	"fmt"
	"log"

	"github.com/codingexplorations/data-lake/pkg/config"
	"golang.org/x/exp/slices"
)

type ConsoleLog struct{}

func NewConsoleLog() *ConsoleLog {
	return &ConsoleLog{}
}

func (logger *ConsoleLog) Error(msg string) {
	if slices.Contains([]string{"ERROR", "WARN", "INFO", "DEBUG"}, config.GetConfig().LoggerLevel) {
		file, line := getCaller(2)
		msg = fmt.Sprintf("[ERROR] %s#%d - %s", file, line, msg)
		log.Println(Red + msg + Reset)
	}
}

func (logger *ConsoleLog) Warn(msg string) {
	if slices.Contains([]string{"WARN", "INFO", "DEBUG"}, config.GetConfig().LoggerLevel) {
		file, line := getCaller(2)
		msg = fmt.Sprintf("[WARN] %s#%d - %s", file, line, msg)
		log.Println(Yellow + msg + Reset)
	}
}

func (logger *ConsoleLog) Info(msg string) {
	if slices.Contains([]string{"INFO", "DEBUG"}, config.GetConfig().LoggerLevel) {
		file, line := getCaller(2)
		msg = fmt.Sprintf("[INFO] %s#%d - %s", file, line, msg)
		log.Println(msg)
	}
}

func (logger *ConsoleLog) Debug(msg string) {
	if slices.Contains([]string{"DEBUG"}, config.GetConfig().LoggerLevel) {
		file, line := getCaller(2)
		msg = fmt.Sprintf("[DEBUG] %s#%d - %s", file, line, msg)
		log.Println(Cyan + msg + Reset)
	}
}
