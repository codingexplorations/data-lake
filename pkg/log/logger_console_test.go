package log

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
)

func TestConsoleLog_Error(t *testing.T) {
	var buf bytes.Buffer

	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	logger := NewConsoleLog()

	logger.Error("testing ERROR")

	if !strings.Contains(buf.String(), "testing ERROR") {
		t.Error("Failed to output ERROR log.")
	}
}

func TestConsoleLog_Warn(t *testing.T) {
	var buf bytes.Buffer

	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	logger := NewConsoleLog()

	logger.Warn("testing WARN")

	if !strings.Contains(buf.String(), "testing WARN") {
		t.Error("Failed to output WARN log.")
	}
}

func TestConsoleLog_Info(t *testing.T) {
	var buf bytes.Buffer

	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	logger := NewConsoleLog()

	logger.Info("testing INFO")

	if !strings.Contains(buf.String(), "testing INFO") {
		t.Error("Failed to output INFO log.")
	}
}

func TestConsoleLog_Debug(t *testing.T) {
	var buf bytes.Buffer

	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	logger := NewConsoleLog()

	logger.Debug("testing DEBUG")

	if !strings.Contains(buf.String(), "testing DEBUG") {
		t.Error("Failed to output DEBUG log.")
	}
}
