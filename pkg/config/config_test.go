package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_Load(t *testing.T) {
	config := GetConfig()

	if config.DataFolder != "/tmp/data-lake" {
		t.Errorf("expected /tmp/data-lake, got %s", config.DataFolder)
	}
}

func TestConfig_LoadFails(t *testing.T) {
	t.Setenv("CONFIG_FILE", "/tmp/should/not/be/there/test.yaml")
	_, err := newConfig()

	if err == nil {
		t.Error("expected a failed config load")
	}
}

func TestConfig_LoadingDefaultValues(t *testing.T) {
	t.Setenv("CONFIG_FILE", "") // reset Arrakis configuration

	config, err := newConfig()
	if err != nil {
		t.Error("error in loading default configuration")
	}

	assert.Equal(t, "/tmp/data-lake", config.DataFolder)
	assert.Equal(t, "local", config.IngestProcessorType)
	assert.Equal(t, "ingest-bucket", config.AwsBucketName)
	assert.Equal(t, "ingest-queue", config.AwsIngestQueueName)
	assert.Equal(t, "logger-queue", config.AwsLoggerQueueName)
	assert.Equal(t, "CONSOLE", config.LoggerType)
	assert.Equal(t, "INFO", config.LoggerLevel)
}
