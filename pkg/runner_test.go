package pkg

import (
	"testing"

	models_v1 "github.com/codingexplorations/data-lake/models/v1"
	"github.com/codingexplorations/data-lake/pkg/config"
	mocks "github.com/codingexplorations/data-lake/test/mocks/pkg/ingest"
)

func TestRunner(t *testing.T) {
	conf := config.GetConfig()
	processor := mocks.NewIngestProcessor(t)

	processor.On("ProcessFolder", "/tmp/data-lake").Return([]*models_v1.Object{}, nil)

	NewRunner(conf, processor).Run()
}
