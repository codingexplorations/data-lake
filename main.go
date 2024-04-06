package main

import (
	"os"
	"time"

	"github.com/codingexplorations/data-lake/pkg"
	"github.com/codingexplorations/data-lake/pkg/config"
	"github.com/codingexplorations/data-lake/pkg/ingest"
	"github.com/codingexplorations/data-lake/pkg/log"
)

// main function that processes a local file
func main() {
	logger := log.NewConsoleLog()

	for _, e := range os.Environ() {
		// pair := strings.SplitN(e, "=", 2)
		logger.Info(e)
	}

	conf := config.GetConfig()
	processor := ingest.GetIngestProcessor(conf)

	r := pkg.NewRunner(conf, processor)

	r.Config.Print()

	for {
		r.Run()
		time.Sleep(10 * time.Second)
	}
}
