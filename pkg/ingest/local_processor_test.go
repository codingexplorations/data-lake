package ingest

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFolderIngest_ProcessFolder_CheckDepth(t *testing.T) {
	tests := []struct {
		name     string
		folder   string
		location string
	}{
		{
			name:     "directory",
			folder:   "/../../test/files",
			location: "/ingest/test.txt",
		},
		{
			name:     "file",
			folder:   "/../../test/files/ingest",
			location: "/test.txt",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			processor := &LocalIngestProcessorImpl{}

			pwd, _ := os.Getwd()

			processedObjects, err := processor.ProcessFolder(pwd + tc.folder)

			assert.Nil(t, err)
			assert.Len(t, processedObjects, 1)
			assert.Equal(t, "test.txt", processedObjects[0].FileName)
			assert.Equal(t, pwd+tc.folder+tc.location, processedObjects[0].FileLocation)
			assert.Equal(t, "text/plain", processedObjects[0].ContentType)
			assert.Equal(t, int32(15), processedObjects[0].ContentSize)
		})
	}
}

func TestFolderIngest_ProcessFile_Success(t *testing.T) {
	processor := &LocalIngestProcessorImpl{}

	pwd, _ := os.Getwd()

	fileName := pwd + "/../../test/files/ingest/test.txt"

	processedObject, err := processor.ProcessFile(fileName)

	assert.Nil(t, err)
	assert.Equal(t, "test.txt", processedObject.FileName)
	assert.Equal(t, fileName, processedObject.FileLocation)
	assert.Equal(t, "text/plain", processedObject.ContentType)
	assert.Equal(t, int32(15), processedObject.ContentSize)
}

func TestFolderIngest_ProcessFolder_Failure(t *testing.T) {
	processor := &LocalIngestProcessorImpl{}

	pwd, _ := os.Getwd()

	folder := pwd + "/../../test/files/missing"

	processedObject, err := processor.ProcessFolder(folder)

	assert.Error(t, err)
	assert.Equal(t, "open /Users/benjaminparrish/Development/CodingExplorations/data-lake/pkg/ingest/../../test/files/missing: no such file or directory", err.Error())
	assert.Nil(t, processedObject)
}

func TestFolderIngest_ProcessFile_Failure(t *testing.T) {
	processor := &LocalIngestProcessorImpl{}

	pwd, _ := os.Getwd()

	fileName := pwd + "/../../test/files/ingest/missing.txt"

	processedObject, err := processor.ProcessFile(fileName)

	assert.Error(t, err)
	assert.Nil(t, processedObject)
}
