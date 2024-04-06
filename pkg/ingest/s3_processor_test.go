package ingest

import (
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/codingexplorations/data-lake/pkg/config"
	"github.com/codingexplorations/data-lake/pkg/log"
	mocks "github.com/codingexplorations/data-lake/test/mocks/pkg/aws"
	"github.com/stretchr/testify/assert"
)

func Test_S3Processor_NewS3IngestProcessorImpl(t *testing.T) {
	conf := config.GetConfig()
	logger := log.NewConsoleLog()

	processor := NewS3IngestProcessorImpl(conf, logger)

	assert.NotNil(t, processor)
}

func Test_S3Processor_ProcessFolder(t *testing.T) {
	conf := config.GetConfig()

	s3Client := mocks.NewS3Client(t)

	listObjectsOutput := []types.Object{
		{
			Key: aws.String("test/test1.txt"),
		},
		{
			Key: aws.String("test/test2.txt"),
		},
	}
	s3Client.On("ListObjects", conf.AwsBucketName, aws.String("test/")).Return(listObjectsOutput, nil)

	headObjectOutput := &s3.HeadObjectOutput{
		ContentType:   aws.String("text/plain"),
		ContentLength: aws.Int64(15),
	}
	s3Client.On("HeadObject", conf.AwsBucketName, "test/test1.txt").Return(headObjectOutput, nil)
	s3Client.On("HeadObject", conf.AwsBucketName, "test/test2.txt").Return(headObjectOutput, nil)

	processor := &S3IngestProcessorImpl{
		conf:     conf,
		logger:   log.NewConsoleLog(),
		s3Client: s3Client,
	}

	processedObjects, err := processor.ProcessFolder("test/")

	assert.Nil(t, err)
	assert.Equal(t, 2, len(processedObjects))
	assert.Equal(t, "test1.txt", processedObjects[0].FileName)
	assert.Equal(t, "test/test1.txt", processedObjects[0].FileLocation)
	assert.Equal(t, "text/plain", processedObjects[0].ContentType)
	assert.Equal(t, int32(15), processedObjects[0].ContentSize)
	assert.Equal(t, "test2.txt", processedObjects[1].FileName)
	assert.Equal(t, "test/test2.txt", processedObjects[1].FileLocation)
	assert.Equal(t, "text/plain", processedObjects[1].ContentType)
	assert.Equal(t, int32(15), processedObjects[1].ContentSize)
}

func Test_S3Processor_ProcessFile(t *testing.T) {
	conf := config.GetConfig()

	s3Client := mocks.NewS3Client(t)

	headObjectOutput := &s3.HeadObjectOutput{
		ContentType:   aws.String("text/plain"),
		ContentLength: aws.Int64(15),
	}
	s3Client.On("HeadObject", conf.AwsBucketName, "test/test.txt").Return(headObjectOutput, nil)

	processor := &S3IngestProcessorImpl{
		conf:     conf,
		logger:   log.NewConsoleLog(),
		s3Client: s3Client,
	}

	processedObject, err := processor.ProcessFile("test/test.txt")

	assert.Nil(t, err)
	assert.Equal(t, "test.txt", processedObject.FileName)
	assert.Equal(t, "test/test.txt", processedObject.FileLocation)
	assert.Equal(t, "text/plain", processedObject.ContentType)
	assert.Equal(t, int32(15), processedObject.ContentSize)
}
