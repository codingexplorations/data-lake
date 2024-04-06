package aws

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/codingexplorations/data-lake/pkg/config"
	"github.com/codingexplorations/data-lake/pkg/log"
	"github.com/stretchr/testify/assert"
)

func TestS3Client_HeadObject(t *testing.T) {
	logger := log.NewConsoleLog()
	s3Client, _ := NewS3()

	conf := config.GetConfig()

	objectKey := "test/something/test.txt"

	logger.Debug(fmt.Sprintf("uploading object to bucket %v with key %v", conf.AwsBucketName, objectKey))

	putObjectOutput, err := uploadLocalObject(
		s3Client,
		conf.AwsBucketName,
		objectKey,
		"/app/test/files/ingest/test.txt",
		map[string]string{
			"key1": "value1",
		},
	)

	logger.Debug(fmt.Sprintf("putObjectOutput: %v", putObjectOutput))

	if err != nil {
		logger.Debug(fmt.Sprintf("failed to upload object: %v", err))
		t.Error("failed to upload object")
	}

	headObject, _ := s3Client.HeadObject(conf.AwsBucketName, objectKey)

	assert.NotNil(t, headObject.Metadata)
}

func TestS3Client_HeadObjectBadBucket(t *testing.T) {
	s3Client, _ := NewS3()

	conf := config.GetConfig()

	objectKey := "test/something"

	_, _ = uploadLocalObject(
		s3Client,
		conf.AwsBucketName,
		objectKey,
		"../../test/files/ingest/test.txt",
		map[string]string{
			"key1": "value1",
		},
	)

	_, err := s3Client.HeadObject("bad-bucket-metadata", objectKey)
	if err == nil {
		t.Error("successfully retrieved metadata from a bad bucket, but expected it to fail")
		return
	}

	assert.Error(t, err)
}

func uploadLocalObject(s3Client S3, bucketName string, objectKey string, fileName string, metadata map[string]string) (*s3.PutObjectOutput, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	putObjectInput := &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(objectKey),
		Body:        file,
		ContentType: aws.String("text/plain"),
		Metadata:    metadata,
	}

	return s3Client.Client.PutObject(context.TODO(), putObjectInput)
}
