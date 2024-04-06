package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSdkConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/codingexplorations/data-lake/pkg/log"
)

type S3Client interface {
	ListObjects(bucketName string, prefix *string) ([]types.Object, error)
	HeadObject(bucketName string, objectKey string) (*s3.HeadObjectOutput, error)
}

type S3 struct {
	Client *s3.Client
}

func NewS3() (S3, error) {
	cfg, err := awsSdkConfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		return S3{}, err
	}

	c := s3.NewFromConfig(cfg)

	s3Client := S3{
		Client: c,
	}

	return s3Client, nil
}

// ListObjects lists the objects in a bucket.
func (client *S3) ListObjects(bucketName string, prefix *string) ([]types.Object, error) {
	config := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}
	if prefix != nil {
		config.Prefix = prefix
	}

	result, err := client.Client.ListObjectsV2(context.TODO(), config)

	var contents []types.Object
	if err != nil {
		log.NewConsoleLog().Error(fmt.Sprintf("couldn't list objects in bucket %v.\n", bucketName))
	} else {
		contents = result.Contents
	}

	return contents, err
}

func (client *S3) HeadObject(bucket, key string) (*s3.HeadObjectOutput, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := client.Client.HeadObject(context.TODO(), input)

	if err != nil {
		return nil, err
	}

	return result, nil
}
