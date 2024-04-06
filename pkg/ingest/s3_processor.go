package ingest

import (
	"fmt"
	golog "log"
	"strings"

	models_v1 "github.com/codingexplorations/data-lake/models/v1"
	"github.com/codingexplorations/data-lake/pkg/aws"
	"github.com/codingexplorations/data-lake/pkg/config"
	"github.com/codingexplorations/data-lake/pkg/log"
)

type S3IngestProcessorImpl struct {
	conf     *config.Config
	logger   log.Logger
	s3Client aws.S3Client
}

func NewS3IngestProcessorImpl(conf *config.Config, logger log.Logger) *S3IngestProcessorImpl {
	logger.Info("Using S3 ingest processor")

	s3Client, err := aws.NewS3()
	if err != nil {
		logger.Error(fmt.Sprintf("couldn't create s3 client: %v\n", err))
		return nil
	}

	return &S3IngestProcessorImpl{
		conf:     conf,
		logger:   logger,
		s3Client: &s3Client,
	}
}

// ProcessFolder processes the file
func (processor *S3IngestProcessorImpl) ProcessFolder(prefix string) ([]*models_v1.Object, error) {
	golog.Println("Processing folder: ", prefix)
	objects, err := processor.s3Client.ListObjects(processor.conf.AwsBucketName, &prefix)
	if err != nil {
		processor.logger.Error(fmt.Sprintf("couldn't list objects in bucket %v.\n", processor.conf.AwsBucketName))
		return nil, err
	}

	processedObjects := make([]*models_v1.Object, 0)

	for _, object := range objects {
		if processedFile, err := processor.ProcessFile(*object.Key); err != nil {
			return nil, err
		} else {
			processor.logger.Info(fmt.Sprintf("processed file: %v\n", processedFile))
			processedObjects = append(processedObjects, processedFile)
		}
	}

	return processedObjects, nil
}

// ProcessFile processes the file
func (processor *S3IngestProcessorImpl) ProcessFile(key string) (*models_v1.Object, error) {
	headObject, err := processor.s3Client.HeadObject(processor.conf.AwsBucketName, key)
	if err != nil {
		processor.logger.Error(fmt.Sprintf("couldn't get object %v in bucket %v.\n", key, processor.conf.AwsBucketName))
		return nil, err
	}

	pathSplit := strings.Split(key, "/")

	object := &models_v1.Object{
		FileName:     pathSplit[len(pathSplit)-1],
		FileLocation: key,
		ContentType:  *headObject.ContentType,
		ContentSize:  int32(*headObject.ContentLength),
	}

	valid, err := validate(object)
	if err != nil {
		processor.logger.Error(fmt.Sprintf("error validating object: %v\n", err))
		return nil, err
	}

	if !valid {
		processor.logger.Error(fmt.Sprintf("object is invalid: %v\n", object))
		return nil, nil
	}

	return object, nil
}
