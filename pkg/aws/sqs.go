package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSdkConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/codingexplorations/data-lake/pkg/log"
)

type SqsClient interface {
	GetQueueUrl(queueName string) (*sqs.GetQueueUrlOutput, error)
	GetMessages(attributeNames []string, queueURL *string, maxMessages int32, timeout int32) (*sqs.ReceiveMessageOutput, error)
	RemoveMessage(queueURL *string, messageHandle *string) (*sqs.DeleteMessageOutput, error)
}

type Sqs struct {
	Client *sqs.Client
}

func NewSqs() (SqsClient, error) {
	cfg, err := awsSdkConfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.NewConsoleLog().Error(fmt.Sprintf("cannot load the AWS configs: %s", err))
		return Sqs{}, err
	}

	return &Sqs{Client: sqs.NewFromConfig(cfg)}, nil
}

// GetQueueUrl gets the URL of an Amazon SQS queue.
func (client Sqs) GetQueueUrl(queueName string) (*sqs.GetQueueUrlOutput, error) {
	qUInput := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}

	return client.Client.GetQueueUrl(context.TODO(), qUInput)
}

// GetMessages gets the most recent message from an Amazon SQS queue.
func (client Sqs) GetMessages(attributeNames []string, queueURL *string, maxMessages int32, timeout int32) (*sqs.ReceiveMessageOutput, error) {
	input := &sqs.ReceiveMessageInput{
		MessageAttributeNames: attributeNames,
		QueueUrl:              queueURL,
		MaxNumberOfMessages:   maxMessages,
		VisibilityTimeout:     timeout,
	}

	return client.Client.ReceiveMessage(context.TODO(), input)
}

// RemoveMessage deletes a message from an Amazon SQS queue.
func (client Sqs) RemoveMessage(queueURL *string, messageHandle *string) (*sqs.DeleteMessageOutput, error) {
	input := &sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: messageHandle,
	}

	return client.Client.DeleteMessage(context.TODO(), input)
}
