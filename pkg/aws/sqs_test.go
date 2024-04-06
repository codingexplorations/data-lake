package aws

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSdkConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/codingexplorations/data-lake/pkg/config"
	"github.com/codingexplorations/data-lake/pkg/log"
	"github.com/stretchr/testify/assert"
)

func TestSqsClient_GetQueueUrl(t *testing.T) {
	conf := config.GetConfig()
	sqsClient, _ := NewSqs()

	result, err := sqsClient.GetQueueUrl(conf.AwsIngestQueueName)
	if err != nil {
		t.Errorf("Got an error getting the queue URL: %v", err)
		return
	}

	assert.NotNil(t, result)
	assert.Regexp(t,
		fmt.Sprintf("^http://sqs\\..*?\\.localhost.*?:4566/000000000000/%s$", conf.AwsIngestQueueName),
		*result.QueueUrl,
		"Wrong queue created",
	)
}

func TestSqsClient_GetMessages(t *testing.T) {
	conf := config.GetConfig()
	sqsClient, _ := NewSqs()

	// Get URL of queue
	result, err := sqsClient.GetQueueUrl(conf.AwsIngestQueueName)
	if err != nil {
		t.Errorf("Got an error getting the queue URL: %v", err)
		return
	}

	_, err = sendMessage(10, map[string]types.MessageAttributeValue{
		"Title": {
			DataType:    aws.String("String"),
			StringValue: aws.String("The Whistler"),
		},
		"Author": {
			DataType:    aws.String("String"),
			StringValue: aws.String("John Grisham"),
		},
		"WeeksOn": {
			DataType:    aws.String("Number"),
			StringValue: aws.String("6"),
		},
	}, "Information about current NY Times fiction bestseller for week of 12/11/2016.", result.QueueUrl)
	if err != nil {
		t.Errorf("Got an error sending the message: %v", err)
		return
	}

	msgResult, err := sqsClient.GetMessages([]string{string(types.QueueAttributeNameAll)}, result.QueueUrl, 1, 60)
	if err != nil {
		t.Errorf("Got an error receiving messages: %v", err)
		return
	}

	assert.NotNil(t, msgResult.Messages, "No messages found")
	assert.Equal(t, len(msgResult.Messages), 1)
}

func TestSqsClient_RemoveMessage(t *testing.T) {
	conf := config.GetConfig()
	sqsClient, _ := NewSqs()

	// Get URL of queue
	result, err := sqsClient.GetQueueUrl(conf.AwsIngestQueueName)
	if err != nil {
		t.Errorf("Got an error getting the queue URL: %v", err)
		return
	}

	_, err = sendMessage(10, map[string]types.MessageAttributeValue{
		"Title": {
			DataType:    aws.String("String"),
			StringValue: aws.String("The Whistler"),
		},
		"Author": {
			DataType:    aws.String("String"),
			StringValue: aws.String("John Grisham"),
		},
		"WeeksOn": {
			DataType:    aws.String("Number"),
			StringValue: aws.String("6"),
		},
	}, "Information about current NY Times fiction bestseller for week of 12/11/2016.", result.QueueUrl)
	if err != nil {
		t.Errorf("Got an error sending the message: %v", err)
		return
	}

	msgResult, err := sqsClient.GetMessages([]string{string(types.QueueAttributeNameAll)}, result.QueueUrl, 1, 60)
	if err != nil {
		t.Errorf("Got an error receiving messages: %v", err)
		return
	}

	_, err = sqsClient.RemoveMessage(result.QueueUrl, msgResult.Messages[0].ReceiptHandle)
	assert.NoError(t, err, fmt.Sprintf("Got an error deleting the message: %v", err))
}

func sendMessage(delay int32, attributes map[string]types.MessageAttributeValue, body string, queueUrl *string) (*sqs.SendMessageOutput, error) {
	cfg, err := awsSdkConfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.NewConsoleLog().Error(fmt.Sprintf("cannot load the AWS configs: %s", err))
		return nil, err
	}

	sqsClient := &Sqs{Client: sqs.NewFromConfig(cfg)}

	input := &sqs.SendMessageInput{
		DelaySeconds:      delay,
		MessageAttributes: attributes,
		MessageBody:       aws.String(body),
		QueueUrl:          queueUrl,
	}

	return sqsClient.Client.SendMessage(context.Background(), input)
}
