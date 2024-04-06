package log

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSdkConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	modelsv1 "github.com/codingexplorations/data-lake/models/v1"
	"github.com/codingexplorations/data-lake/pkg/config"
	"golang.org/x/exp/slices"
)

type SqsLogger interface {
	sendLogMessage(msg string, logLevel modelsv1.Log_LogLevel)
}

type SqsLog struct {
	Logger
	SqsLogger

	Sqs      LoggerSqsClient
	QueueUrl *string
}

func NewSqsLog() (*SqsLog, error) {
	sqs, err := NewLoggerSqs()
	if err != nil {
		log.Println("failed to create an SQS client.")
		return nil, err
	}

	respQueueUrl, err := sqs.GetQueueUrl(config.GetConfig().AwsLoggerQueueName)
	if err != nil {
		log.Println("failed to retrieve the logger-service queue url from SQS service")
		return nil, err
	}

	sqsLog := &SqsLog{
		Sqs:      sqs,
		QueueUrl: respQueueUrl.QueueUrl,
	}

	return sqsLog, nil
}

func (logger *SqsLog) Error(msg string) {
	if slices.Contains([]string{"ERROR", "WARN", "INFO", "DEBUG"}, config.GetConfig().LoggerLevel) {
		logger.sendLogMessage(msg, modelsv1.Log_ERROR)
	}
}

func (logger *SqsLog) Warn(msg string) {
	if slices.Contains([]string{"WARN", "INFO", "DEBUG"}, config.GetConfig().LoggerLevel) {
		logger.sendLogMessage(msg, modelsv1.Log_WARNING)
	}
}

func (logger *SqsLog) Info(msg string) {
	if slices.Contains([]string{"INFO", "DEBUG"}, config.GetConfig().LoggerLevel) {
		logger.sendLogMessage(msg, modelsv1.Log_INFO)
	}
}

func (logger *SqsLog) Debug(msg string) {
	if slices.Contains([]string{"DEBUG"}, config.GetConfig().LoggerLevel) {
		logger.sendLogMessage(msg, modelsv1.Log_DEBUG)
	}
}

type LoggerSqsClient interface {
	GetQueueUrl(queueName string) (*sqs.GetQueueUrlOutput, error)
	SendMessage(delay int32, attributes map[string]types.MessageAttributeValue, body string, queueUrl *string) (*sqs.SendMessageOutput, error)
}

type LoggerSqs struct {
	Client *sqs.Client
}

func NewLoggerSqs() (LoggerSqsClient, error) {
	cfg, err := awsSdkConfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Printf("cannot load the AWS configs: %s", err)
		return LoggerSqs{}, err
	}

	c := sqs.NewFromConfig(cfg)

	sqsClient := &LoggerSqs{
		Client: c,
	}

	return sqsClient, nil
}

// GetQueueUrl gets the URL of an Amazon SQS queue.
func (client LoggerSqs) GetQueueUrl(queueName string) (*sqs.GetQueueUrlOutput, error) {
	qUInput := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}

	return client.Client.GetQueueUrl(context.TODO(), qUInput)
}

// SendMessage sends a message to an Amazon SQS queue.
func (client LoggerSqs) SendMessage(delay int32, attributes map[string]types.MessageAttributeValue, body string, queueUrl *string) (*sqs.SendMessageOutput, error) {
	input := &sqs.SendMessageInput{
		DelaySeconds:      delay,
		MessageAttributes: attributes,
		MessageBody:       aws.String(body),
		QueueUrl:          queueUrl,
	}

	return client.Client.SendMessage(context.Background(), input)
}

func (logger *SqsLog) sendLogMessage(msg string, logLevel modelsv1.Log_LogLevel) {
	file, line := getCaller(3)

	l := &modelsv1.Log{
		Timestamp: time.Now().UnixMilli(),
		Level:     logLevel,
		File:      file,
		Line:      line,
		Message:   msg,
	}

	jsonObject, err := json.Marshal(l)
	if err != nil {
		log.Printf("failed to parse object to JSON - %v", l)
		log.Printf("error: %v", err)
		log.Printf("message: %s", msg)
		return
	}

	_, err = logger.Sqs.SendMessage(
		0,
		map[string]types.MessageAttributeValue{},
		string(jsonObject),
		logger.QueueUrl,
	)
	if err != nil {
		log.Printf("failed to send a message to %s", *logger.QueueUrl)
		log.Printf("error: %v", err)
		log.Printf("message: %s", msg)
	}
}
