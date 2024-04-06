#!/bin/bash
echo "########### Setting region as env variable ##########"
export AWS_REGION=us-east-1

echo "########### Setting up localstack profile ###########"
aws configure set aws_access_key_id access_key --profile=localstack
aws configure set aws_secret_access_key secret_key --profile=localstack
aws configure set region $AWS_REGION --profile=localstack

echo "########### Setting testing profile ###########"
export AWS_DEFAULT_PROFILE=localstack

# Create queues
awslocal sqs create-queue --region $AWS_REGION --queue-name $TEST_INGEST_QUEUE_NAME --attributes '{"ReceiveMessageWaitTimeSeconds": "20"}'

# Create buckets
awslocal s3 mb s3://$TEST_INGEST_BUCKET_NAME
awslocal s3api put-bucket-cors --bucket $TEST_INGEST_BUCKET_NAME --cors-configuration file:///etc/localstack/init/bucket-cors.json

# Get SQS Event ARN
INGEST_QUEUE_EVENT_SQS_ARN=$(awslocal sqs get-queue-attributes \
                              --queue-url="$(awslocal sqs get-queue-url --queue-name $TEST_INGEST_QUEUE_NAME --query 'QueueUrl' --output text)" \
                              --attribute-names 'QueueArn' \
                              --query 'Attributes.QueueArn' \
                              --output text)

# Create event configurations for the buckets
awslocal s3api put-bucket-notification-configuration --bucket $TEST_INGEST_BUCKET_NAME \
    --notification-configuration  '{
                                      "QueueConfigurations": [
                                         {
                                           "QueueArn": "'"$INGEST_QUEUE_EVENT_SQS_ARN"'",
                                           "Events": ["s3:ObjectCreated:Put"]
                                         },
                                         {
                                            "QueueArn": "'"$INGEST_QUEUE_EVENT_SQS_ARN"'",
                                            "Events": ["s3:ObjectCreated:Copy"]
                                         },
                                         {
                                           "QueueArn": "'"$INGEST_QUEUE_EVENT_SQS_ARN"'",
                                           "Events": ["s3:ObjectCreated:Post"]
                                         },
                                         {
                                            "QueueArn": "'"$INGEST_QUEUE_EVENT_SQS_ARN"'",
                                            "Events": ["s3:ObjectCreated:CompleteMultipartUpload"]
                                         }
                                       ]
                                     }'
