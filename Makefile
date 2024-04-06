cwd:=$(shell pwd)
YT:=\033[0;33m
NC:=\033[0m

.DEFAULT_GOAL := all

all: proto mocks test build

proto: ## Build protobuf models
	@echo "$(YT)Building protobuf ...$(NC)"
	buf generate

mocks: ## Generate mocks
	@echo "$(YT)Generating mocks ...$(NC)"
	mockery --all --keeptree --output ./test/mocks/ --outpkg mocks

run: proto ## Run main
	@echo "$(YT)Running main.go ...$(NC)"
	CONFIG_FILE=./.env go run main.go

test-unit: proto mocks ## Run tests
	@echo "$(YT)Running tests ...$(NC)"
	CONFIG_FILE=$(cwd)/test/configs/test.yaml go test -v -cover ./pkg/log/... ./pkg/config/... ./pkg/ingest/... ./pkg/.

test-int: proto mocks ## Run integration tests - this will start LocalStack, await healthy LocalStack container, run tests, and clean up docker containers.
	$(eval STACK_NAME:=test-int)
	@echo "$(YT)Starting compose stack '${STACK_NAME}' for integration tests ...$(NC)"
	@docker compose -p ${STACK_NAME} -f ./test/deploy/docker-compose.yml down --remove-orphans -v
	@docker compose -p ${STACK_NAME} -f ./test/deploy/docker-compose.yml up --exit-code-from integration-test-service --remove-orphans
	@docker compose -p ${STACK_NAME} -f ./test/deploy/docker-compose.yml down --remove-orphans -v
	@echo "$(YT)Tests Complete.$(NC)"

test: test-unit test-int ## Run all tests

build: ## Builds the docker image
	@echo -e "$(YT)Building data-lake docker image ...$(NC)"
	@env GOOS=linux GOARCH=amd64 go build -o ./target/data-lake main.go
	@docker build --platform linux/amd64 -t data-lake:latest -f deploy/Dockerfile .
	@docker scout quickview data-lake:latest

start: ## Start the docker stack
	@echo -e "$(YT)Running data-lake docker stack ...$(NC)"
	@docker compose -p data-lake --env-file deploy/.env -f deploy/docker-compose.yml up --remove-orphans

stop: ## Stop the docker stack
	@echo -e "$(YT)Stopping data-lake docker stack ...$(NC)"
	@docker compose -p data-lake -f deploy/docker-compose.yml down --remove-orphans

put-object:
	@echo -e "$(YT)Putting object to S3 ...$(NC)"
	@aws --endpoint-url=http://localhost:4566 s3 cp data/sample.txt s3://data-lake-ingest-bucket/app/data/sample.txt
	@aws --endpoint-url=http://localhost:4566 s3 cp data/sample-2.txt s3://data-lake-ingest-bucket/app/data/sample-2.txt

read-ingest-messages:
	@echo -e "$(YT)Reading ingest messages from SQS ...$(NC)"
	@aws --endpoint-url=http://localhost:4566 sqs receive-message --queue-url http://localhost:4566/000000000000/data-lake-ingest-queue

purge-ingest-messages:
	@echo -e "$(YT)Purging ingest messages from SQS ...$(NC)"
	@aws --endpoint-url=http://localhost:4566 sqs purge-queue --queue-url http://localhost:4566/000000000000/data-lake-ingest-queue

read-log-messages:
	@echo -e "$(YT)Reading log messages from SQS ...$(NC)"
	@aws --endpoint-url=http://localhost:4566 sqs receive-message --queue-url http://localhost:4566/000000000000/data-lake-logger-queue

purge-log-messages:
	@echo -e "$(YT)Purging log messages from SQS ...$(NC)"
	@aws --endpoint-url=http://localhost:4566 sqs purge-queue --queue-url http://localhost:4566/000000000000/data-lake-logger-queue