# Development instructions

## Unit Tests

You can run the unit tests by simply running  the command: `go test -cover -race ./...`

## Development and Testing

To try the  in test mode (using [localstack](https://hub.docker.com/r/localstack/localstack))
- start docker using: `docker-compose up -d`

Now you can run the application: `go run cmd/main.go`