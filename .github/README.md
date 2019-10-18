Watermill - Amazon SNS/SQS PubSub Adapter
---

#### Notes for SQS 

A new SQS Publisher will create the queue if it does not exist.

#### Notes for SNS

A new SQS Publisher will create the topic if it does not exist. Be aware that without any subsciptions the messages published to that newly created topic would be lost.  Make sure you always create the topic and subscriptions before you start an SNS publisher.

#### Unit Tests

You can run the unit tests by simply running  the command: `go test -cover -race ./...`

#### Development and Testing

To try the  in test mode (using [goaws](https://github.com/p4tin/goaws))
- start docker using: `docker-compose up -d`
- export AWS_ENDPOINT=http://localhost:4100/

Now you can run the application: `go run cmd/main.go`
