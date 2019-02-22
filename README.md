[![Build Status](https://travis-ci.com/febytanzil/gobroker.svg?branch=master)](https://travis-ci.com/febytanzil/gobroker)
[![GitHub release](https://img.shields.io/github/release/febytanzil/gobroker.svg)](https://GitHub.com/febytanzil/gobroker/releases/)
[![GitHub tag](https://img.shields.io/github/tag/febytanzil/gobroker.svg)](https://GitHub.com/febytanzil/gobroker/tags/)
[![GitHub license](https://img.shields.io/github/license/febytanzil/gobroker.svg)](https://github.com/febytanzil/gobroker/blob/master/LICENSE)
# gobroker
wrapper for all (to-be) kinds of message brokers.

## Supported message brokers & patterns
### PubSub
- RabbitMQ (*fanout*)
- Google Cloud Pub/Sub (go v1.9.x or later)

## Intentions & Features
- Generic terms & functions to use message brokers
- Auto reconnection
- Limit & requeue messages*
- Concurrent subscribers
- Support for mockgen unit-testing

## Install
```bash
# go get
$ go get github.com/febytanzil/gobroker

# dep
$ dep ensure -add github.com/febytanzil/gobroker
```

## Usage
Complete examples are provided in `examples` folder/ package
### RabbitMQ
```go
// initialize publisher RabbitMQ
p := pubsub.NewPublisher(gobroker.RabbitMQ, pubsub.RabbitMQAMPQ("amqp://guest:guest@localhost:5672/", "vhost"))

p.Publish("test.fanout", "msg"+t.String())
```
```go
// register RabbitMQ subscriber(s) & run it
s := pubsub.NewSubscriber(gobroker.RabbitMQ, []*pubsub.SubHandler{
    {
        Name:       "test",
        Topic:      "test.fanout",
        Handler:    testRMQ,
        MaxRequeue: 10,
    },
}, pubsub.RabbitMQAMPQ("amqp://guest:guest@localhost:5672/", "vhost"))

s.Start()
```
### Google
```go
// initialize publisher Google
p := pubsub.NewPublisher(gobroker.Google, pubsub.GoogleJSONFile("gcp-project-id", "/path/to/google/application/credentials/cred.json"))

p.Publish("test", "msg"+t.String())
```
```go
// register Google subscriber(s) & run it
s := pubsub.NewSubscriber(gobroker.Google, []*pubsub.SubHandler{
		{
			Name:       "consumer-test",
			Topic:      "test",
			Handler:    testGoogle,
			MaxRequeue: 10,
		},
	},
		pubsub.GoogleJSONFile("gcp-project-id", "/path/to/google/application/credentials/cred.json"))
		
s.Start()
```
### Creating subcriber/ consumer
```go
// subcriber function format
// return nil will ack the message as success
// return error will requeue based on config

func testRMQ(msg *gobroker.Message) error {
	log.Println("consume rabbitmq", string(msg.Body))
	return nil
}
func testGoogle(msg *gobroker.Message) error {
	log.Println("consume google pubsub", string(msg.Body))
	return errors.New("requeue msg")
}
```

## Notes
Due to requeue limiter, the behavior both in RabbitMQ & Google Pub/Sub is changed to republish to the topic with additional header that contains counter to make this possible

## Contributing
Please use a fork to create a pull request

## Contributors
- [ichsanrp](https://github.com/ichsanrp)
- [jonathanhaposan](https://github.com/jonathanhaposan)