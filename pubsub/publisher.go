package pubsub

import (
	"github.com/febytanzil/gobroker"
)

// Publisher provides adapter to publish message
type Publisher interface {
	// Publish publishes a message to a worker pool and might block for broker ack.
	Publish(topic string, message interface{}) error
	// PublishAsync publishes a message to a worker pool without waiting for broker ack.
	PublishAsync(topic string, message interface{}) Future
}

type Future interface {
	// Wait waits the broker to ack, will return error from the broker after sent the message if any.
	Wait() error
}

// NewPublisher implements adapter instance for Publisher
func NewPublisher(impl gobroker.Implementation, options ...Option) Publisher {
	c := &config{}
	for _, o := range options {
		o(c)
	}
	if nil == c.codec {
		c.codec = gobroker.StdJSONCodec
		c.contentType = "application/json"
	}

	switch impl {
	case gobroker.RabbitMQ:
		return newRabbitMQPub(c)
	case gobroker.Google:
		return newGooglePub(c)
	case gobroker.NSQ:
		return newNSQPub(c)
	default:
		return nil
	}
}
