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

// NewPublisher returns the publisher instance based on the desired implementation
func NewPublisher(impl gobroker.Implementation, options ...Option) Publisher {
	switch impl {
	case gobroker.RabbitMQ:
		return newRabbitMQPub(options...)
	case gobroker.Google:
		return newGooglePub(options...)
	case gobroker.NSQ:
		return newNSQPub(options...)
	default:
		return nil
	}
}
