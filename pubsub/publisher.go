package pubsub

import "github.com/febytanzil/gobroker"

type Publisher interface {
	Publish(topic string, message interface{}) error
}

type PubConfig struct {
	// ServerURL accepts a string to specify message broker server
	// RabbitMQ - AMQP URI format
	// Google - Unused
	ServerURL string

	// VHost specifies pubsub namespace
	// RabbitMQ - Virtual Host
	// Google - ProjectID
	VHost string
}

func NewPublisher(impl gobroker.Implementation, cfg *PubConfig) Publisher {
	switch impl {
	case gobroker.RabbitMQ:
		return newRabbitMQPub(cfg)
	case gobroker.Google:
		return newGooglePub(cfg)
	default:
		return nil
	}
}
