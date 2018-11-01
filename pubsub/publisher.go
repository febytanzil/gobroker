package pubsub

import "github.com/febytanzil/gobroker"

type Publisher interface {
	Publish(topic string, message interface{}) error
}

type PubConfig struct {
	ServerURL string
	VHost     string
}

func NewPublisher(impl gobroker.Implementation, cfg *PubConfig) Publisher {
	switch impl {
	case gobroker.RabbitMQ:
		return newRabbitMQPub(cfg)
	case gobroker.Kafka:
		return nil
	default:
		return nil
	}
}
