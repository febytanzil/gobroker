package pubsub

type Publisher interface {
	Publish(topic string, message interface{}) error
}

type PubConfig struct {
	ServerURL string
	VHost     string
}

type Implementation int

const (
	RabbitMQ = Implementation(iota)
	Kafka
)

func NewPublisher(impl Implementation, cfg *PubConfig) Publisher {
	switch impl {
	case RabbitMQ:
		return newRabbitMQPub(cfg)
	case Kafka:
		return nil
	default:
		return nil
	}
}
