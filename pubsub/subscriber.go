package pubsub

import (
	"github.com/febytanzil/gobroker"
)

type Subscriber interface {
	// Start will spawn workers to subscribe
	Start()
	// Stop will terminate all connections and workers
	Stop()
}

type worker interface {
	Consume(name, topic string, maxRequeue int, handler gobroker.Handler)
	Stop() error
}

type Sub struct {
	Name       string
	Topic      string
	Handler    gobroker.Handler
	Concurrent int
	MaxRequeue int
}

type SubConfig struct {
	List      []*Sub
	ServerURL string
	VHost     string
}

type defaultSubscriber struct {
	engine []worker
	config *SubConfig
	impl   gobroker.Implementation
}

const (
	defaultMaxRequeue int = 9999
)

func NewSubscriber(impl gobroker.Implementation, cfg *SubConfig) Subscriber {
	switch impl {
	case gobroker.RabbitMQ:
		return &defaultSubscriber{
			config: cfg,
			impl:   gobroker.RabbitMQ,
		}
	case gobroker.Kafka:
		return nil
	default:
		return nil
	}
}

func (d *defaultSubscriber) Start() {
	d.engine = make([]worker, len(d.config.List))
	switch d.impl {
	case gobroker.RabbitMQ:
		for i, v := range d.config.List {
			d.engine[i] = newRabbitMQWorker(d.config.ServerURL, d.config.VHost)
			if 0 > v.MaxRequeue {
				v.MaxRequeue = defaultMaxRequeue
			}
			go d.engine[i].Consume(v.Name, v.Topic, v.MaxRequeue, v.Handler)
		}
	case gobroker.Kafka:

	default:

	}
}

func (d *defaultSubscriber) Stop() {
	for i := range d.config.List {
		d.engine[i].Stop()
	}
}
