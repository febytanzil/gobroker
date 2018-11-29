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
	return &defaultSubscriber{
		config: cfg,
		impl:   impl,
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
	case gobroker.Google:
		for i, v := range d.config.List {
			d.engine[i] = newGoogleWorker(d.config.VHost)
			if 0 > v.MaxRequeue {
				v.MaxRequeue = defaultMaxRequeue
			}
			go d.engine[i].Consume(v.Name, v.Topic, v.MaxRequeue, v.Handler)
		}
	default:

	}
}

func (d *defaultSubscriber) Stop() {
	for i := range d.config.List {
		d.engine[i].Stop()
	}
}
