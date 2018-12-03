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

type SubHandler struct {
	Name       string
	Topic      string
	Handler    gobroker.Handler
	Concurrent int
	MaxRequeue int
}

type defaultSubscriber struct {
	engine []worker
	c      *config
	subs   []*SubHandler
	impl   gobroker.Implementation
}

const (
	defaultMaxRequeue int = 9999
)

func NewSubscriber(impl gobroker.Implementation, handlers []*SubHandler, options ...Option) Subscriber {
	c := &config{}
	for _, o := range options {
		o(c)
	}
	s := &defaultSubscriber{
		c:    c,
		subs: handlers,
		impl: impl,
	}

	return s
}

func (d *defaultSubscriber) Start() {
	d.engine = make([]worker, len(d.subs))
	switch d.impl {
	case gobroker.RabbitMQ:
		for i, v := range d.subs {
			d.engine[i] = newRabbitMQWorker(d.c.serverURL, d.c.vHost)
			if 0 > v.MaxRequeue {
				v.MaxRequeue = defaultMaxRequeue
			}
			go d.engine[i].Consume(v.Name, v.Topic, v.MaxRequeue, v.Handler)
		}
	case gobroker.Google:
		for i, v := range d.subs {
			d.engine[i] = newGoogleWorker(d.c.projectID, d.c.googleJSONFile)
			if 0 > v.MaxRequeue {
				v.MaxRequeue = defaultMaxRequeue
			}
			go d.engine[i].Consume(v.Name, v.Topic, v.MaxRequeue, v.Handler)
		}
	default:

	}
}

func (d *defaultSubscriber) Stop() {
	for i := range d.subs {
		d.engine[i].Stop()
	}
}
