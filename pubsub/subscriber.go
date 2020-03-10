package pubsub

import (
	"github.com/febytanzil/gobroker"
	"time"
)

// Subscriber provides adapter to subscribe topics
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

// SubHandler defines subscriber configuration
type SubHandler struct {
	Name        string
	Topic       string
	Handler     gobroker.Handler
	Concurrent  int
	MaxRequeue  int
	MaxInFlight int

	// Timeout configures an in-flight message ack deadline processed by subscriber
	Timeout time.Duration
}

type defaultSubscriber struct {
	workers []worker
	c       *config
	subs    []*SubHandler
	impl    gobroker.Implementation
}

const (
	defaultMaxRequeue int = 9999
)

// NewSubscriber implements adapter instance for Subscriber
func NewSubscriber(impl gobroker.Implementation, handlers []*SubHandler, options ...Option) Subscriber {
	c := &config{}
	for _, o := range options {
		o(c)
	}
	if nil == c.codec {
		c.codec = gobroker.StdJSONCodec
		c.contentType = "application/json"
	}

	s := &defaultSubscriber{
		c:    c,
		subs: handlers,
		impl: impl,
	}

	return s
}

func (d *defaultSubscriber) Start() {
	d.workers = make([]worker, len(d.subs))
	switch d.impl {
	case gobroker.RabbitMQ:
		for i, v := range d.subs {
			d.workers[i] = newRabbitMQWorker(d.c, v.MaxInFlight)
			d.run(i, v)
		}
	case gobroker.Google:
		for i, v := range d.subs {
			d.workers[i] = newGoogleWorker(d.c, v.MaxInFlight, v.Timeout)
			d.run(i, v)
		}
	case gobroker.NSQ:
		for i, v := range d.subs {
			d.workers[i] = newNSQWorker(d.c, v)
			d.run(i, v)
		}
	default:

	}
}

func (d *defaultSubscriber) run(index int, sub *SubHandler) {
	if 0 > sub.MaxRequeue {
		sub.MaxRequeue = defaultMaxRequeue
	}
	if 0 >= sub.Concurrent {
		sub.Concurrent = 1
	}
	for i := 0; i < sub.Concurrent; i++ {
		go d.workers[index].Consume(sub.Name, sub.Topic, sub.MaxRequeue, sub.Handler)
	}
}

func (d *defaultSubscriber) Stop() {
	for range d.subs {
		for j := range d.workers {
			d.workers[j].Stop()
		}
	}
}
