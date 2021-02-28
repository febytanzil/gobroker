package pubsub

import (
	"github.com/febytanzil/gobroker"
	"github.com/nsqio/go-nsq"
)

type nsqPub struct {
	p     *nsq.Producer
	codec gobroker.Codec
}

type NSQFuture struct {
	err  error
	done chan *nsq.ProducerTransaction
}

func (n *NSQFuture) Wait() error {
	if nil != n.err {
		return n.err
	}
	defer close(n.done)

	pt := <-n.done
	return pt.Error
}

func (n *nsqPub) Publish(topic string, message interface{}) error {
	data, err := n.codec.Encode(message)
	if nil != err {
		return err
	}

	return n.p.Publish(topic, data)
}

func (n *nsqPub) PublishAsync(topic string, message interface{}) Future {
	data, err := n.codec.Encode(message)
	if nil != err {
		return &NSQFuture{err: err}
	}
	done := make(chan *nsq.ProducerTransaction)

	err = n.p.PublishAsync(topic, data, done)
	if nil != err {
		return &NSQFuture{err: err}
	}

	return &NSQFuture{
		done: done,
	}
}

func newNSQPub(cfg *config) *nsqPub {
	prod, _ := nsq.NewProducer(cfg.serverURL, nsq.NewConfig())
	return &nsqPub{
		p:     prod,
		codec: cfg.codec,
	}
}
