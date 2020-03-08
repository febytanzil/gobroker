package pubsub

import (
	"github.com/febytanzil/gobroker"
	"github.com/nsqio/go-nsq"
)

type nsqPub struct {
	p     *nsq.Producer
	codec gobroker.Codec
}

func (n *nsqPub) Publish(topic string, message interface{}) error {
	data, err := n.codec.Encode(message)
	if nil != err {
		return err
	}

	return n.p.Publish(topic, data)
}
