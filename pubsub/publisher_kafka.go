package pubsub

import "encoding/json"

type kafkaPub struct {
}

func newKafkaPub(cfg *PubConfig) *kafkaPub {
	return &kafkaPub{}
}

func (k *kafkaPub) Publish(topic string, message interface{}) error {
	_, err := json.Marshal(message)
	if nil != err {
		return err
	}
	return nil
}
