package pubsub

import "github.com/febytanzil/gobroker"

type nsqWorker struct {
}

func (n *nsqWorker) Consume(name, topic string, maxRequeue int, handler gobroker.Handler) {

}

func (n *nsqWorker) Stop() error {

}
