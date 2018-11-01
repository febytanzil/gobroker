package pubsub

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type rabbitMQPub struct {
	conn    *amqp.Connection
	channel *sync.Map
	m       *sync.Mutex
	cm      *sync.Mutex
	msgQ    chan rabbitMQPubMsg
	server  string
	host    string
	state   int32
}

type futurePublish struct {
	err error
}

type rabbitMQPubMsg struct {
	exchange string
	body     []byte
	done     chan futurePublish
	headers  amqp.Table
}

const (
	stateInit = iota
	stateDisconnected
	stateConnected
)

func newRabbitMQPub(cfg *PubConfig) *rabbitMQPub {
	return &rabbitMQPub{
		channel: &sync.Map{},
		server:  cfg.ServerURL,
		host:    cfg.VHost,
		m:       &sync.Mutex{},
		cm:      &sync.Mutex{},
		msgQ:    make(chan rabbitMQPubMsg),
	}
}

func (r *rabbitMQPub) getExchangeChannel(exchange string) (channel *amqp.Channel, err error) {
	ch, exist := r.channel.Load(exchange)
	if !exist {
		r.cm.Lock()
		defer r.cm.Unlock()

		ch, exist = r.channel.Load(exchange)
		if !exist {
			if nil != r.conn {
				channel, err = r.conn.Channel()
				if nil != amqp.ErrClosed {
					err = r.connect()
					if nil != err {
						return nil, err
					}
				}

				err = channel.ExchangeDeclare(
					exchange,
					"fanout", // type
					true,     // durable
					false,    // auto-deleted
					false,    // internal
					false,    // no-wait
					nil,      // arguments
				)
				if nil == err {
					r.channel.Store(exchange, channel)
				}
			} else {
				err = r.connect()
			}
		}
	} else {
		channel = ch.(*amqp.Channel)
	}

	return
}

func (r *rabbitMQPub) purgeChannel() (err error) {
	if nil != r.conn {
		r.conn.Close()
		r.channel.Range(func(key, value interface{}) bool {
			r.channel.Delete(key)
			return true
		})
	}

	return
}

func (r *rabbitMQPub) Publish(exchange string, message interface{}) error {
	body, err := json.Marshal(message)
	if nil != err {
		return err
	}
	future := make(chan futurePublish)
	defer close(future)

	err = r.publish(exchange, body, nil, future)
	if nil != err {
		return err
	}
	f := <-future

	return f.err
}

func (r *rabbitMQPub) listen(msgs <-chan rabbitMQPubMsg) {
	for one := range msgs {
		ch, err := r.getExchangeChannel(one.exchange)
		if nil != err {
			one.done <- futurePublish{
				err: err,
			}
			continue
		}

		err = ch.Publish(one.exchange, "", false, false, amqp.Publishing{
			Body:    one.body,
			Headers: one.headers,
		})
		if nil != err {
			log.Println("publisher failed to publish message", err)
			r.close()
		}
		go func(m rabbitMQPubMsg, err error) {
			m.done <- futurePublish{
				err: err,
			}
		}(one, err)
	}
}

func (r *rabbitMQPub) publish(exchange string, body []byte, headers amqp.Table, done chan futurePublish) error {
	if atomic.LoadInt32(&r.state) != stateConnected {
		if err := r.connect(); nil != err {
			return err
		}
	}

	go func() {
		r.msgQ <- rabbitMQPubMsg{
			done:     done,
			body:     body,
			exchange: exchange,
			headers:  headers,
		}
	}()

	return nil
}

func (r *rabbitMQPub) connect() error {
	r.m.Lock()
	defer r.m.Unlock()

	if atomic.LoadInt32(&r.state) == stateConnected {
		return nil
	}

	conn, err := amqp.DialConfig(r.server, amqp.Config{
		Heartbeat: 10 * time.Second,
		Vhost:     r.host,
	})
	if nil != err {
		return err
	}

	r.purgeChannel()
	r.conn = conn

	go r.listen(r.msgQ)

	atomic.StoreInt32(&r.state, stateConnected)
	log.Println("publisher connected successfully")

	return nil
}

func (r *rabbitMQPub) close() {
	if !atomic.CompareAndSwapInt32(&r.state, stateConnected, stateDisconnected) {
		return
	}
	go func() {
		r.purgeChannel()
		close(r.msgQ)
	}()
}
