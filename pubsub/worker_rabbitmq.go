package pubsub

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/febytanzil/gobroker"
	"github.com/streadway/amqp"
)

type rabbitMQWorker struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	server    string
	host      string
	pub       *rabbitMQPub
	isStopped int32
	qos       int
	retry     int
}

func newRabbitMQWorker(server, vhost string, maxInFlight, retry int) *rabbitMQWorker {
	return &rabbitMQWorker{
		server: server,
		host:   vhost,
		qos:    maxInFlight,
		retry:  retry,
	}
}

func (r *rabbitMQWorker) Consume(queue, exchange string, maxRequeue int, handler gobroker.Handler) {
	retries := 0
	for {
		if 1 == atomic.LoadInt32(&r.isStopped) {
			log.Println("worker has been stopped")
			break
		}

		if err := r.initConn(queue, exchange); nil != err {
			log.Printf("worker failed to initialize retried [%d] %s \n", retries, err)
			if 0 == r.retry || r.retry > retries {
				retries++
				continue
			}
			r.Stop()
			break
		} else {
			// reset retry counter for next possible disconnect
			retries = 0
		}

		deliveries, err := r.channel.Consume(queue, "", false, false, false, false, nil)
		if nil != err {
			log.Println("failed to consume caused by", err)
		}

		for msg := range deliveries {
			count := int32(0)
			if _, ok := msg.Headers["requeueCount"]; ok {
				count, _ = msg.Headers["requeueCount"].(int32)
				if maxRequeue < int(count) {
					log.Printf("maxRequeue limit msg [%s|%s|%s|%d]\n", msg.Exchange, queue, string(msg.Body), count)
					msg.Reject(false)
					continue
				}
			}

			err = handler(&gobroker.Message{
				Body:     msg.Body,
				Attempts: int(count),
			})
			if nil != err {
				count++
				go func() {
					f := make(chan futurePublish)
					defer close(f)
					err = r.pub.publish(exchange, msg.Body, amqp.Table{
						"requeueCount": count,
					}, f)
					if nil != err {
						log.Printf("failed to requeue msg [%s|%s|%s|%s|%d] err: %s\n", exchange, queue, msg.AppId, string(msg.Body), count, err)
						return
					}
					ftr := <-f
					if nil != ftr.err {
						log.Printf("failed to requeue msg [%s|%s|%s|%s|%d] err: %s\n", exchange, queue, msg.AppId, string(msg.Body), count, ftr.err)
					}
				}()

				msg.Reject(false)
				continue
			}

			msg.Ack(false)
		}
		log.Println("worker rabbitmq deliver ended")
	}
}

func (r *rabbitMQWorker) Stop() error {
	if !atomic.CompareAndSwapInt32(&r.isStopped, 0, 1) {
		return nil
	}
	log.Printf("stopping worker, closing connection to vhost[%s] server[%s]", r.host, r.server)

	if nil == r.conn {
		return nil
	}

	return r.conn.Close()
}

func (r *rabbitMQWorker) initConn(queue, exchange string) error {
	conn, err := amqp.DialConfig(r.server, amqp.Config{
		Heartbeat: 10 * time.Second,
		Vhost:     r.host,
	})
	if nil != err {
		return err
	}

	ch, err := conn.Channel()
	if nil != err {
		return err
	}

	err = ch.ExchangeDeclare(
		exchange,
		amqp.ExchangeFanout, // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	if nil != err {
		return err
	}

	q, err := ch.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if nil != err {
		return err
	}

	qos := 3
	if 0 < r.qos {
		qos = r.qos
	}
	err = ch.Qos(
		qos,   // prefetch count
		0,     // prefetch size
		false, // global
	)
	if nil != err {
		return err
	}

	err = ch.QueueBind(q.Name, "", exchange, false, nil)
	if nil != err {
		return err
	}
	p := newRabbitMQPub(&config{
		serverURL: r.server,
		vHost:     r.host,
	})

	r.conn = conn
	r.channel = ch
	r.pub = p

	log.Printf("worker connection initialized: exchange[%s] queue[%s]\n", exchange, queue)

	return nil
}
