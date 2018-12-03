package pubsub

import (
	"github.com/febytanzil/gobroker"
	"github.com/streadway/amqp"
	"log"
	"sync/atomic"
	"time"
)

type rabbitMQWorker struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	server    string
	host      string
	pub       *rabbitMQPub
	isStopped int32
}

func newRabbitMQWorker(server, vhost string) *rabbitMQWorker {
	return &rabbitMQWorker{
		server: server,
		host:   vhost,
	}
}

func (r *rabbitMQWorker) Consume(queue, exchange string, maxRequeue int, handler gobroker.Handler) {
	for {
		if 1 == atomic.LoadInt32(&r.isStopped) {
			log.Println("worker has been stopped")
			break
		}

		if err := r.initConn(queue, exchange); nil != err {
			log.Println("worker failed to initialize")
			r.Stop()
			break
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
				Body: msg.Body,
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
		log.Panicln("worker could not initialize rabbitmq connection", err)
	}

	ch, err := conn.Channel()
	if nil != err {
		log.Panicln("worker could not open rabbitmq channel", err)
	}

	err = ch.ExchangeDeclare(
		exchange,
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if nil != err {
		log.Panicln("worker could not declare rabbitmq exchange", exchange, err)
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
		log.Panicln("worker could not declare rabbitmq queue", queue, err)
	}

	err = ch.QueueBind(q.Name, "", exchange, false, nil)
	if nil != err {
		log.Panicln("worker could not bind rabbitmq queue", queue, err)
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
