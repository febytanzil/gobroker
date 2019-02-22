package pubsub

import (
	"github.com/febytanzil/gobroker"
	"github.com/febytanzil/gobroker/pubsub"
	"log"
	"os"
	"os/signal"
	"time"
)

func mainRMQ() {
	p := pubsub.NewPublisher(gobroker.RabbitMQ, pubsub.RabbitMQAMQP("amqp://guest:guest@localhost:5672/", "vhost"))

	ticker := time.NewTicker(time.Second)
	go func() {
		for t := range ticker.C {
			err := p.Publish("test.fanout", "msg"+t.String())
			log.Println(err)
		}
	}()

	s := pubsub.NewSubscriber(gobroker.RabbitMQ, []*pubsub.SubHandler{
		{
			Name:       "test",
			Topic:      "test.fanout",
			Handler:    testRMQ,
			MaxRequeue: 10,
			Concurrent: 2,
		},
	}, pubsub.RabbitMQAMQP("amqp://guest:guest@localhost:5672/", "vhost"))

	s.Start()

	c := make(chan os.Signal, 1)
	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	// SIGKILL, SIGQUIT or SIGTERM (Ctrl+/) will not be caught.
	signal.Notify(c, os.Interrupt)

	// Block until we receive our signal.
	<-c
	s.Stop()
	log.Println("shutting down")
	os.Exit(0)
}

func testRMQ(msg *gobroker.Message) error {
	log.Println("consume rabbitmq", string(msg.Body))
	return nil
}
