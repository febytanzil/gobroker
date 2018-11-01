package examples

import (
	"fmt"
	"github.com/febytanzil/gobroker"
	"github.com/febytanzil/gobroker/pubsub"
	"log"
	"os"
	"os/signal"
	"time"
)

func main() {
	p := pubsub.NewPublisher(pubsub.RabbitMQ, &pubsub.PubConfig{
		ServerURL: fmt.Sprintf("amqp://guest:guest@localhost:5672/"),
		VHost:     "vhost",
	})

	ticker := time.NewTicker(time.Second)
	go func() {
		for t := range ticker.C {
			err := p.Publish("test.fanout", "msg"+t.String())
			log.Println(err)
		}
	}()

	s := pubsub.NewSubscriber(pubsub.RabbitMQ, &pubsub.SubConfig{
		ServerURL: fmt.Sprintf("amqp://guest:guest@localhost:5672/"),
		List: []*pubsub.Sub{
			{
				Name:       "test",
				Topic:      "test.fanout",
				Handler:    test,
				MaxRequeue: 10,
			},
		},
		VHost: "vhost",
	})

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

func test(msg *gobroker.Message) error {
	log.Println("consume", string(msg.Body))
	return nil
}
