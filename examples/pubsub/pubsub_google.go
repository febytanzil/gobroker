package pubsub

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/febytanzil/gobroker"
	"github.com/febytanzil/gobroker/pubsub"
)

func mainGoogle() {
	p := pubsub.NewPublisher(gobroker.Google, pubsub.GoogleJSONFile("gcp-project-id", "/path/to/google/application/credentials/cred.json", "namespace"))

	ticker := time.NewTicker(time.Second)
	go func() {
		for t := range ticker.C {
			err := p.Publish("test", "msg"+t.String())
			log.Println(err)
		}
	}()

	s := pubsub.NewSubscriber(gobroker.Google, []*pubsub.SubHandler{
		{
			Name:       "consumer-test1",
			Topic:      "test",
			Handler:    testGoogle,
			MaxRequeue: 10,
			Concurrent: 2,
		},
		{
			Name:       "consumer-test2",
			Topic:      "test",
			Handler:    testGoogle2,
			MaxRequeue: 10,
			Concurrent: 3,
		},
	},
		pubsub.GoogleJSONFile("gcp-project-id", "/path/to/google/application/credentials/cred.json", "namespace"))

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

func testGoogle(msg *gobroker.Message) error {
	log.Println("consume google pubsub", string(msg.Body))
	return errors.New("requeue msg")
}

func testGoogle2(msg *gobroker.Message) error {
	log.Println("consume google pubsub2", string(msg.Body))
	return nil
}
