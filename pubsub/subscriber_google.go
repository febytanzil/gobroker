package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/febytanzil/gobroker"
	"log"
	"strconv"
	"sync/atomic"
)

type googleWorker struct {
	c         *pubsub.Client
	projectID string
	isStopped int32
}

func newGoogleWorker(projectID string) *googleWorker {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if nil != err {
		log.Fatal("failed to initialize google publisher", err)
	}
	return &googleWorker{
		c:         client,
		projectID: projectID,
	}
}

func (g *googleWorker) Consume(name, topic string, maxRequeue int, handler gobroker.Handler) {
	for {
		if 1 == atomic.LoadInt32(&g.isStopped) {
			log.Println("worker has been stopped")
			break
		}
		ctx := context.Background()
		sub, err := g.c.CreateSubscription(ctx, name, pubsub.SubscriptionConfig{
			Topic: g.c.Topic(topic),
		})
		if nil != err {
			log.Println("worker failed to initialize", err)
			g.Stop()
			break
		}

		log.Printf("worker connection initialized: topic[%s] consumer[%s]\n", topic, name)
		cctx, cancel := context.WithCancel(ctx)
		err = sub.Receive(cctx, func(ctx context.Context, message *pubsub.Message) {
			count := 0
			if s, ok := message.Attributes["requeue_count"]; ok {
				count, _ = strconv.Atoi(s)
			}
			if count > maxRequeue {
				log.Printf("maxRequeue limit msg [%s|%s|%s|%d]\n", topic, name, string(message.Data), count)
				message.Ack()
			} else {
				err = handler(&gobroker.Message{
					Body: message.Data,
				})
			}
			if nil != err {
				count++
				message.Attributes["requeue_count"] = strconv.Itoa(count)
				message.Nack()
			} else {
				message.Ack()
			}
		})
		if nil != err {
			log.Println("worker failed to consume message", err)
			go func() {
				cancel()
			}()
		}
		<-cctx.Done()
	}
}

func (g *googleWorker) Stop() error {
	if !atomic.CompareAndSwapInt32(&g.isStopped, 0, 1) {
		return nil
	}
	log.Printf("stopping worker, closing connection to projectid[%s]", g.projectID)
	if nil == g.c {
		return nil
	}

	return g.c.Close()
}
