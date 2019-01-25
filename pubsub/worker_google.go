package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/febytanzil/gobroker"
	"google.golang.org/api/option"
	"log"
	"strconv"
	"sync/atomic"
	"fmt"
)

type googleWorker struct {
	c         *pubsub.Client
	projectID string
	isStopped int32
	pub       *googlePub
}

func newGoogleWorker(projectID, credFile string) *googleWorker {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsFile(credFile))
	if nil != err {
		log.Fatal("failed to initialize google publisher", err)
	}
	return &googleWorker{
		c:         client,
		projectID: projectID,
		pub: newGooglePub(&config{
			projectID:      projectID,
			googleJSONFile: credFile,
		}),
	}
}

func (g *googleWorker) Consume(name, topic string, maxRequeue int, handler gobroker.Handler) {
	for {
		if 1 == atomic.LoadInt32(&g.isStopped) {
			log.Println("worker has been stopped")
			break
		}
		ctx := context.Background()
		subName := fmt.Sprintf("%s-%s", topic, name)
		sub := g.c.Subscription(subName)
		if exist, err := sub.Exists(ctx); nil == err {
			if !exist {
				t := g.c.Topic(topic)
				if exist, err = t.Exists(ctx); !exist {
					t, err = g.c.CreateTopic(ctx, topic)
					if nil != err {
						// error usually race condition of creating new topic
						// let this continue to retry
						log.Println("worker failed to create new topic", err)
						continue
					}
				}
				sub, err = g.c.CreateSubscription(ctx, subName, pubsub.SubscriptionConfig{
					Topic: t,
				})
				if nil != err {
					// error usually race condition of creating new subscriber
					// let this continue to retry
					log.Println("worker failed to create new subscription", err)
					continue
				}
				sub.ReceiveSettings = pubsub.ReceiveSettings{
					// TODO setting inflight
				}
			}
		} else {
			log.Println("worker failed to initialize", err)
			g.Stop()
			break
		}

		log.Printf("worker connection initialized: topic[%s] consumer[%s]\n", topic, subName)
		cctx, cancel := context.WithCancel(ctx)
		err := sub.Receive(cctx, func(ctx context.Context, message *pubsub.Message) {
			var handlerErr error
			count := 0
			if s, ok := message.Attributes["requeue_count"]; ok {
				count, _ = strconv.Atoi(s)
			}
			if count > maxRequeue {
				log.Printf("maxRequeue limit msg [%s|%s|%d]\n", subName, string(message.Data), count)
			} else {
				handlerErr = handler(&gobroker.Message{
					Body:     message.Data,
					Attempts: count,
				})
				if nil != handlerErr {
					count++
					if _, ok := message.Attributes["requeue_count"]; !ok {
						message.Attributes = make(map[string]string)
					}
					message.Attributes["requeue_count"] = strconv.Itoa(count)
					err := g.pub.publish(topic, &pubsub.Message{
						Data:       message.Data,
						Attributes: message.Attributes,
					})
					if nil != err {
						log.Printf("failed to requeue msg [%s|%s|%s|%d] err: %s\n", subName, message.ID, string(message.Data), count, err)
					}
				}
			}
			message.Ack()
		})
		if nil != err {
			log.Println("worker failed to consume message", err)
			go func() {
				cancel()
			}()
		}
		<-cctx.Done()
		log.Println("worker google receive ended")
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
