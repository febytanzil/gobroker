package pubsub

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"

	"cloud.google.com/go/pubsub"
	"github.com/febytanzil/gobroker"
	"google.golang.org/api/option"
	"time"
)

type googleWorker struct {
	c                   *pubsub.Client
	projectID           string
	cluster             string
	isStopped           int32
	pub                 *googlePub
	maxOutstanding      int
	timeout             time.Duration
	timeoutMaxExtension time.Duration
	codec               gobroker.Codec
	contentType         string
}

const maxPubSubTimeout = 10 * time.Minute

func newGoogleWorker(c *config, maxInFlight int, timeout, timeoutMaxExtension time.Duration) *googleWorker {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, c.projectID, option.WithCredentialsFile(c.googleJSONFile))
	if nil != err {
		log.Fatal("failed to initialize google publisher", err)
	}

	if c.cluster == "" {
		log.Fatalln("cluster name cannot empty")
	}
	if maxPubSubTimeout < timeout || 0 >= timeout {
		timeout = maxPubSubTimeout
	}

	return &googleWorker{
		c:         client,
		projectID: c.projectID,
		pub: newGooglePub(&config{
			projectID:      c.projectID,
			googleJSONFile: c.googleJSONFile,
			cluster:        c.cluster,
			codec:          c.codec,
		}),
		maxOutstanding:      maxInFlight,
		cluster:             c.cluster,
		timeout:             timeout,
		timeoutMaxExtension: timeoutMaxExtension,
		codec:               c.codec,
		contentType:         c.contentType,
	}
}

func (g *googleWorker) Consume(name, topic string, maxRequeue int, handler gobroker.Handler) {
	for {
		if 1 == atomic.LoadInt32(&g.isStopped) {
			log.Println("worker has been stopped")
			break
		}
		ctx := context.Background()
		subName := fmt.Sprintf("%s-%s-%s", g.cluster, topic, name)
		topicName := fmt.Sprintf("%s-%s", g.cluster, topic)

		sub := g.c.Subscription(subName)
		if exist, err := sub.Exists(ctx); nil == err {
			if !exist {
				t := g.c.Topic(topicName)
				if exist, err = t.Exists(ctx); !exist {
					t, err = g.c.CreateTopic(ctx, topicName)
					if nil != err {
						// error usually race condition of creating new topic
						// let this continue to retry
						log.Println("worker failed to create new topic:", err)
						continue
					}
				}
				sub, err = g.c.CreateSubscription(ctx, subName, pubsub.SubscriptionConfig{
					Topic: t,
				})
				if nil != err {
					// error usually race condition of creating new subscriber
					// let this continue to retry
					log.Println("worker failed to create new subscription:", err)
					continue
				}
			}
		} else {
			log.Println("worker failed to initialize:", err)
			g.Stop()
			break
		}

		maxOutstanding := 1
		if 0 < g.maxOutstanding {
			maxOutstanding = g.maxOutstanding
		}
		sub.ReceiveSettings = pubsub.ReceiveSettings{
			MaxExtension:           g.timeoutMaxExtension,
			MaxOutstandingMessages: maxOutstanding,
		}
		sub.Update(ctx, pubsub.SubscriptionConfigToUpdate{
			AckDeadline: g.timeout,
		})

		log.Printf("worker connection initialized: topic[%s] consumer[%s]\n", topicName, subName)
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
					Body:        message.Data,
					Attempts:    count,
					ContentType: g.contentType,
				})
				if nil != handlerErr {
					count++
					if _, ok := message.Attributes["requeue_count"]; !ok {
						message.Attributes = make(map[string]string)
					}
					message.Attributes["requeue_count"] = strconv.Itoa(count)
					err := g.pub.publish(topicName, &pubsub.Message{
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
		log.Printf("worker google consume stopped: topic[%s] consumer[%s]\n", topicName, subName)
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
