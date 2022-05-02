package pubsub

import (
	"context"
	"fmt"
	"log"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/febytanzil/gobroker"
	"google.golang.org/api/option"
)

type googlePub struct {
	c       *pubsub.Client
	topics  *sync.Map
	m       sync.Mutex
	cluster string
	codec   gobroker.Codec
}

type GPSFuture struct {
	err    error
	result *pubsub.PublishResult
	ctx    context.Context
}

func (g *GPSFuture) Wait() error {
	if nil != g.err {
		return g.err
	}

	if nil != g.result {
		<-g.result.Ready()
		_, err := g.result.Get(g.ctx)
		return err
	}

	return nil
}

func newGooglePub(options ...Option) *googlePub {
	cfg := configOptions(options...)
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, cfg.projectID, option.WithCredentialsFile(cfg.googleJSONFile))
	if nil != err {
		log.Fatalln("failed to initialize google publisher", err)
	}

	if cfg.cluster == "" {
		log.Fatalln("cluster name cannot empty")
	}

	return &googlePub{
		c:       client,
		topics:  &sync.Map{},
		cluster: cfg.cluster,
		codec:   cfg.codec,
	}
}

func (g *googlePub) Publish(topic string, message interface{}) error {
	data, err := g.codec.Encode(message)
	if nil != err {
		return err
	}

	topicName := fmt.Sprintf("%s-%s", g.cluster, topic)
	return g.publish(topicName, &pubsub.Message{
		Data: data,
	})
}

func (g *googlePub) PublishAsync(topic string, message interface{}) Future {
	data, err := g.codec.Encode(message)
	if nil != err {
		return &GPSFuture{err: err}
	}

	ctx := context.Background()
	t, err := g.getTopic(topic)
	if nil != err {
		return &GPSFuture{err: err}
	}

	result := t.Publish(ctx, &pubsub.Message{
		Data: data,
	})

	return &GPSFuture{
		result: result,
		ctx:    ctx,
	}
}

func (g *googlePub) publish(topic string, message *pubsub.Message) error {
	ctx := context.Background()
	t, err := g.getTopic(topic)
	if nil != err {
		return err
	}

	result := t.Publish(ctx, message)
	_, err = result.Get(ctx)

	return err
}

func (g *googlePub) getTopic(topic string) (*pubsub.Topic, error) {
	var err error

	if t, exist := g.topics.Load(topic); exist {
		return t.(*pubsub.Topic), err
	}
	g.m.Lock()
	defer g.m.Unlock()

	if t, exist := g.topics.Load(topic); exist {
		return t.(*pubsub.Topic), err
	}

	ctx := context.Background()
	t := g.c.Topic(topic)
	if exist, _ := t.Exists(ctx); !exist {
		t, err = g.c.CreateTopic(ctx, topic)
		if nil != err {
			return nil, err
		}
	}
	g.topics.Store(topic, t)

	return t, err
}
