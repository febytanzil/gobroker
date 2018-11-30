package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"log"
	"sync"
)

type googlePub struct {
	c      *pubsub.Client
	topics *sync.Map
	m      sync.Mutex
}

func newGooglePub(cfg *PubConfig) *googlePub {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, cfg.VHost)
	if nil != err {
		log.Fatal("failed to initialize google publisher", err)
	}
	return &googlePub{
		c:      client,
		topics: &sync.Map{},
	}
}

func (g *googlePub) Publish(topic string, message interface{}) error {
	data, err := json.Marshal(message)
	if nil != err {
		return err
	}

	t := g.getTopic(topic)
	ctx := context.Background()
	result := t.Publish(ctx, &pubsub.Message{
		Data: data,
	})

	_, err = result.Get(ctx)
	return err
}

func (g *googlePub) getTopic(topic string) *pubsub.Topic {
	if t, exist := g.topics.Load(topic); exist {
		return t.(*pubsub.Topic)
	}
	g.m.Lock()
	defer g.m.Unlock()

	if t, exist := g.topics.Load(topic); exist {
		return t.(*pubsub.Topic)
	}

	t := g.c.Topic(topic)
	g.topics.Store(topic, t)

	return t
}
