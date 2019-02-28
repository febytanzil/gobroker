package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type googlePub struct {
	c       *pubsub.Client
	topics  *sync.Map
	m       sync.Mutex
	cluster string
}

func newGooglePub(cfg *config) *googlePub {
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
	}
}

func (g *googlePub) Publish(topic string, message interface{}) error {
	data, err := json.Marshal(message)
	if nil != err {
		return err
	}

	topicName := fmt.Sprintf("%s-%s", g.cluster, topic)
	return g.publish(topicName, &pubsub.Message{
		Data: data,
	})
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
