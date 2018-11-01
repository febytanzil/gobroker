# gobroker
wrapper for all (to-be) kinds of message brokers.

## Supported message brokers & patterns
- RabbitMQ pubsub (*fanout*)

## Intentions & Features
- Generic terms & functions to use message brokers
- Auto reconnection
- Requeue message
- Concurrent subscribers in single service (*TODO*)

## Usage
Examples are provided in `examples` folder/ package
```
// initialize publisher
p := pubsub.NewPublisher(gobroker.RabbitMQ, &pubsub.PubConfig{
  ServerURL: fmt.Sprintf("amqp://guest:guest@localhost:5672/"),
  VHost:     "vhost",
})

p.Publish("test.fanout", "msg"+t.String())
```
```
// register subscriber(s) & run it
s := pubsub.NewSubscriber(gobroker.RabbitMQ, &pubsub.SubConfig{
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
```
```
// subcriber function format
// return nil will ack the message as sucess
// return error will requeue based on config
func test(msg *gobroker.Message) error {
	log.Println("consume", string(msg.Body))
	return nil
}
```

## Contributing
Please use a fork to create a pull request
