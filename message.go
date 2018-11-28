package gobroker

type Message struct {
	Body []byte
}

type Handler func(msg *Message) error

type Implementation int

const (
	RabbitMQ = Implementation(iota)
	Kafka
	Google
)
