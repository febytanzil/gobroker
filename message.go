package gobroker

type Message struct {
	Body     []byte
	Attempts int
}

type Handler func(msg *Message) error

type Implementation int

const (
	RabbitMQ = Implementation(iota)
	Google
)
