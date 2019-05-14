package gobroker

// Message encapsulates actual message being sent & published by message broker
type Message struct {
	// Body to be deprecated soon switched by DecodedBody
	Body []byte
	// DecodedBody stores new body format for next release
	DecodedBody interface{}
	Attempts    int
	ContentType string
}

// Handler defines how client should handle incoming messages as subscriber
type Handler func(msg *Message) error

// Implementation defines supported adapters
type Implementation int

const (
	RabbitMQ = Implementation(iota)
	Google
)
