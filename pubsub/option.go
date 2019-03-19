package pubsub

// Option configures Publisher & Subscriber
type Option func(c *config)

type config struct {
	// serverURL accepts a string to specify message broker server
	// rabbitMQ - AMQP URI format
	serverURL string

	// vHost specifies pubsub namespace
	// rabbitMQ - Virtual Host
	vHost string

	// googleAppCred
	googleAppCred []byte

	// googleJSONFile specifies path to .json credential file
	googleJSONFile string

	// projectID specifies Google Pubsub project-id
	projectID string

	// cluster separator topic and subscriptions in Google Pubsub
	cluster string

	// retry counts maximum retry attempts to reconnect to server
	// 0 means unlimited retry
	retry int
}

// RabbitMQAMQP configures Publisher & Subscriber for RabbitMQ connection
func RabbitMQAMQP(server, vHost string) Option {
	return func(c *config) {
		c.serverURL = server
		c.vHost = vHost
	}
}

// GoogleJSON configures Publisher & Subscriber for Google Cloud Pub/Sub auth using JSON bytes
func GoogleJSON(projectID, cluster string, cred []byte) Option {
	return func(c *config) {
		c.googleAppCred = cred
		c.projectID = projectID
		c.cluster = cluster
	}
}

// GoogleJSONFile configures Publisher & Subscriber for Google Cloud Pub/Sub auth using JSON filename
func GoogleJSONFile(projectID, cluster, filename string) Option {
	return func(c *config) {
		c.googleJSONFile = filename
		c.projectID = projectID
		c.cluster = cluster
	}
}

// MaxReconnect defines retry attempts to reconnect, 0 means unlimited retry
func MaxReconnect(retry int) Option {
	return func(c *config) {
		if 0 > retry {
			retry = 3
		}
		c.retry = retry
	}
}
