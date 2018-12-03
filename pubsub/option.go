package pubsub

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
}

func RabbitMQAMPQ(server, vHost string) Option {
	return func(c *config) {
		c.serverURL = server
		c.vHost = vHost
	}
}

func GoogleJSON(projectID string, cred []byte) Option {
	return func(c *config) {
		c.googleAppCred = cred
		c.projectID = projectID
	}
}

func GoogleJSONFile(projectID, filename string) Option {
	return func(c *config) {
		c.googleJSONFile = filename
		c.projectID = projectID
	}
}
