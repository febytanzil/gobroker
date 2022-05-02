package gobroker

type DeadLetterError struct {
	topic      string
	errMessage string
}

func (d *DeadLetterError) Error() string {
	return d.errMessage
}

func (d *DeadLetterError) GetTopic() string {
	return d.topic
}

func NewDeadLetterError(topic, errMessage string) *DeadLetterError {
	return &DeadLetterError{topic: topic, errMessage: errMessage}
}
