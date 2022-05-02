package gobroker

import "time"

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

type DeferredError struct {
	delay      time.Duration
	errMessage string
}

func (d *DeferredError) Error() string {
	return d.errMessage
}

func (d *DeferredError) GetDelay() time.Duration {
	return d.delay
}

func NewDeferredError(delay time.Duration, errMessage string) *DeferredError {
	return &DeferredError{delay: delay, errMessage: errMessage}
}
