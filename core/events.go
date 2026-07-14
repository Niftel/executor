package core

import (
	"github.com/praetordev/events"
)

// EventSubscriber defines how the executor listens for jobs.
type EventSubscriber interface {
	SubscribeToExecutionRequests() (<-chan events.ExecutionRequest, error)
}

// EventPublisher defines how the executor emits events.
type EventPublisher interface {
	PublishJobEvent(event *events.JobEvent) error
	PublishLogChunk(chunk *events.LogChunk) error
}

// NOOPEventBus is a placeholder for testing/dev
type NOOPEventBus struct {
	ReqChan chan events.ExecutionRequest
}

func NewNOOPEventBus() *NOOPEventBus {
	return &NOOPEventBus{
		ReqChan: make(chan events.ExecutionRequest, 100),
	}
}

func (b *NOOPEventBus) SubscribeToExecutionRequests() (<-chan events.ExecutionRequest, error) {
	return b.ReqChan, nil
}

func (b *NOOPEventBus) PublishJobEvent(event *events.JobEvent) error {
	return nil
}

func (b *NOOPEventBus) PublishLogChunk(chunk *events.LogChunk) error {
	return nil
}
