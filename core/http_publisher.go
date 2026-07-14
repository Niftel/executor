package core

import (
	"context"

	"github.com/praetordev/events"
	"github.com/praetordev/executor/ingestclient"
)

// HttpEventPublisher publishes executor-side events to ingestion over HTTP via
// the shared ingestclient (auth, timeouts, and retries live there).
type HttpEventPublisher struct {
	client *ingestclient.Client
}

func NewHttpEventPublisher(client *ingestclient.Client) *HttpEventPublisher {
	return &HttpEventPublisher{client: client}
}

func (p *HttpEventPublisher) PublishJobEvent(event *events.JobEvent) error {
	return p.client.PostEvents(context.Background(), event.ExecutionRunID.String(), []events.JobEvent{*event})
}

// PublishLogChunk is a no-op for the executor: bulk stdout is streamed to the
// object store by the host-runner's log syncer, not the executor. The method only
// satisfies the EventPublisher interface.
func (p *HttpEventPublisher) PublishLogChunk(chunk *events.LogChunk) error {
	return nil
}
