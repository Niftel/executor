package core_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/praetordev/events"
	"github.com/praetordev/executor/core"
)

func TestAgentProcessing(t *testing.T) {
	// Setup generic In-Memory Bus for testing
	reqChan := make(chan events.ExecutionRequest, 10)
	eventChan := make(chan events.JobEvent, 10)

	sub := &TestSubscriber{ch: reqChan}
	pub := &TestPublisher{ch: eventChan}
	runner := &core.MockRunner{} // Our mock runner emits 5 events total (1 start + 3 tasks + 1 end)

	agent := core.NewAgent(sub, pub, runner, 2)

	// Start Agent in goroutine
	go agent.Start()

	// Feed a request
	uid := uuid.New()
	req := events.ExecutionRequest{
		ExecutionRunID: uid,
		UnifiedJobID:   1,
	}
	reqChan <- req
	close(reqChan) // Close to let agent finish after processing

	// Collect events
	// We expect 5 events from MockRunner
	expectedEvents := 5
	receivedEvents := 0

	// Wait with timeout
	timeout := time.After(2 * time.Second)

	// Simple loop to read expected number of events
	for i := 0; i < expectedEvents; i++ {
		select {
		case evt := <-eventChan:
			if evt.ExecutionRunID != uid {
				t.Errorf("Expected run ID %s, got %s", uid, evt.ExecutionRunID)
			}
			receivedEvents++
		case <-timeout:
			t.Fatalf("Timed out waiting for events. Received %d/%d", receivedEvents, expectedEvents)
		}
	}

	if receivedEvents != expectedEvents {
		t.Errorf("Expected %d events, got %d", expectedEvents, receivedEvents)
	}
}

// -- Test Helpers --

type TestSubscriber struct {
	ch chan events.ExecutionRequest
}

func (s *TestSubscriber) SubscribeToExecutionRequests() (<-chan events.ExecutionRequest, error) {
	return s.ch, nil
}

type TestPublisher struct {
	ch chan events.JobEvent
}

func (p *TestPublisher) PublishJobEvent(event *events.JobEvent) error {
	p.ch <- *event
	return nil
}
func (p *TestPublisher) PublishLogChunk(chunk *events.LogChunk) error {
	return nil
}
