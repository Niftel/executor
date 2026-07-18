package core_test

import (
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

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

func TestAgentBoundsRunnerFailureEvent(t *testing.T) {
	reqChan := make(chan events.ExecutionRequest, 1)
	eventChan := make(chan events.JobEvent, 1)
	agent := core.NewAgent(
		&TestSubscriber{ch: reqChan},
		&TestPublisher{ch: eventChan},
		&failingRunner{err: errors.New("bootstrap: " + strings.Repeat("x", 2<<20) + " শেষ failure")},
		1,
	)
	done := make(chan error, 1)
	go func() { done <- agent.Start() }()
	reqChan <- events.ExecutionRequest{ExecutionRunID: uuid.New(), UnifiedJobID: 42}
	close(reqChan)

	select {
	case evt := <-eventChan:
		if evt.EventType != "JOB_FAILED" || evt.StdoutSnippet == nil {
			t.Fatalf("unexpected terminal event: %#v", evt)
		}
		message := *evt.StdoutSnippet
		if len(message) > 16*1024 || !utf8.ValidString(message) || !strings.Contains(message, "[truncated]") || !strings.HasSuffix(message, "শেষ failure") {
			t.Fatalf("failure event was not safely bounded: bytes=%d tail=%q", len(message), message[max(0, len(message)-64):])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for bounded failure event")
	}
	if err := <-done; err != nil {
		t.Fatalf("agent stopped with error: %v", err)
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

type failingRunner struct{ err error }

func (r *failingRunner) Run(_ *events.ExecutionRequest, _ chan<- events.JobEvent) error {
	return r.err
}

func (p *TestPublisher) PublishJobEvent(event *events.JobEvent) error {
	p.ch <- *event
	return nil
}
func (p *TestPublisher) PublishLogChunk(chunk *events.LogChunk) error {
	return nil
}
