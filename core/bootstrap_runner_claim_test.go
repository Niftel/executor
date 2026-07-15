package core

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/praetordev/events"
)

type testClaimer struct {
	err      error
	runID    uuid.UUID
	dispatch uuid.UUID
}

func (claimer *testClaimer) Claim(_ context.Context, runID, dispatchID uuid.UUID) error {
	claimer.runID = runID
	claimer.dispatch = dispatchID
	return claimer.err
}

func TestSecureDispatchRequiresClaimClient(t *testing.T) {
	req := events.ExecutionRequest{ExecutionRunID: uuid.New(), DispatchID: uuid.New()}
	runner := NewBootstrapRunner("", "", "", "", "", "", nil)
	err := runner.Run(&req, make(chan events.JobEvent, 1))
	if err == nil || !strings.Contains(err.Error(), "no claim client is configured") {
		t.Fatalf("Run error = %v, want missing claim client", err)
	}
}

func TestClaimFailureStopsSecureDispatch(t *testing.T) {
	sentinel := errors.New("claim rejected")
	claimer := &testClaimer{err: sentinel}
	req := events.ExecutionRequest{ExecutionRunID: uuid.New(), DispatchID: uuid.New()}
	runner := NewBootstrapRunner("", "", "", "", "", "", nil)
	runner.Claimer = claimer
	err := runner.Run(&req, make(chan events.JobEvent, 1))
	if !errors.Is(err, sentinel) {
		t.Fatalf("Run error = %v, want claim failure", err)
	}
	if claimer.runID != req.ExecutionRunID || claimer.dispatch != req.DispatchID {
		t.Fatalf("claim identifiers = %s/%s, want %s/%s", claimer.runID, claimer.dispatch, req.ExecutionRunID, req.DispatchID)
	}
}
