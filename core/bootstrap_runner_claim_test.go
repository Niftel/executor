package core

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Niftel/praetor-secrets/credential"
	"github.com/google/uuid"
	"github.com/praetordev/events"
)

type testClaimer struct {
	err      error
	runID    uuid.UUID
	dispatch uuid.UUID
}

type testCredentialResolver struct {
	request credential.ResolveRequest
	result  credential.ResolvedCredential
	err     error
}

func (resolver *testCredentialResolver) Resolve(_ context.Context, request credential.ResolveRequest) (credential.ResolvedCredential, error) {
	resolver.request = request
	result := resolver.result
	if result.RunID == "" {
		result.RunID = request.RunID
	}
	if result.AttemptID == "" {
		result.AttemptID = request.AttemptID
	}
	if result.ExpiresAt.IsZero() {
		result.ExpiresAt = time.Now().Add(time.Minute)
	}
	return result, resolver.err
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

func TestSecureCredentialRequiresSecretsClient(t *testing.T) {
	req := events.ExecutionRequest{
		ExecutionRunID: uuid.New(),
		DispatchID:     uuid.New(),
		JobManifest:    events.JobManifest{CredentialID: 42},
	}
	runner := NewBootstrapRunner("", "", "", "", "", "", nil)
	runner.Claimer = &testClaimer{}
	err := runner.Run(&req, make(chan events.JobEvent, 1))
	if err == nil || !strings.Contains(err.Error(), "no secrets client is configured") {
		t.Fatalf("Run error = %v, want missing secrets client", err)
	}
}

func TestSecureCredentialResolvedDirectly(t *testing.T) {
	resolver := &testCredentialResolver{result: credential.ResolvedCredential{
		Environment: map[string]string{"ANSIBLE_REMOTE_USER": "automation"},
		Files:       []credential.ResolvedFile{{Name: "ANSIBLE_PRIVATE_KEY_FILE", Mode: "0600", Content: "private material"}},
	}}
	req := events.ExecutionRequest{
		ExecutionRunID: uuid.New(),
		DispatchID:     uuid.New(),
		JobManifest:    events.JobManifest{CredentialID: 42},
	}
	runner := NewBootstrapRunner("", "", t.TempDir(), "", "", "", nil)
	runner.Claimer = &testClaimer{}
	runner.CredentialResolver = resolver
	_ = runner.Run(&req, make(chan events.JobEvent, 1))

	if resolver.request.RunID != req.ExecutionRunID.String() || resolver.request.AttemptID == "" || resolver.request.RequestedAt.IsZero() {
		t.Fatalf("Resolve request = %+v, want run ID, attempt ID, and request time", resolver.request)
	}
	if got := req.JobManifest.CredentialEnv["ANSIBLE_REMOTE_USER"]; got != "automation" {
		t.Fatalf("CredentialEnv user = %q", got)
	}
	if got := req.JobManifest.CredentialFiles["ANSIBLE_PRIVATE_KEY_FILE"]; got != "private material" {
		t.Fatalf("CredentialFiles key = %q", got)
	}
}

func TestSecureCredentialRejectsInvalidFileMode(t *testing.T) {
	resolver := &testCredentialResolver{result: credential.ResolvedCredential{
		Files: []credential.ResolvedFile{{Name: "ANSIBLE_PRIVATE_KEY_FILE", Mode: "0644", Content: "must not leak"}},
	}}
	req := events.ExecutionRequest{
		ExecutionRunID: uuid.New(),
		DispatchID:     uuid.New(),
		JobManifest:    events.JobManifest{CredentialID: 42},
	}
	runner := NewBootstrapRunner("", "", "", "", "", "", nil)
	runner.Claimer = &testClaimer{}
	runner.CredentialResolver = resolver
	err := runner.Run(&req, make(chan events.JobEvent, 1))
	if err == nil || !strings.Contains(err.Error(), "invalid credential file") {
		t.Fatalf("Run error = %v, want invalid credential file", err)
	}
	if strings.Contains(err.Error(), "must not leak") {
		t.Fatalf("Run error contains secret material: %v", err)
	}
}

func TestSecureCredentialRejectsMismatchedResolution(t *testing.T) {
	resolver := &testCredentialResolver{result: credential.ResolvedCredential{RunID: uuid.NewString()}}
	req := events.ExecutionRequest{
		ExecutionRunID: uuid.New(),
		DispatchID:     uuid.New(),
		JobManifest:    events.JobManifest{CredentialID: 42},
	}
	runner := NewBootstrapRunner("", "", "", "", "", "", nil)
	runner.Claimer = &testClaimer{}
	runner.CredentialResolver = resolver
	err := runner.Run(&req, make(chan events.JobEvent, 1))
	if err == nil || !strings.Contains(err.Error(), "invalid resolution metadata") {
		t.Fatalf("Run error = %v, want invalid resolution metadata", err)
	}
}
