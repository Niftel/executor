package claimclient

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewRejectsIncompleteOrInsecureConfiguration(t *testing.T) {
	tests := []Config{
		{},
		{SchedulerURL: "http://scheduler.internal", CAFile: "ca.pem", CertificateFile: "executor.pem", PrivateKeyFile: "executor-key.pem", Timeout: 10 * time.Second},
		{SchedulerURL: "https://scheduler.internal", Timeout: 10 * time.Second},
	}
	for _, config := range tests {
		if _, err := New(config); !errors.Is(err, ErrConfiguration) {
			t.Fatalf("New(%+v) error = %v, want ErrConfiguration", config, err)
		}
	}
}

func TestClaimRejectsEmptyIdentifiers(t *testing.T) {
	client := &Client{}
	if err := client.Claim(t.Context(), uuid.Nil, uuid.New()); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("Claim error = %v, want ErrConfiguration", err)
	}
}
