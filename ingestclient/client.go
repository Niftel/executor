// Package ingestclient is the single HTTP client for talking to the ingestion
// service. It centralizes the base URL, the internal auth token, timeouts, and
// bounded retries so callers (executor, reconciler) don't each hand-roll these.
package ingestclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/praetordev/events"
)

// Client talks to a single ingestion base URL. Token, when set, is sent as a
// bearer credential on every request (required by the internal resolve endpoint;
// harmless on the public event/log endpoints).
type Client struct {
	baseURL string
	token   string
	hc      *http.Client
}

// New returns a client for the given ingestion base URL (e.g. http://ingestion:8081)
// and internal token. An empty baseURL yields a client whose calls error, which
// callers can treat as "no HTTP ingestion configured".
func New(baseURL, token string) *Client {
	return &Client{
		baseURL: baseURL,
		token:   token,
		hc:      &http.Client{Timeout: 10 * time.Second},
	}
}

// BaseURL reports the configured ingestion base URL ("" if unset).
func (c *Client) BaseURL() string { return c.baseURL }

// Credentials is the resolved injector set for a run's Machine credential.
type Credentials struct {
	Env   map[string]string `json:"env"`
	Files map[string]string `json:"files"`
}

// ResolveCredentials fetches the decrypted injectors for a run from ingestion's
// authenticated, run-scoped endpoint. The secret is returned to the caller for
// in-memory use only — never persist it.
func (c *Client) ResolveCredentials(ctx context.Context, runID string) (*Credentials, error) {
	if c.baseURL == "" {
		return nil, fmt.Errorf("ingestclient: no base URL configured")
	}
	url := fmt.Sprintf("%s/internal/v1/runs/%s/credentials", c.baseURL, runID)
	var out Credentials
	err := c.doRetry(ctx, func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		c.auth(req)
		resp, err := c.hc.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			return fmt.Errorf("resolve credentials: status %d: %s", resp.StatusCode, string(body))
		}
		return json.NewDecoder(resp.Body).Decode(&out)
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// Runnable reports whether a run may still be bootstrapped (false = terminal or
// absent). Fail-open on transport/status errors so a transient issue never blocks
// a legitimate job — only an explicit runnable=false suppresses the bootstrap.
func (c *Client) Runnable(ctx context.Context, runID string) (bool, error) {
	if c.baseURL == "" {
		return true, nil
	}
	url := fmt.Sprintf("%s/api/v1/runs/%s/runnable", c.baseURL, runID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return true, err
	}
	c.auth(req)
	resp, err := c.hc.Do(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return true, fmt.Errorf("runnable: status %d", resp.StatusCode)
	}
	var body struct {
		Runnable bool `json:"runnable"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return true, err
	}
	return body.Runnable, nil
}

// ResolveInventory fetches the rendered Ansible INI for an inventory. The manifest
// ships only the inventory id (by reference); the executor calls this at dispatch
// and fills the INI into its manifest copy before pushing to the host-runner (#13).
func (c *Client) ResolveInventory(ctx context.Context, inventoryID int64) (string, error) {
	if c.baseURL == "" {
		return "", fmt.Errorf("ingestclient: no base URL configured")
	}
	url := fmt.Sprintf("%s/internal/v1/inventories/%d/rendered", c.baseURL, inventoryID)
	var ini string
	err := c.doRetry(ctx, func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		c.auth(req)
		resp, err := c.hc.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			return fmt.Errorf("resolve inventory: status %d: %s", resp.StatusCode, string(b))
		}
		b, rerr := io.ReadAll(resp.Body)
		if rerr != nil {
			return rerr
		}
		ini = string(b)
		return nil
	})
	return ini, err
}

// ResolveFacts fetches an inventory's stored host facts (keyed by host name). Used
// at dispatch for fact-cache jobs so the facts travel by reference, not embedded
// in the manifest (#48).
func (c *Client) ResolveFacts(ctx context.Context, inventoryID int64) (map[string]json.RawMessage, error) {
	if c.baseURL == "" {
		return nil, fmt.Errorf("ingestclient: no base URL configured")
	}
	url := fmt.Sprintf("%s/internal/v1/inventories/%d/facts", c.baseURL, inventoryID)
	out := map[string]json.RawMessage{}
	err := c.doRetry(ctx, func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		c.auth(req)
		resp, err := c.hc.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			return fmt.Errorf("resolve facts: status %d: %s", resp.StatusCode, string(b))
		}
		return json.NewDecoder(resp.Body).Decode(&out)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PostEvents delivers a batch of job events for a run.
func (c *Client) PostEvents(ctx context.Context, runID string, evs []events.JobEvent) error {
	if c.baseURL == "" {
		return fmt.Errorf("ingestclient: no base URL configured")
	}
	data, err := json.Marshal(evs)
	if err != nil {
		return fmt.Errorf("marshal events: %w", err)
	}
	url := fmt.Sprintf("%s/api/v1/runs/%s/events", c.baseURL, runID)
	return c.doRetry(ctx, func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		c.auth(req)
		resp, err := c.hc.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("post events: status %d", resp.StatusCode)
		}
		return nil
	})
}

func (c *Client) auth(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
}

// doRetry runs fn up to 3 times with linear backoff, giving up early if ctx is done.
func (c *Client) doRetry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt < 3 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
		}
	}
	return err
}
