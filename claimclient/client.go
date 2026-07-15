// Package claimclient provides the narrow mTLS client an executor uses to bind
// a dispatched run to its certificate identity before processing the run.
package claimclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/google/uuid"
)

var ErrConfiguration = errors.New("invalid claim client configuration")

type Config struct {
	SchedulerURL    string
	CAFile          string
	CertificateFile string
	PrivateKeyFile  string
	Timeout         time.Duration
}

type Client struct {
	base *url.URL
	http *http.Client
}

func New(config Config) (*Client, error) {
	base, err := url.Parse(config.SchedulerURL)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil ||
		base.RawQuery != "" || base.Fragment != "" || (base.Path != "" && base.Path != "/") ||
		config.CAFile == "" || config.CertificateFile == "" || config.PrivateKeyFile == "" ||
		config.Timeout < time.Second || config.Timeout > time.Minute {
		return nil, ErrConfiguration
	}
	base.Path = ""
	caPEM, err := os.ReadFile(config.CAFile)
	if err != nil {
		return nil, ErrConfiguration
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, ErrConfiguration
	}
	certificate, err := tls.LoadX509KeyPair(config.CertificateFile, config.PrivateKeyFile)
	if err != nil {
		return nil, ErrConfiguration
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
			RootCAs: roots, Certificates: []tls.Certificate{certificate},
		},
		DisableCompression: true, ForceAttemptHTTP2: false,
		MaxIdleConns: 8, MaxIdleConnsPerHost: 4, IdleConnTimeout: 30 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 5 * time.Second,
	}
	return &Client{base: base, http: &http.Client{
		Transport: transport, Timeout: config.Timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func (client *Client) Claim(ctx context.Context, runID, dispatchID uuid.UUID) error {
	if client == nil || runID == uuid.Nil || dispatchID == uuid.Nil {
		return ErrConfiguration
	}
	body, err := json.Marshal(struct {
		DispatchID uuid.UUID `json:"dispatch_id"`
	}{dispatchID})
	if err != nil {
		return err
	}
	endpoint := *client.base
	endpoint.Path = "/internal/v1/execution-runs/" + url.PathEscape(runID.String()) + "/claim"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := client.http.Do(request)
	if err != nil {
		return fmt.Errorf("claim execution run: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("claim execution run: scheduler returned status %d", response.StatusCode)
	}
	return nil
}
