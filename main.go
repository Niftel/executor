package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/praetordev/env"
	"github.com/praetordev/eventbus"
	"github.com/praetordev/events"
	"github.com/praetordev/executor/claimclient"
	"github.com/praetordev/executor/core"
	"github.com/praetordev/executor/ingestclient"
	"github.com/praetordev/metrics"
	"github.com/praetordev/plog"
)

func main() {
	plog.Configure("executor")
	log.Println("Starting Executor Agent...")

	// 1. Setup Infrastructure
	bus, err := eventbus.NewBus(env.String("NATS_URL", eventbus.DefaultURL))
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer bus.Close()

	// Shared ingestion client (base URL + internal token, retries) used for the
	// run-scoped runnable pre-flight and just-in-time credential resolution.
	ingestionURL := env.String("INGESTION_URL", "")
	ingest := ingestclient.New(ingestionURL, env.String("PRAETOR_INTERNAL_TOKEN", ""))

	// Determine Publisher (HTTP or NATS)
	var publisher core.EventPublisher = bus
	if ingestionURL != "" {
		log.Printf("Using HTTP Ingestion Publisher at %s", ingestionURL)
		publisher = core.NewHttpEventPublisher(ingest)
	} else {
		log.Println("Using NATS Event Publisher")
	}

	// 4. Create Runner (Bootstrap Mode) — all config resolved here and passed in.
	runner := core.NewBootstrapRunner(
		env.String("GITEA_INTERNAL_URL", ""),
		env.String("GITEA_OWNER", ""),
		env.String("RUNTIME_DIR", ""),
		ingestionURL,
		env.String("HOST_RUNNER_CALLBACK_URL", ""),
		env.String("PRAETOR_INTERNAL_TOKEN", ""),
		ingest,
	)
	claimURL := env.String("PRAETOR_SCHEDULER_CLAIM_URL", "")
	if claimURL != "" {
		claimer, err := claimclient.New(claimclient.Config{
			SchedulerURL:    claimURL,
			CAFile:          env.String("PRAETOR_SCHEDULER_CA_FILE", ""),
			CertificateFile: env.String("PRAETOR_EXECUTOR_CERT_FILE", ""),
			PrivateKeyFile:  env.String("PRAETOR_EXECUTOR_KEY_FILE", ""),
			Timeout:         10 * time.Second,
		})
		if err != nil {
			log.Fatalf("secure claim client misconfigured: %v", err)
		}
		runner.Claimer = claimer
	}

	// Check for One-Shot Mode
	if env.String("PRAETOR_MODE", "") == "oneshot" {
		log.Println("Starting in ONE-SHOT mode")
		manifestPath := env.String("PRAETOR_MANIFEST_PATH", "/etc/praetor/manifest.json")

		// Read manifest
		data, err := os.ReadFile(manifestPath)
		if err != nil {
			log.Fatalf("Failed to read manifest at %s: %v", manifestPath, err)
		}

		var req events.ExecutionRequest
		if err := json.Unmarshal(data, &req); err != nil {
			log.Fatalf("Failed to unmarshal manifest: %v", err)
		}

		log.Printf("Loaded execution request %s for job %d", req.ExecutionRunID, req.UnifiedJobID)

		// Create event channel and publisher loop
		eventChan := make(chan events.JobEvent, 100)
		doneChan := make(chan bool)

		go func() {
			for evt := range eventChan {
				if err := publisher.PublishJobEvent(&evt); err != nil {
					log.Printf("Failed to publish event: %v", err)
				}
			}
			doneChan <- true
		}()

		// Run job. On failure, emit a JOB_FAILED event (as the daemon path does)
		// so the run is marked failed promptly instead of waiting for the
		// reconciler's heartbeat timeout, then flush before exiting.
		runErr := runner.Run(&req, eventChan)
		if runErr != nil {
			log.Printf("Job execution failed: %v", runErr)
			failMsg := fmt.Sprintf("Runner failed: %v", runErr)
			eventChan <- events.JobEvent{
				ExecutionRunID: req.ExecutionRunID,
				UnifiedJobID:   req.UnifiedJobID,
				EventType:      "JOB_FAILED",
				Timestamp:      time.Now(),
				StdoutSnippet:  &failMsg,
			}
		}

		close(eventChan)
		<-doneChan // Wait for events to flush
		if runErr != nil {
			os.Exit(1)
		}
		log.Println("One-shot execution finished successfully.")
		return
	}

	// Long-running (daemon) mode exposes Prometheus /metrics; one-shot exits too
	// fast to scrape.
	metrics.Serve("")

	// Recover interrupted localhost runs from their persisted WAL before serving
	// new work: a local run whose host-runner died with the executor container is
	// resumed from /var/lib/praetor/jobs instead of being declared lost (#45).
	runner.ResumeLocalJobs("")

	// 2. Create Agent (Daemon Mode)
	// We use NATS for Subscription (bus), and our selected publisher for Events
	agent := core.NewAgent(bus, publisher, runner, env.Int("EXECUTOR_WORKERS", 2))

	// 4. Start
	if err := agent.Start(); err != nil {
		log.Fatalf("Agent failed: %v", err)
	}
}
