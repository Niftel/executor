package core

import (
	"fmt"
	"github.com/praetordev/plog"
	"sync"
	"time"

	"github.com/praetordev/events"
)

// logger is the executor package component logger; the composition root
// installs the handler (pkg/plog).
var logger = plog.New("executor")

type Agent struct {
	Subscriber EventSubscriber
	Publisher  EventPublisher
	Runner     Runner
	Workers    int
	wg         sync.WaitGroup
}

func NewAgent(sub EventSubscriber, pub EventPublisher, runner Runner, workers int) *Agent {
	if workers <= 0 {
		workers = 2
	}
	return &Agent{
		Subscriber: sub,
		Publisher:  pub,
		Runner:     runner,
		Workers:    workers,
	}
}

func (a *Agent) Start() error {
	reqChan, err := a.Subscriber.SubscribeToExecutionRequests()
	if err != nil {
		return err
	}

	logger.Info("agent started, waiting for jobs")

	// simple worker pool
	for i := 0; i < a.Workers; i++ {
		a.wg.Add(1)
		go a.worker(i, reqChan)
	}

	a.wg.Wait()
	return nil
}

func (a *Agent) worker(id int, reqChan <-chan events.ExecutionRequest) {
	defer a.wg.Done()
	logger.Info("worker started", "worker", id)

	for req := range reqChan {
		logger.Info("worker picked up run", "worker", id, "run_id", req.ExecutionRunID)
		a.processRequest(req)
	}
}

// executorSeqBase reserves a high sequence range for executor-emitted events
// (bootstrap failures, inventory-sync lifecycle) so they never collide with the
// host-runner's own seqs, which start at 1 and grow with task count. The consumer
// dedups on (execution_run_id, seq); without this reservation an executor
// bootstrap-failure JOB_FAILED at seq 1 would collide with the host-runner's
// seq-1 event and one would be silently dropped. No single run emits a billion
// host-runner events, so the range is collision-free and stays ascending.
const executorSeqBase = 1_000_000_000

// publishWithRetry publishes an event with a few bounded retries so a transient
// publish failure doesn't silently drop the event (terminal events especially).
func (a *Agent) publishWithRetry(evt *events.JobEvent) error {
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		if err = a.Publisher.PublishJobEvent(evt); err == nil {
			return nil
		}
		logger.Warn("publish event failed", "event_type", evt.EventType, "run_id", evt.ExecutionRunID, "attempt", attempt, "err", err)
		time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
	}
	return err
}

func (a *Agent) processRequest(req events.ExecutionRequest) {
	// Channel to receive events from the runner
	// We make it buffered so the runner doesn't block too easily
	eventChan := make(chan events.JobEvent, 100)

	// Publish executor-side events under the reserved high seq range, retrying so a
	// terminal event (e.g. a bootstrap-failure JOB_FAILED) isn't silently lost —
	// dropping it would leave the job hanging until the scheduler's timeout.
	go func() {
		seq := int64(executorSeqBase)
		for evt := range eventChan {
			evt.Seq = seq
			if err := a.publishWithRetry(&evt); err != nil {
				logger.Error("gave up publishing event after retries", "event_type", evt.EventType, "run_id", evt.ExecutionRunID, "err", err)
			}
			seq++
		}
	}()

	// Run the job (blocking for this worker)
	if err := a.Runner.Run(&req, eventChan); err != nil {
		logger.Error("job run failed", "err", err)
		// Emit JOB_FAILED event since runner didn't succeed
		failMsg := fmt.Sprintf("Runner failed: %v", err)
		eventChan <- events.JobEvent{
			ExecutionRunID: req.ExecutionRunID,
			UnifiedJobID:   req.UnifiedJobID,
			EventType:      "JOB_FAILED",
			Timestamp:      time.Now(),
			StdoutSnippet:  &failMsg,
		}
	}
	close(eventChan)
}
