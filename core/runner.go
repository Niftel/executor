package core

import (
	"time"

	"github.com/praetordev/events"
)

// Runner defines the interface for running a job.
type Runner interface {
	Run(req *events.ExecutionRequest, eventChan chan<- events.JobEvent) error
}

// MockRunner simulates an Ansible run.
type MockRunner struct{}

func (r *MockRunner) Run(req *events.ExecutionRequest, eventChan chan<- events.JobEvent) error {
	logger.Info("mock runner starting job", "job_id", req.UnifiedJobID, "run_id", req.ExecutionRunID)

	// 1. Emit JOB_STARTED
	eventChan <- events.JobEvent{
		ExecutionRunID: req.ExecutionRunID,
		UnifiedJobID:   req.UnifiedJobID,
		EventType:      "JOB_STARTED",
		Timestamp:      time.Now(),
	}

	// 2. Simulate some tasks
	tasks := []string{"Gathering Facts", "Install Nginx", "Start Service"}
	for i, task := range tasks {
		time.Sleep(500 * time.Millisecond) // Simulate work

		eventChan <- events.JobEvent{
			ExecutionRunID: req.ExecutionRunID,
			UnifiedJobID:   req.UnifiedJobID,
			EventType:      "TASK_OK",
			Timestamp:      time.Now(),
			TaskName:       stringPtr(task),
			Seq:            int64(i + 2), // 1 is started
		}
	}

	// 3. Emit JOB_COMPLETED
	eventChan <- events.JobEvent{
		ExecutionRunID: req.ExecutionRunID,
		UnifiedJobID:   req.UnifiedJobID,
		EventType:      "JOB_COMPLETED",
		Timestamp:      time.Now(),
		Seq:            int64(len(tasks) + 2),
	}

	logger.Info("mock runner finished job", "job_id", req.UnifiedJobID)
	return nil
}

func stringPtr(s string) *string {
	return &s
}
