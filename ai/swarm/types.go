package swarm

import (
	"time"
)

// JobStatus represents the state of a distributed job.
type JobStatus string

const (
	JobStatusPending   JobStatus = "PENDING"
	JobStatusRunning   JobStatus = "RUNNING"
	JobStatusCompleted JobStatus = "COMPLETED"
	JobStatusFailed    JobStatus = "FAILED"
)

// Job represents a unit of work distributed to the swarm.
type Job struct {
	ID        string            `json:"id"`
	MacroName string            `json:"macro_name"`
	Params    map[string]string `json:"params"`
	// TargetFilter allows targeting specific nodes (e.g., "region=us-east").
	// If empty, any available worker can pick it up.
	TargetFilter map[string]string `json:"target_filter"`
	Status       JobStatus         `json:"status"`
	CreatedAt    time.Time         `json:"created_at"` // Date/time job was created
	CreatedBy    string            `json:"created_by"` // Agent ID or User ID
}

// JobResult represents the output of a job execution on a specific node.
type JobResult struct {
	JobID      string    `json:"job_id"`
	NodeID     string    `json:"node_id"`
	Status     JobStatus `json:"status"`
	Output     string    `json:"output"` // JSON string or plain text result
	Error      string    `json:"error,omitempty"`
	ExecutedAt time.Time `json:"executed_at"`
	Duration   string    `json:"duration"`
}
