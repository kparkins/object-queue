package queue

import "time"

// JobStatus represents the status of a job in the queue
type JobStatus string

const (
	// JobStatusPending indicates the job is waiting to be claimed
	JobStatusPending JobStatus = "pending"
	// JobStatusClaimed indicates the job has been claimed by a worker
	JobStatusClaimed JobStatus = "claimed"
	// JobStatusCompleted indicates the job has been completed
	JobStatusCompleted JobStatus = "completed"
)

// Job represents a single job in the queue
type Job struct {
	ID          string                 `json:"id"`
	Status      JobStatus              `json:"status"`
	Data        map[string]interface{} `json:"data"`
	CreatedAt   time.Time              `json:"created_at"`
	ClaimedAt   *time.Time             `json:"claimed_at,omitempty"`
	LastHeartbeat *time.Time           `json:"last_heartbeat,omitempty"`
	WorkerID    string                 `json:"worker_id,omitempty"`
}

// QueueState represents the entire state of the queue stored in object storage
type QueueState struct {
	Version int64  `json:"version"`
	Broker  string `json:"broker,omitempty"`
	Jobs    []Job  `json:"jobs"`
}

// Request represents a request from a client to the broker
type Request struct {
	Type      RequestType            `json:"type"`
	JobData   map[string]interface{} `json:"job_data,omitempty"`
	JobID     string                 `json:"job_id,omitempty"`
	WorkerID  string                 `json:"worker_id,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	ResponseCh chan<- Response       `json:"-"`
}

// RequestType specifies the type of request
type RequestType string

const (
	RequestTypePush      RequestType = "push"
	RequestTypeClaim     RequestType = "claim"
	RequestTypeHeartbeat RequestType = "heartbeat"
	RequestTypeComplete  RequestType = "complete"
)

// Response represents a response from the broker to a client
type Response struct {
	Success bool   `json:"success"`
	Job     *Job   `json:"job,omitempty"`
	Error   error  `json:"error,omitempty"`
}
