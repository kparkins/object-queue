package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"object-queue/pkg/queue"
	"object-queue/pkg/storage"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
)

const (
	// DefaultBatchInterval is the maximum time to wait before flushing a batch
	DefaultBatchInterval = 100 * time.Millisecond

	// DefaultHeartbeatTimeout is the time after which a job is considered abandoned
	DefaultHeartbeatTimeout = 15 * time.Second

	// DefaultBrokerCheckInterval is how often to check if we're still the active broker
	DefaultBrokerCheckInterval = 3 * time.Second

	DefaultBufferSize = 10000
)

// Config holds broker configuration
type Config struct {
	Storage             storage.Storage
	Address             string
	BatchInterval       time.Duration
	HeartbeatTimeout    time.Duration
	BrokerCheckInterval time.Duration
	BufferSize          int
}

// Broker manages the queue and handles client requests
type Broker struct {
	config     Config
	requestCh  chan queue.Request
	shutdownCh chan struct{}
	closeOnce  sync.Once
	wg         sync.WaitGroup
}

// NewBroker creates a new broker
func NewBroker(config Config) *Broker {
	if config.BatchInterval == 0 {
		config.BatchInterval = DefaultBatchInterval
	}
	if config.HeartbeatTimeout == 0 {
		config.HeartbeatTimeout = DefaultHeartbeatTimeout
	}
	if config.BrokerCheckInterval == 0 {
		config.BrokerCheckInterval = DefaultBrokerCheckInterval
	}
	if config.BufferSize == 0 {
		config.BufferSize = DefaultBufferSize
	}

	return &Broker{
		config:     config,
		requestCh:  make(chan queue.Request, config.BufferSize),
		shutdownCh: make(chan struct{}),
	}
}

// Start starts the broker
func (b *Broker) Start(ctx context.Context) error {
	if err := b.register(); err != nil {
		return fmt.Errorf("failed to register broker: %w", err)
	}

	b.wg.Add(2)
	go b.groupCommitLoop(ctx)
	go b.brokerCheckLoop(ctx)

	log.Println("Broker started")
	return nil
}

// Stop stops the broker
func (b *Broker) Stop() {
	b.closeOnce.Do(func() { close(b.shutdownCh) })
	b.wg.Wait()
	log.Println("Broker stopped")
}

// Done returns a channel that is closed when the broker shuts down
func (b *Broker) Done() <-chan struct{} {
	return b.shutdownCh
}

// DiscoverBroker reads storage and returns the current broker address
func DiscoverBroker(s storage.Storage) (string, error) {
	state, err := s.Read()
	if err != nil {
		return "", fmt.Errorf("failed to read state: %w", err)
	}
	if state.Broker == "" {
		return "", errors.New("no broker registered")
	}
	return state.Broker, nil
}

var errBrokerReplaced = errors.New("broker has been replaced")

func (b *Broker) register() error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 5 * time.Second

	return backoff.Retry(func() error {
		state, err := b.config.Storage.Read()
		if err != nil {
			return fmt.Errorf("failed to read state: %w", err)
		}
		newState := *state
		newState.Jobs = make([]queue.Job, len(state.Jobs))
		copy(newState.Jobs, state.Jobs)
		newState.Broker = b.config.Address

		err = b.config.Storage.CompareAndSwap(state.Version, &newState)
		if err == storage.ErrCASFailure {
			return err
		}
		if err != nil {
			return backoff.Permanent(err)
		}

		log.Printf("Registered as active broker at %s", b.config.Address)
		return nil
	}, bo)
}

func (b *Broker) brokerCheckLoop(ctx context.Context) {
	defer b.wg.Done()

	ticker := time.NewTicker(b.config.BrokerCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.shutdownCh:
			return
		case <-ticker.C:
			state, err := b.config.Storage.Read()
			if err != nil {
				log.Printf("Broker check failed: %v", err)
				continue
			}
			if state.Broker != b.config.Address {
				log.Printf("Broker replaced by %s, shutting down", state.Broker)
				b.closeOnce.Do(func() { close(b.shutdownCh) })
				return
			}
		}
	}
}

// HandleRequest handles a request from a client
func (b *Broker) HandleRequest(req queue.Request) {
	select {
	case b.requestCh <- req:
	case <-b.shutdownCh:
		req.ResponseCh <- queue.Response{
			Success: false,
			Error:   errors.New("broker is shutting down"),
		}
	}
}

// groupCommitLoop runs the group commit loop
func drainRequests(ch chan queue.Request, batch []queue.Request) []queue.Request {
	for {
		select {
		case req := <-ch:
			batch = append(batch, req)
		default:
			return batch
		}
	}
}

func (b *Broker) groupCommitLoop(ctx context.Context) {
	defer b.wg.Done()

	timer := time.NewTimer(b.config.BatchInterval)
	defer timer.Stop()

	batch := make([]queue.Request, 0, b.config.BufferSize)

	flush := func() {
		defer timer.Reset(b.config.BatchInterval)
		batch = drainRequests(b.requestCh, batch)
		if len(batch) <= 0 {
			return
		}
		if err := b.processRequests(batch); err != nil {
			log.Printf("Error processing requests: %v", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-b.shutdownCh:
			flush()
			return
		case <-timer.C:
			flush()
		}
	}
}

// processRequests processes a batch of requests with exponential backoff
func (b *Broker) processRequests(requests []queue.Request) error {
	// Configure exponential backoff
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 10 * time.Millisecond
	bo.MaxInterval = 50 * time.Millisecond
	bo.MaxElapsedTime = 1 * time.Second

	var responses []queue.Response

	// Operation to retry
	operation := func() error {
		state, err := b.config.Storage.Read()
		if err != nil {
			return fmt.Errorf("failed to read state: %w", err)
		}

		if state.Broker != b.config.Address {
			log.Printf("Broker replaced by %s, shutting down", state.Broker)
			b.closeOnce.Do(func() { close(b.shutdownCh) })
			return backoff.Permanent(errBrokerReplaced)
		}

		newState := *state
		newState.Jobs = make([]queue.Job, len(state.Jobs))
		copy(newState.Jobs, state.Jobs)

		// Clean up timed-out jobs
		b.cleanupTimedOutJobs(&newState)

		// Process each request
		responses = make([]queue.Response, len(requests))
		for idx, req := range requests {
			responses[idx] = b.handleSingleRequest(&newState, req)
		}

		err = b.config.Storage.CompareAndSwap(state.Version, &newState)
		if err != nil {
			if err == storage.ErrCASFailure {
				return err
			}
			// Other errors are not retryable
			return backoff.Permanent(fmt.Errorf("storage error: %w", err))
		}

		return nil
	}

	// Retry with backoff
	err := backoff.Retry(operation, bo)
	if err != nil {
		// Failed, send error responses
		for _, req := range requests {
			req.ResponseCh <- queue.Response{
				Success: false,
				Error:   err,
			}
		}
		return err
	}

	// Success, send responses
	for idx, req := range requests {
		req.ResponseCh <- responses[idx]
	}
	return nil
}

// handleSingleRequest handles a single request and updates the state
func (b *Broker) handleSingleRequest(state *queue.QueueState, req queue.Request) queue.Response {
	switch req.Type {
	case queue.RequestTypePush:
		return b.handlePush(state, req)
	case queue.RequestTypeClaim:
		return b.handleClaim(state, req)
	case queue.RequestTypeHeartbeat:
		return b.handleHeartbeat(state, req)
	case queue.RequestTypeComplete:
		return b.handleComplete(state, req)
	default:
		return queue.Response{
			Success: false,
			Error:   fmt.Errorf("unknown request type: %s", req.Type),
		}
	}
}

// handlePush handles a push request
func (b *Broker) handlePush(state *queue.QueueState, req queue.Request) queue.Response {
	job := queue.Job{
		ID:        uuid.New().String(),
		Status:    queue.JobStatusPending,
		Data:      req.JobData,
		CreatedAt: time.Now(),
	}

	state.Jobs = append(state.Jobs, job)

	return queue.Response{
		Success: true,
		Job:     &job,
	}
}

// handleClaim handles a claim request
func (b *Broker) handleClaim(state *queue.QueueState, req queue.Request) queue.Response {
	// Find first unclaimed job
	for i := range state.Jobs {
		if state.Jobs[i].Status == queue.JobStatusPending {
			now := time.Now()
			state.Jobs[i].Status = queue.JobStatusClaimed
			state.Jobs[i].ClaimedAt = &now
			state.Jobs[i].LastHeartbeat = &now
			state.Jobs[i].WorkerID = req.WorkerID

			return queue.Response{
				Success: true,
				Job:     &state.Jobs[i],
			}
		}
	}

	return queue.Response{
		Success: false,
		Error:   errors.New("no jobs available"),
	}
}

// handleHeartbeat handles a heartbeat request
func (b *Broker) handleHeartbeat(state *queue.QueueState, req queue.Request) queue.Response {
	for i := range state.Jobs {
		if state.Jobs[i].ID == req.JobID && state.Jobs[i].WorkerID == req.WorkerID {
			state.Jobs[i].LastHeartbeat = &req.Timestamp
			return queue.Response{Success: true}
		}
	}

	return queue.Response{
		Success: false,
		Error:   errors.New("job not found or worker mismatch"),
	}
}

// handleComplete handles a complete request. Idempotent — if the job is
// already gone, we treat it as a successful completion (duplicate request).
func (b *Broker) handleComplete(state *queue.QueueState, req queue.Request) queue.Response {
	for i := range state.Jobs {
		if state.Jobs[i].ID == req.JobID && state.Jobs[i].WorkerID == req.WorkerID {
			log.Printf("Removing completed job %s from queue", req.JobID)
			state.Jobs = append(state.Jobs[:i], state.Jobs[i+1:]...)
			return queue.Response{Success: true}
		}
	}
	return queue.Response{Success: true}
}

// cleanupTimedOutJobs resets jobs that have timed out
func (b *Broker) cleanupTimedOutJobs(state *queue.QueueState) {
	now := time.Now()
	for i := range state.Jobs {
		if state.Jobs[i].Status == queue.JobStatusClaimed &&
			state.Jobs[i].LastHeartbeat != nil &&
			now.Sub(*state.Jobs[i].LastHeartbeat) > b.config.HeartbeatTimeout {
			// Reset the job
			state.Jobs[i].Status = queue.JobStatusPending
			state.Jobs[i].ClaimedAt = nil
			state.Jobs[i].LastHeartbeat = nil
			state.Jobs[i].WorkerID = ""
			log.Printf("Job %s timed out, resetting", state.Jobs[i].ID)
		}
	}
}
