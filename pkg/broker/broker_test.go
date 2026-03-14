package broker

import (
	"context"
	"os"
	"testing"
	"time"

	"object-queue/pkg/queue"
	"object-queue/pkg/storage"
)

// setupBroker creates a broker backed by a temp file for testing.
func setupBroker(t *testing.T, opts ...func(*Config)) (*Broker, func()) {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "broker-test-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	os.Remove(tmpPath) // Remove so NewFileStorage initializes it

	store, err := storage.NewFileStorage(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		t.Fatalf("Failed to create storage: %v", err)
	}

	cfg := Config{
		Storage:       store,
		Address:       "test-broker:50051",
		BatchInterval: 10 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	brk := NewBroker(cfg)

	ctx := context.Background()
	if err := brk.Start(ctx); err != nil {
		store.Close()
		os.Remove(tmpPath)
		t.Fatalf("Failed to start broker: %v", err)
	}

	cleanup := func() {
		brk.Stop()
		store.Close()
		os.Remove(tmpPath)
	}

	return brk, cleanup
}

// sendRequest sends a request to the broker and waits for the response.
func sendRequest(t *testing.T, brk *Broker, req queue.Request) queue.Response {
	t.Helper()
	ch := make(chan queue.Response, 1)
	req.ResponseCh = ch
	brk.HandleRequest(req)

	select {
	case resp := <-ch:
		return resp
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for broker response")
		return queue.Response{}
	}
}

func TestBrokerPushAndClaim(t *testing.T) {
	brk, cleanup := setupBroker(t)
	defer cleanup()

	// Push a job
	pushResp := sendRequest(t, brk, queue.Request{
		Type:    queue.RequestTypePush,
		JobData: map[string]interface{}{"task": "test-job"},
	})
	if !pushResp.Success {
		t.Fatalf("Push failed: %v", pushResp.Error)
	}
	if pushResp.Job == nil {
		t.Fatal("Push returned nil job")
	}
	if pushResp.Job.ID == "" {
		t.Fatal("Push returned empty job ID")
	}

	// Claim the job
	claimResp := sendRequest(t, brk, queue.Request{
		Type:     queue.RequestTypeClaim,
		WorkerID: "worker-1",
	})
	if !claimResp.Success {
		t.Fatalf("Claim failed: %v", claimResp.Error)
	}
	if claimResp.Job.ID != pushResp.Job.ID {
		t.Errorf("Claimed job ID %s != pushed job ID %s", claimResp.Job.ID, pushResp.Job.ID)
	}
	if claimResp.Job.Status != queue.JobStatusClaimed {
		t.Errorf("Expected status claimed, got %s", claimResp.Job.Status)
	}
	if claimResp.Job.WorkerID != "worker-1" {
		t.Errorf("Expected worker-1, got %s", claimResp.Job.WorkerID)
	}
}

func TestBrokerHeartbeat(t *testing.T) {
	brk, cleanup := setupBroker(t)
	defer cleanup()

	// Push and claim
	sendRequest(t, brk, queue.Request{
		Type:    queue.RequestTypePush,
		JobData: map[string]interface{}{"task": "hb-test"},
	})
	claimResp := sendRequest(t, brk, queue.Request{
		Type:     queue.RequestTypeClaim,
		WorkerID: "worker-1",
	})

	time.Sleep(20 * time.Millisecond)
	beforeHB := time.Now()

	// Heartbeat
	hbResp := sendRequest(t, brk, queue.Request{
		Type:     queue.RequestTypeHeartbeat,
		JobID:    claimResp.Job.ID,
		WorkerID: "worker-1",
	})
	if !hbResp.Success {
		t.Fatalf("Heartbeat failed: %v", hbResp.Error)
	}

	// Read state and verify heartbeat timestamp is recent
	state, err := brk.config.Storage.Read()
	if err != nil {
		t.Fatalf("Failed to read state: %v", err)
	}
	for _, job := range state.Jobs {
		if job.ID == claimResp.Job.ID {
			if job.LastHeartbeat == nil {
				t.Fatal("LastHeartbeat is nil after heartbeat")
			}
			if job.LastHeartbeat.Before(beforeHB) {
				t.Errorf("LastHeartbeat %v is before the heartbeat call at %v", *job.LastHeartbeat, beforeHB)
			}
			return
		}
	}
	t.Fatal("Job not found in state after heartbeat")
}

func TestBrokerComplete(t *testing.T) {
	brk, cleanup := setupBroker(t)
	defer cleanup()

	// Push and claim
	sendRequest(t, brk, queue.Request{
		Type:    queue.RequestTypePush,
		JobData: map[string]interface{}{"task": "complete-test"},
	})
	claimResp := sendRequest(t, brk, queue.Request{
		Type:     queue.RequestTypeClaim,
		WorkerID: "worker-1",
	})

	// Complete the job
	completeResp := sendRequest(t, brk, queue.Request{
		Type:     queue.RequestTypeComplete,
		JobID:    claimResp.Job.ID,
		WorkerID: "worker-1",
	})
	if !completeResp.Success {
		t.Fatalf("Complete failed: %v", completeResp.Error)
	}

	// Second claim should fail — no more jobs
	claimResp2 := sendRequest(t, brk, queue.Request{
		Type:     queue.RequestTypeClaim,
		WorkerID: "worker-2",
	})
	if claimResp2.Success {
		t.Fatal("Expected claim to fail after job completed, but it succeeded")
	}
}

func TestBrokerHeartbeatTimeout(t *testing.T) {
	brk, cleanup := setupBroker(t, func(cfg *Config) {
		cfg.HeartbeatTimeout = 50 * time.Millisecond
	})
	defer cleanup()

	// Push and claim
	sendRequest(t, brk, queue.Request{
		Type:    queue.RequestTypePush,
		JobData: map[string]interface{}{"task": "timeout-test"},
	})
	sendRequest(t, brk, queue.Request{
		Type:     queue.RequestTypeClaim,
		WorkerID: "worker-1",
	})

	// Wait for heartbeat timeout
	time.Sleep(100 * time.Millisecond)

	// The job should be reset to pending — another worker can claim it
	claimResp := sendRequest(t, brk, queue.Request{
		Type:     queue.RequestTypeClaim,
		WorkerID: "worker-2",
	})
	if !claimResp.Success {
		t.Fatalf("Expected timed-out job to be reclaimable, but claim failed: %v", claimResp.Error)
	}
	if claimResp.Job.WorkerID != "worker-2" {
		t.Errorf("Expected worker-2, got %s", claimResp.Job.WorkerID)
	}
}

func TestBrokerCompleteIdempotent(t *testing.T) {
	brk, cleanup := setupBroker(t)
	defer cleanup()

	// Complete a non-existent job — should succeed (idempotent)
	resp := sendRequest(t, brk, queue.Request{
		Type:     queue.RequestTypeComplete,
		JobID:    "non-existent-job",
		WorkerID: "worker-1",
	})
	if !resp.Success {
		t.Fatalf("Expected idempotent complete to succeed, got error: %v", resp.Error)
	}
}
