package storage

import (
	"os"
	"testing"

	"object-queue/pkg/queue"
)

func TestFileStorage(t *testing.T) {
	tmpFile := "/tmp/test-queue.json"
	defer os.Remove(tmpFile)

	storage, err := NewFileStorage(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Test Read
	state, err := storage.Read()
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if state.Version != 1 {
		t.Errorf("Expected version 1, got %d", state.Version)
	}

	// Test CompareAndSwap success
	newState := *state
	newState.Broker = "localhost:8080"
	err = storage.CompareAndSwap(state.Version, &newState)
	if err != nil {
		t.Fatalf("CAS failed: %v", err)
	}

	// Verify the write
	state, err = storage.Read()
	if err != nil {
		t.Fatalf("Failed to read after CAS: %v", err)
	}
	if state.Version != 2 {
		t.Errorf("Expected version 2, got %d", state.Version)
	}
	if state.Broker != "localhost:8080" {
		t.Errorf("Expected broker localhost:8080, got %s", state.Broker)
	}

	// Test CompareAndSwap failure (version mismatch)
	newState = *state
	newState.Broker = "localhost:9090"
	err = storage.CompareAndSwap(1, &newState) // Wrong version
	if err != ErrCASFailure {
		t.Errorf("Expected CAS failure, got %v", err)
	}
}

func TestCompareAndSwapConcurrency(t *testing.T) {
	tmpFile := "/tmp/test-queue-concurrent.json"
	defer os.Remove(tmpFile)

	storage, err := NewFileStorage(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	state, _ := storage.Read()
	initialVersion := state.Version

	// Try concurrent writes - only one should succeed
	done := make(chan error, 2)

	for i := 0; i < 2; i++ {
		go func(id int) {
			newState := queue.QueueState{
				Version: initialVersion,
				Jobs:    []queue.Job{{ID: string(rune(id))}},
			}
			done <- storage.CompareAndSwap(initialVersion, &newState)
		}(i)
	}

	// Collect results
	var successes, failures int
	for i := 0; i < 2; i++ {
		err := <-done
		if err == nil {
			successes++
		} else if err == ErrCASFailure {
			failures++
		}
	}

	// Exactly one should succeed
	if successes != 1 {
		t.Errorf("Expected 1 success, got %d", successes)
	}
	if failures != 1 {
		t.Errorf("Expected 1 failure, got %d", failures)
	}
}
