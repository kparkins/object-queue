package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"object-queue/pkg/queue"
)

var (
	// ErrCASFailure indicates a compare-and-swap operation failed
	ErrCASFailure = errors.New("compare-and-swap failed: version mismatch")
	// ErrNotFound indicates the object was not found
	ErrNotFound = errors.New("object not found")
)

// Storage defines the interface for object storage operations
type Storage interface {
	// Read reads the current queue state
	Read() (*queue.QueueState, error)

	// CompareAndSwap atomically updates the queue state only if the version matches
	CompareAndSwap(expectedVersion int64, newState *queue.QueueState) error

	// Close closes the storage connection
	Close() error
}

// FileStorage implements Storage using a local file
// This is a simple implementation for demonstration and testing
type FileStorage struct {
	filePath string
	mu       sync.RWMutex
}

// NewFileStorage creates a new file-based storage
func NewFileStorage(filePath string) (*FileStorage, error) {
	fs := &FileStorage{
		filePath: filePath,
	}

	// Initialize the file if it doesn't exist
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		initialState := &queue.QueueState{
			Version: 1,
			Jobs:    []queue.Job{},
		}
		data, err := json.MarshalIndent(initialState, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("failed to marshal initial state: %w", err)
		}
		if err := os.WriteFile(filePath, data, 0644); err != nil {
			return nil, fmt.Errorf("failed to write initial state: %w", err)
		}
	}

	return fs, nil
}

// Read reads the current queue state
func (fs *FileStorage) Read() (*queue.QueueState, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	data, err := os.ReadFile(fs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var state queue.QueueState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return &state, nil
}

// CompareAndSwap atomically updates the queue state only if the version matches
func (fs *FileStorage) CompareAndSwap(expectedVersion int64, newState *queue.QueueState) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Read current state
	data, err := os.ReadFile(fs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return fmt.Errorf("failed to read file: %w", err)
	}

	var currentState queue.QueueState
	if err := json.Unmarshal(data, &currentState); err != nil {
		return fmt.Errorf("failed to unmarshal state: %w", err)
	}

	// Check version
	if currentState.Version != expectedVersion {
		return ErrCASFailure
	}

	// Increment version for new state
	newState.Version = expectedVersion + 1

	// Write new state
	data, err = json.MarshalIndent(newState, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal new state: %w", err)
	}

	if err := os.WriteFile(fs.filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// Close closes the storage connection
func (fs *FileStorage) Close() error {
	return nil
}
