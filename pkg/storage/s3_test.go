package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"

	"object-queue/pkg/queue"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// mockS3Client implements S3API for testing.
type mockS3Client struct {
	mu   sync.Mutex
	data []byte
	etag string
}

func newMockS3Client(state *queue.QueueState) *mockS3Client {
	data, _ := json.MarshalIndent(state, "", "  ")
	return &mockS3Client{
		data: data,
		etag: fmt.Sprintf(`"%d"`, state.Version),
	}
}

func (m *mockS3Client) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.data == nil {
		return nil, &types.NoSuchKey{Message: aws.String("not found")}
	}

	dataCopy := make([]byte, len(m.data))
	copy(dataCopy, m.data)

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(dataCopy)),
		ETag: aws.String(m.etag),
	}, nil
}

func (m *mockS3Client) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check IfMatch (CAS semantics)
	if input.IfMatch != nil && *input.IfMatch != m.etag {
		return nil, &mockAPIError{code: "PreconditionFailed", message: "At least one of the pre-conditions you specified did not hold"}
	}

	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	m.data = data

	// Parse to get version for ETag
	var state queue.QueueState
	if err := json.Unmarshal(data, &state); err == nil {
		m.etag = fmt.Sprintf(`"%d"`, state.Version)
	}

	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.data == nil {
		return nil, &types.NotFound{Message: aws.String("not found")}
	}

	return &s3.HeadObjectOutput{
		ETag: aws.String(m.etag),
	}, nil
}

// mockAPIError implements smithy.APIError for testing precondition failures.
type mockAPIError struct {
	code    string
	message string
}

func (e *mockAPIError) Error() string            { return e.message }
func (e *mockAPIError) ErrorCode() string         { return e.code }
func (e *mockAPIError) ErrorMessage() string      { return e.message }
func (e *mockAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

func newTestS3Storage(mock *mockS3Client) *S3Storage {
	return &S3Storage{
		client: mock,
		bucket: "test-bucket",
		key:    "test-key",
		ctx:    context.Background(),
	}
}

func TestS3CASSuccess(t *testing.T) {
	mock := newMockS3Client(&queue.QueueState{Version: 1, Jobs: []queue.Job{}})
	store := newTestS3Storage(mock)

	newState := &queue.QueueState{
		Broker: "test-broker:50051",
		Jobs:   []queue.Job{},
	}

	err := store.CompareAndSwap(1, newState)
	if err != nil {
		t.Fatalf("CAS should succeed: %v", err)
	}
	if newState.Version != 2 {
		t.Errorf("Expected version 2, got %d", newState.Version)
	}

	// Verify the state was written
	state, err := store.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if state.Version != 2 {
		t.Errorf("Expected stored version 2, got %d", state.Version)
	}
	if state.Broker != "test-broker:50051" {
		t.Errorf("Expected broker test-broker:50051, got %s", state.Broker)
	}
}

func TestS3CASVersionMismatch(t *testing.T) {
	mock := newMockS3Client(&queue.QueueState{Version: 3, Jobs: []queue.Job{}})
	store := newTestS3Storage(mock)

	newState := &queue.QueueState{Jobs: []queue.Job{}}

	err := store.CompareAndSwap(1, newState) // wrong version
	if err != ErrCASFailure {
		t.Fatalf("Expected ErrCASFailure, got %v", err)
	}
}

func TestS3CASETagMismatch(t *testing.T) {
	mock := newMockS3Client(&queue.QueueState{Version: 1, Jobs: []queue.Job{}})
	store := newTestS3Storage(mock)

	// Simulate a concurrent write by changing the ETag between GetObject and PutObject
	concurrentMock := &concurrentWriteMock{
		inner:          mock,
		changeAfterGet: true,
	}
	store.client = concurrentMock

	newState := &queue.QueueState{Jobs: []queue.Job{}}
	err := store.CompareAndSwap(1, newState)
	if err != ErrCASFailure {
		t.Fatalf("Expected ErrCASFailure due to ETag mismatch, got %v", err)
	}
}

// concurrentWriteMock simulates a concurrent write happening between GetObject and PutObject.
type concurrentWriteMock struct {
	inner          *mockS3Client
	changeAfterGet bool
	getCalled      bool
}

func (m *concurrentWriteMock) GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	result, err := m.inner.GetObject(ctx, input, optFns...)
	if err != nil {
		return nil, err
	}
	if m.changeAfterGet && !m.getCalled {
		m.getCalled = true
		// Simulate another writer changing the object
		m.inner.mu.Lock()
		m.inner.etag = `"changed-by-another-writer"`
		m.inner.mu.Unlock()
	}
	return result, nil
}

func (m *concurrentWriteMock) PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return m.inner.PutObject(ctx, input, optFns...)
}

func (m *concurrentWriteMock) HeadObject(ctx context.Context, input *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return m.inner.HeadObject(ctx, input, optFns...)
}

func TestS3ReadNotFound(t *testing.T) {
	mock := &mockS3Client{} // no data = not found
	store := newTestS3Storage(mock)

	_, err := store.Read()
	if err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound, got %v", err)
	}
}
