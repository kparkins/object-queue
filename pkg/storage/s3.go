package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"object-queue/pkg/queue"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Storage implements Storage using AWS S3 or S3-compatible storage (MinIO, etc.)
type S3Storage struct {
	client *s3.Client
	bucket string
	key    string
	ctx    context.Context
}

// S3Config holds configuration for S3 storage
type S3Config struct {
	// AWS SDK config
	AWSConfig aws.Config

	// Bucket name
	Bucket string

	// Object key (e.g., "queue.json")
	Key string

	// Context for operations (optional)
	Context context.Context
}

// NewS3Storage creates a new S3-based storage
func NewS3Storage(config S3Config) (*S3Storage, error) {
	if config.Bucket == "" {
		return nil, fmt.Errorf("bucket name is required")
	}
	if config.Key == "" {
		return nil, fmt.Errorf("object key is required")
	}
	if config.Context == nil {
		config.Context = context.Background()
	}

	client := s3.NewFromConfig(config.AWSConfig)

	return NewS3StorageWithClient(client, config.Bucket, config.Key, config.Context)
}

// NewS3StorageWithClient creates a new S3-based storage with a pre-configured client
// This is useful for custom endpoint configurations (e.g., MinIO)
func NewS3StorageWithClient(client *s3.Client, bucket, key string, ctx context.Context) (*S3Storage, error) {
	if bucket == "" {
		return nil, fmt.Errorf("bucket name is required")
	}
	if key == "" {
		return nil, fmt.Errorf("object key is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s3Storage := &S3Storage{
		client: client,
		bucket: bucket,
		key:    key,
		ctx:    ctx,
	}

	// Initialize the object if it doesn't exist
	if err := s3Storage.initializeIfNotExists(); err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	return s3Storage, nil
}

// initializeIfNotExists creates the initial queue state if the object doesn't exist
func (s *S3Storage) initializeIfNotExists() error {
	// Try to read the object
	_, err := s.client.HeadObject(s.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	})

	if err == nil {
		// Object exists
		return nil
	}

	// Check if it's a not found error
	var notFound *types.NotFound
	var noSuchKey *types.NoSuchKey
	if !(errors.As(err, &notFound) || errors.As(err, &noSuchKey)) {
		return fmt.Errorf("failed to check if object exists: %w", err)
	}

	// Object doesn't exist, create it
	initialState := &queue.QueueState{
		Version: 1,
		Jobs:    []queue.Job{},
	}

	data, err := json.MarshalIndent(initialState, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal initial state: %w", err)
	}

	_, err = s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("failed to create initial object: %w", err)
	}

	return nil
}

// Read reads the current queue state from S3
func (s *S3Storage) Read() (*queue.QueueState, error) {
	result, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	})
	if err != nil {
		var notFound *types.NotFound
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &notFound) || errors.As(err, &noSuchKey) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body: %w", err)
	}

	var state queue.QueueState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return &state, nil
}

// CompareAndSwap atomically updates the queue state using S3's conditional writes
// S3 doesn't have built-in CAS, so we use ETag-based conditional updates
func (s *S3Storage) CompareAndSwap(expectedVersion int64, newState *queue.QueueState) error {
	// First, read to get the current ETag and verify version
	headResult, err := s.client.HeadObject(s.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	})
	if err != nil {
		var notFound *types.NotFound
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &notFound) || errors.As(err, &noSuchKey) {
			return ErrNotFound
		}
		return fmt.Errorf("failed to head object: %w", err)
	}

	// Read the current state to verify version
	currentState, err := s.Read()
	if err != nil {
		return err
	}

	// Check version match
	if currentState.Version != expectedVersion {
		return ErrCASFailure
	}

	// Increment version
	newState.Version = expectedVersion + 1

	// Marshal new state
	data, err := json.MarshalIndent(newState, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal new state: %w", err)
	}

	// Use If-Match to ensure the object hasn't changed
	// This provides the CAS semantics we need
	_, err = s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket:  aws.String(s.bucket),
		Key:     aws.String(s.key),
		Body:    bytes.NewReader(data),
		IfMatch: headResult.ETag,
	})
	if err != nil {
		// Check if it's a precondition failure (ETag mismatch)
		// AWS SDK v2 returns this as a regular error with specific error code
		if err.Error() != "" && (bytes.Contains([]byte(err.Error()), []byte("PreconditionFailed")) ||
			bytes.Contains([]byte(err.Error()), []byte("412"))) {
			return ErrCASFailure
		}
		return fmt.Errorf("failed to put object: %w", err)
	}

	return nil
}

// Close closes the S3 storage connection
func (s *S3Storage) Close() error {
	// S3 client doesn't need explicit cleanup
	return nil
}
