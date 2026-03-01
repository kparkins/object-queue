package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"object-queue/pkg/broker"
	"object-queue/pkg/server"
	"object-queue/pkg/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Create storage
	store, err := createStorage(ctx)
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}
	defer store.Close()

	// Create broker
	brk := broker.NewBroker(broker.Config{
		Storage: store,
	})

	if err := brk.Start(ctx); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}
	defer brk.Stop()

	// Create gRPC server
	server := server.NewQueueServer(brk)

	// Start gRPC server in background
	go func() {
		if err := server.Serve(":50051"); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	log.Println("Broker running on :50051")
	log.Println("Press Ctrl+C to stop")

	// Wait for shutdown signal
	<-sigCh
	log.Println("Shutting down...")

	server.Stop()
	cancel()
	log.Println("Shutdown complete")
}

func getEnv(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func createStorage(ctx context.Context) (storage.Storage, error) {
	endpoint := getEnv("MINIO_ENDPOINT", "http://localhost:9000")

	accessKey := getEnv("MINIO_ACCESS_KEY", "minioadmin")
	secretKey := getEnv("MINIO_SECRET_KEY", "minioadmin")
	bucket := getEnv("MINIO_BUCKET", "object-queue")
	key := getEnv("MINIO_KEY", "queue.json")

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKey, secretKey, "",
		)),
	)
	if err != nil {
		return nil, err
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	// Ensure the bucket exists
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		// Ignore "bucket already exists" errors
		if !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") &&
			!strings.Contains(err.Error(), "BucketAlreadyExists") {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	log.Printf("Using MinIO storage at %s/%s/%s", endpoint, bucket, key)
	return storage.NewS3StorageWithClient(s3Client, bucket, key, ctx)
}
