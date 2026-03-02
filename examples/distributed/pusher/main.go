package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pb "object-queue/api/proto"
	"object-queue/pkg/broker"
	"object-queue/pkg/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	brokerAddr := os.Getenv("BROKER_ADDR")
	if brokerAddr == "" {
		store, err := createStorage(ctx)
		if err != nil {
			log.Fatalf("Failed to create storage for discovery: %v", err)
		}
		defer store.Close()

		brokerAddr, err = broker.DiscoverBroker(store)
		if err != nil {
			log.Fatalf("Failed to discover broker: %v", err)
		}
		log.Printf("Discovered broker at %s", brokerAddr)
	}

	//nolint:staticcheck
	conn, err := grpc.Dial(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	client := pb.NewQueueServiceClient(conn)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	jobCount := 0
	log.Printf("Pusher connected to broker at %s", brokerAddr)
	log.Println("Pushing jobs every 2 seconds. Press Ctrl+C to stop.")

	for {
		select {
		case <-sigCh:
			log.Println("Shutting down pusher...")
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			jobCount++
			resp, err := client.Push(ctx, &pb.PushRequest{
				Data: map[string]string{
					"task":      "process-item",
					"item_id":   fmt.Sprintf("item-%d", jobCount),
					"timestamp": time.Now().Format(time.RFC3339),
				},
			})
			if err != nil {
				log.Printf("Failed to push job: %v", err)
				continue
			}

			log.Printf("Pushed job %s (item-%d)", resp.Job.Id, jobCount)
		}
	}
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

	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		if !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") &&
			!strings.Contains(err.Error(), "BucketAlreadyExists") {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	return storage.NewS3StorageWithClient(s3Client, bucket, key, ctx)
}
