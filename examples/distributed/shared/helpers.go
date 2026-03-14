package shared

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"object-queue/pkg/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// GetEnv returns the value of the environment variable or the default value.
func GetEnv(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// CreateStorage creates an S3-compatible storage client using environment variables.
func CreateStorage(ctx context.Context) (storage.Storage, error) {
	endpoint := GetEnv("MINIO_ENDPOINT", "http://localhost:9000")
	accessKey := GetEnv("MINIO_ACCESS_KEY", "minioadmin")
	secretKey := GetEnv("MINIO_SECRET_KEY", "minioadmin")
	bucket := GetEnv("MINIO_BUCKET", "object-queue")
	key := GetEnv("MINIO_KEY", "queue.json")

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
		var baeOwned *types.BucketAlreadyOwnedByYou
		var baeExists *types.BucketAlreadyExists
		if !(errors.As(err, &baeOwned) || errors.As(err, &baeExists)) {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	log.Printf("Using MinIO storage at %s/%s/%s", endpoint, bucket, key)
	return storage.NewS3StorageWithClient(s3Client, bucket, key, ctx)
}
