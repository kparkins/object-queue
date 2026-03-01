package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "object-queue/api/proto"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	brokerAddr := "localhost:50051"
	if addr := os.Getenv("BROKER_ADDR"); addr != "" {
		brokerAddr = addr
	}

	workerID := uuid.New().String()

	//nolint:staticcheck
	conn, err := grpc.Dial(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	client := pb.NewQueueServiceClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Track active jobs for heartbeats
	var activeJobs sync.Map
	var wg sync.WaitGroup

	// Heartbeat loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				activeJobs.Range(func(key, _ any) bool {
					jobID := key.(string)
					_, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{
						JobId:    jobID,
						WorkerId: workerID,
					})
					if err != nil {
						log.Printf("Heartbeat failed for job %s: %v", jobID, err)
					}
					return true
				})
			}
		}
	}()

	// Semaphore for max concurrent jobs
	semaphore := make(chan struct{}, 3)

	log.Printf("Worker %s connected to broker at %s", workerID, brokerAddr)
	log.Println("Waiting for jobs. Press Ctrl+C to stop.")

	// Poll loop
	pollTicker := time.NewTicker(100 * time.Millisecond)
	defer pollTicker.Stop()

loop:
	for {
		select {
		case <-sigCh:
			log.Println("Shutting down worker...")
			break loop
		case <-ctx.Done():
			break loop
		case <-pollTicker.C:
			select {
			case semaphore <- struct{}{}:
				resp, err := client.Claim(ctx, &pb.ClaimRequest{WorkerId: workerID})
				if err != nil {
					<-semaphore
					if !strings.Contains(err.Error(), "no jobs available") {
						log.Printf("Failed to claim job: %v", err)
					}
					continue
				}

				job := resp.Job
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-semaphore }()

					activeJobs.Store(job.Id, true)
					defer activeJobs.Delete(job.Id)

					log.Printf("Processing job %s: %v", job.Id, job.Data)

					// Simulate work
					select {
					case <-time.After(20 * time.Millisecond):
					case <-ctx.Done():
						return
					}

					_, err := client.Complete(ctx, &pb.CompleteRequest{
						JobId:    job.Id,
						WorkerId: workerID,
					})
					if err != nil {
						log.Printf("Failed to complete job %s: %v", job.Id, err)
						return
					}

					log.Printf("Completed job %s", job.Id)
				}()
			default:
				// At max capacity
			}
		}
	}

	cancel()
	wg.Wait()
	log.Println("Worker stopped.")
}
