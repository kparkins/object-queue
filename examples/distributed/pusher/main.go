package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "object-queue/api/proto"
	"object-queue/examples/distributed/shared"
	"object-queue/pkg/broker"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	brokerAddr := os.Getenv("BROKER_ADDR")
	if brokerAddr == "" {
		store, err := shared.CreateStorage(ctx)
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
