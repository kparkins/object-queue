package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"object-queue/examples/distributed/shared"
	"object-queue/pkg/broker"
	"object-queue/pkg/server"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Create storage
	store, err := shared.CreateStorage(ctx)
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}
	defer store.Close()

	listenAddr := shared.GetEnv("LISTEN_ADDR", ":50051")
	advertiseAddr := shared.GetEnv("ADVERTISE_ADDR", listenAddr)

	// Create broker
	brk := broker.NewBroker(broker.Config{
		Storage: store,
		Address: advertiseAddr,
	})

	if err := brk.Start(ctx); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}
	defer brk.Stop()

	// Create gRPC server
	server := server.NewQueueServer(brk)

	// Start gRPC server in background
	go func() {
		if err := server.Serve(listenAddr); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	log.Printf("Broker running on %s (advertising as %s)", listenAddr, advertiseAddr)
	log.Println("Press Ctrl+C to stop")

	// Wait for shutdown signal or broker replacement
	select {
	case <-sigCh:
		log.Println("Shutting down...")
	case <-brk.Done():
		log.Println("Broker replaced, shutting down...")
	}

	server.Stop()
	cancel()
	log.Println("Shutdown complete")
}
