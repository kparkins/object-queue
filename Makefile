generate:
	protoc --go_out=. --go-grpc_out=. api/proto/queue.proto

build-example:
	go build -o examples/distributed/broker/broker ./examples/distributed/broker
	go build -o examples/distributed/pusher/pusher ./examples/distributed/pusher
	go build -o examples/distributed/worker/worker ./examples/distributed/worker

clean-example:
	rm -f examples/distributed/broker/broker examples/distributed/pusher/pusher examples/distributed/worker/worker
