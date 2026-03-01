# Distributed Example

Demonstrates the queue running as separate processes communicating over gRPC, with MinIO as the object storage backend:

- **minio** — S3-compatible object storage holding `queue.json`
- **broker** — reads/writes queue state to MinIO via CAS, serves gRPC on `:50051`
- **pusher** — connects to the broker and pushes a job every 2 seconds
- **worker** — connects to the broker and processes jobs as they arrive

## Run with Docker Compose

```bash
docker compose up
```

This starts MinIO, creates the `object-queue` bucket, then launches the broker, pusher, and worker. A shared Go module cache volume avoids re-downloading dependencies on each start.

Scale up workers to see concurrent processing:

```bash
docker compose up --scale worker=3
```

The MinIO console is available at http://localhost:9001 (login: `minioadmin` / `minioadmin`).

## Run Locally

Start MinIO (or use an existing S3 endpoint), then run each process in a separate terminal from the module root:

```bash
# Terminal 1 — broker (falls back to file storage if MINIO_ENDPOINT is unset)
MINIO_ENDPOINT=http://localhost:9000 go run ./examples/distributed/broker/main.go

# Terminal 2 — pusher
go run ./examples/distributed/pusher/main.go

# Terminal 3 — worker
go run ./examples/distributed/worker/main.go
```

### Broker Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MINIO_ENDPOINT` | _(unset, uses file storage)_ | S3/MinIO endpoint URL |
| `MINIO_ACCESS_KEY` | `minioadmin` | Access key |
| `MINIO_SECRET_KEY` | `minioadmin` | Secret key |
| `MINIO_BUCKET` | `object-queue` | Bucket name |
| `MINIO_KEY` | `queue.json` | Object key for queue state |

### Pusher/Worker Environment Variables

| Variable | Default | Description |
|---|---|---|
| `BROKER_ADDR` | `localhost:50051` | Broker gRPC address |

## What to Expect

```
minio-init | Bucket created
broker     | Using MinIO storage at http://minio:9000/object-queue/queue.json
broker     | Broker running on :50051
pusher     | Pushed job <id> (item-1)
worker     | Processing job <id>: map[item_id:item-1 task:process-item ...]
worker     | Finished job <id>
pusher     | Pushed job <id> (item-2)
...
```

To inspect the queue state stored in MinIO:

```bash
docker compose exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker compose exec minio mc cat local/object-queue/queue.json | jq .
```
