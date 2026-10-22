# SmoothMQ — Redis-backed SQS facade

This branch replaces the server's SQLite storage with Redis. Multiple stateless
broker processes share atomic Redis claims, fresh receipt handles, visibility
leases, retry counts and dead-letter queues. It implements a tested subset of
standard-queue SQS using the AWS JSON protocol and SigV4 authentication.

## Production configuration

Use Redis 7+ on persistent storage with AOF enabled, appendfsync=always,
maxmemory-policy=noeviction, stop-writes-on-bgsave-error=yes and
no-appendfsync-on-rewrite=no. Startup and readiness enforce these settings.
Redis Cluster is unsupported. See [the backend contract](docs/redis-backend.md).

Build with `go build -o smoothmq .` or the included Dockerfile. Supply a private
policy file and Redis connection secret:

```sh
./smoothmq server --redis-url-file=/run/secrets/redis-url \
  --sqs-policy-file=/run/secrets/sqs-policy \
  --sqs-region=us-east-1 --sqs-account-id=000000000001
```

The Redis URL file contains a redis:// or rediss:// URL. Policy JSON has this shape;
replace placeholders with independently generated secrets of at least 32 characters:

```json
{"keys":[{"accessKey":"worker-key","secretKey":"REPLACE_WITH_A_RANDOM_SECRET_AT_LEAST_32_CHARACTERS",
"actions":["SendMessage","ReceiveMessage","DeleteMessage","ChangeMessageVisibility","GetQueueAttributes"],
"queues":["jobs"]}]}
```

A separate operator key can use actions=["*"] and queues=["*"]. Never embed real
credentials in source or images. Configure a private/TLS proxy for remote clients
and --sqs-endpoint-override for its URL. Defaults bind SQS on localhost:3001 and
metrics on localhost:2112. Dashboard and external telemetry are disabled.

Clients set an explicit endpoint and credentials on their SQS client. Queue URLs
alone may not select the endpoint. Do not replace credentials for unrelated AWS
services. The /health/live and /health/ready endpoints report process and storage
health. Single-port mode mounts SQS under /sqs.

## Tests

```sh
go vet ./...
go test -race ./...
go build -race -o /tmp/smoothmq-race .
python3 -m venv .venv
.venv/bin/pip install boto3==1.43.116 botocore==1.43.116
.venv/bin/python integration/critic_acceptance.py --binary /tmp/smoothmq-race --slow
```

The [independent acceptance suite](integration/critic_README.md) starts isolated
Redis and two brokers and verifies concurrency, stale receipts, batch APIs,
DLQ/redrive, retention, authentication, outage and crash recovery.

For development only, --redis-require-durable=false permits non-durable Redis and
default development keys without a policy file. Never use this mode for real jobs.
