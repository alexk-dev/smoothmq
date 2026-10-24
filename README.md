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

## Connections and health probes

`--sqs-max-connections` (`Q_SQS_MAX_CONNECTIONS`) limits concurrent connections
on the SQS listener, including idle keep-alive connections and long polls. The
default is **1024**; valid values are **1–65536**. In single-port mode the same
limit applies to the combined listener, including dashboard and metrics traffic.
Excess connections receive HTTP 503 before routing. Higher limits allow more
long polls but also consume more sockets, memory and backend capacity. Size the
limit and container memory together for your payload sizes and workload; 1024
simultaneous maximum-size requests can consume many GiB, unlike mostly idle polls.

`--sqs-health-port` (`Q_SQS_HEALTH_PORT`) defaults to **3002**; **0** disables it
(valid range **0–65535**). This separate listener has its own 32-connection budget
and serves only `/health/live` and `/health/ready`, without SQS authentication.
It binds to `--sqs-host` normally and `--host` in single-port mode. It remains
separate in single-port mode so SQS saturation cannot starve probes; set it to 0
if strictly one listening port is required. It runs only when SQS is enabled.
Readiness uses the same broker checks, including Redis PING and the durability
policy; a failed check returns 503. Liveness reports that the process is serving.
A health port bind failure fails server startup; shutdown closes all listeners.

For Kubernetes, point liveness and readiness probes to **port 3002** using
`/health/live` and `/health/ready`, and bind the listener to a reachable address
(e.g. `--sqs-host=0.0.0.0`, or `--host=0.0.0.0` in single-port mode). Existing
health endpoints on SQS port **3001** remain available, but share its connection
limit. In single-port mode those existing endpoints are `/sqs/health/live` and
`/sqs/health/ready`. Metrics on **2112** are unchanged. Update probe ports when
rolling out to gain saturation isolation; increasing the SQS limit alone cannot
guarantee health access on the main listener.

The options also work with `--config=config.yaml` using Kong's flat option keys:

```yaml
server:
  sqs-max-connections: 1024
  sqs-health-port: 3002
```

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
