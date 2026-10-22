# Redis backend contract

Redis stores queue metadata, message bodies, ready/in-flight indexes, original
send times and move tasks. Lua scripts claim messages atomically across processes.
Redis server time controls leases. Each delivery gets a fresh random receipt;
acknowledgment and visibility changes require its unexpired ownership. Queue
generations fence purge, deletion and recreation.

## Delivery and dead-lettering

Delivery is at-least-once. Lease expiry, lost responses and retried sends can cause
redelivery or duplicate messages. Consumers must retain durable job identifiers,
idempotency and application-level leases. External side effects are not exactly-once.

After maxReceiveCount eligible deliveries, the next receive attempt moves a message
to its DLQ atomically. Original send time/source ARN are preserved. DLQ retention
uses original age. Manual redrive creates a fresh identity/time/count and persists
progress across restarts. Cancellation stops future batches. Redrive requires
source receive/delete and destination send permissions.

Expiration removes bodies and indexes in bounded batches of 256, both on access
and via incremental background scanning. At most 1,000 queues and 100 retained
move tasks exist per configured account. Only one active move task per source is
allowed. Effective move rate is at most 256 messages/second per task.

## API support

Supported: Create/Get/List/DeleteQueue, Get/SetQueueAttributes, PurgeQueue,
Send/Receive/DeleteMessage, ChangeMessageVisibility, all three batch variants,
queue tags and Start/List/CancelMessageMoveTask. Batch failures are per entry;
invalid duplicate IDs/oversized batches fail before mutations. Message attributes,
MD5 checksums, receive count, timestamps and DLQ provenance are supported.

Limits: 10 messages/batch, 1 MiB body plus attributes, 10 custom attributes,
20-second long polling, 900-second delay, 12-hour visibility from the current
receive, 60-second to 14-day retention. FIFO, AWS Query/XML, KMS/SQS-managed
message encryption, IAM policy emulation, Redis Cluster and ListQueues pagination
are rejected explicitly. Narrow ListQueues prefixes when MaxResults would truncate.

One configured account/region is supported per process. Per-key action and queue
ACLs isolate clients. Account and ARN checks cannot be bypassed through QueueUrl
hostnames. Keys load at startup; use overlapping old/new keys for rolling rotation.
Dashboard access requires credentials and cannot acknowledge messages by bare ID.

## Durability and operations

Production requires healthy AOF with synchronous fsync and noeviction. Readiness
rechecks persistence settings/status with at most a one-second cache. Redis outages
fail requests; no local fallback exists. Internal Redis write retries are disabled
because a lost response is ambiguous. Clients still need idempotency.

Multiple broker instances share one Redis primary and namespace. Automatic
failover/Sentinel and multi-region HA are not implemented. Asynchronous replica
promotion can lose acknowledged writes. Process SIGKILL/restart recovery is tested;
physical disk loss requires consistent off-host backups of the full AOF manifest
and referenced files. Test restoration independently. Never share this Redis with
an evicting cache or discard its volume during upgrades.

Set explicit Redis/container memory limits with headroom for AOF rewrite. Monitor
memory, disk space, persistence errors, request latency, readiness, queue/DLQ depth
and application failures. Capacity depends on storage fsync latency and workload;
functional tests do not constitute a production capacity benchmark. Keep Redis
private, use TLS for remote access, and protect secrets outside source/images.
