# Independent broker acceptance tests

`critic_acceptance.py` exercises the public SQS facade using real boto3. It starts
its own Redis process and two independent broker processes on ephemeral localhost
ports, with a unique key prefix and temporary AOF directory. It never connects to
AWS or an existing Redis deployment. Temporary logs are retained on failure.

Use pinned SDK versions for repeatable protocol checks:

```sh
python3 -m venv /tmp/smoothmq-critic-venv
/tmp/smoothmq-critic-venv/bin/python -m pip --isolated install --no-user boto3==1.43.116 botocore==1.43.116
/tmp/smoothmq-critic-venv/bin/python integration/critic_acceptance.py --slow
```

Prerequisites: Go, `redis-server`, and `redis-cli`. Without `--binary`, the script
builds the current checkout into its temporary directory. `--binary /path/to/bin`
allows testing an existing build. `--test test_03_unique_receipts_and_stale_owner_cannot_mutate`
runs one case. `--keep` retains evidence after success too.

The default run covers attribute persistence, idempotent creation, two-process
concurrent claims, receipt fencing, batch partial failure and validation before
write, DLQ limits and provenance, redrive, allow policies, per-key/action/queue
ACLs, purge/delete/recreation, crash recovery, unsupported-feature rejection, and
recovery after a lost long-poll response. It also checks signature tampering and
expiry, malformed authorization safety, queue-generation fencing of long polls,
move-task cancellation and deletion, destination ACLs, binary attribute checksums,
the 1 MiB payload boundary, and rejection of unsafe production Redis persistence.
Case 22 checks single-port URLs both with and without an advertised endpoint override.
`--slow` additionally waits for actual 60-second retention, including delayed and
in-flight messages and DLQ expiration from the original enqueue time. The final physical
retention check reads the agreed backend message hash in this isolated prefix.

The test contract deliberately permits either a client error or an idempotent
successful no-op for obsolete receipts, provided they cannot mutate a newer
delivery. Redis AOF is configured `appendfsync always`; the crash test verifies
Redis/process restart recovery, not physical power loss or HA failover.

| Priority | Acceptance condition | Cases |
| --- | --- | --- |
| P0 | Two broker processes share claims; current receipts fence obsolete owners | 02, 03, 14 |
| P0 | DLQ threshold, provenance, original retention age, redrive/reset and cancel are durable | 05, 06, 12, 15, 16, 20 |
| P0 | Per-key action/queue permissions cannot be bypassed through URLs or move destinations | 07, 17 |
| P0 | Redis failure returns an error; persisted ready, delayed, in-flight and dead work recovers | 09, 11, 20, 21 |
| P0 | Unsafe Redis production persistence fails startup | 19 |
| P1 | Attributes, tags, partial batch errors, validation, purge and single-port URLs are correct | 01, 04, 08, 10, 22 |
| P1 | Invalid authentication is bounded and safe; wire checksums and 1 MiB payloads roundtrip | 13, 18 |

The harness also detects Go race reports when given a `go build -race` binary,
and fails if graceful broker shutdown takes more than eight seconds. Retention
case 12 is intentionally the only slow case; it must run for final acceptance.

DLQ automatic transfer preserves the original `SentTimestamp` and source ARN;
manual redrive creates a new message identity/timestamp and resets the receive
count. Unsupported FIFO and encryption requests must fail explicitly.

Security regressions additionally reject repeated signed headers and structurally
oversized JSON containers before typed allocation. Broker connections use a
restricted Redis ACL; fixture-only administrator access simulates durability and
outage conditions. See docs/redis-acl.md for the required command set.
