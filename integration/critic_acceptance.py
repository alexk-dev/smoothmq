#!/usr/bin/env python3
"""Independent adversarial tests: real boto3, Redis, and two broker processes.

All state is isolated in a temporary directory, ephemeral localhost ports, and a
unique Redis prefix. No AWS calls, existing Redis databases, or live migrations.
Use a pinned recent boto3/botocore version. See critic_README.md.
"""

import argparse
import concurrent.futures
from datetime import datetime, timedelta, timezone
import hashlib
import http.client
import json
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError


OPTIONS = None
REGION = "eu-central-1"
ACCOUNT = "000000000001"
SECRET = "critic-local-secret-not-a-real-credential"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def eventually(call, predicate=bool, timeout=8, interval=0.05):
    end = time.monotonic() + timeout
    result = None
    while time.monotonic() < end:
        result = call()
        if predicate(result):
            return result
        time.sleep(interval)
    raise AssertionError(f"Condition did not become true in {timeout}s; last={result!r}")


class Stack:
    def __init__(self):
        self.directory = Path(tempfile.mkdtemp(prefix="smoothmq-critic-"))
        self.prefix = "critic-" + uuid.uuid4().hex[:12]
        self.redis_port = free_port()
        self.ports = [free_port(), free_port()]
        self.brokers = []
        self.redis = None
        self.logs = []
        self.allowed_queue = "critic-acl-allowed"
        self.denied_queue = "critic-acl-denied"
        self.policy = self.directory / "policy.json"
        self.policy.write_text(json.dumps({"keys": [
            {"accessKey": "admin", "secretKey": SECRET,
             "actions": ["*"], "queues": ["*"]},
            {"accessKey": "worker", "secretKey": SECRET,
             "actions": ["SendMessage", "ReceiveMessage", "DeleteMessage",
                         "ChangeMessageVisibility", "GetQueueAttributes"],
             "queues": [self.allowed_queue]},
            {"accessKey": "lister", "secretKey": SECRET,
             "actions": ["ListQueues", "GetQueueUrl"],
             "queues": [self.allowed_queue]},
            {"accessKey": "mover", "secretKey": SECRET,
             "actions": ["StartMessageMoveTask", "ListMessageMoveTasks", "CancelMessageMoveTask",
                         "ReceiveMessage", "DeleteMessage", "GetQueueAttributes"],
             "queues": ["critic-acl-move-dlq"]},
        ]}))
        self.redis_config = self.directory / "redis.conf"
        self.redis_config.write_text("\n".join([
            "bind 127.0.0.1", f"port {self.redis_port}", "protected-mode yes",
            f'dir "{self.directory}"', "appendonly yes", "appendfsync always",
            'save ""', "maxmemory 256mb", "maxmemory-policy noeviction",
            "stop-writes-on-bgsave-error yes", "daemonize no", "",
        ]))
        self.binary = Path(OPTIONS.binary).resolve() if OPTIONS.binary else self.directory / "smoothmq"

    def redis_command(self, *args):
        return subprocess.check_output([
            OPTIONS.redis_cli, "-h", "127.0.0.1", "-p", str(self.redis_port),
            "--raw", *map(str, args),
        ], text=True, stderr=subprocess.DEVNULL).strip()

    def spawn(self, argv, filename):
        log = (self.directory / filename).open("ab")
        self.logs.append(log)
        return subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT,
                                cwd=self.directory, start_new_session=True)

    def wait_port(self, proc, port):
        def ready():
            if proc.poll() is not None:
                raise AssertionError(f"Process exited {proc.returncode}; inspect {self.directory}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    return True
            except OSError:
                return False
        eventually(ready, timeout=20)

    def start_redis(self):
        self.redis = self.spawn([OPTIONS.redis_server, str(self.redis_config)], "redis.log")
        self.wait_port(self.redis, self.redis_port)
        eventually(lambda: self.redis_command("PING") == "PONG")

    def start_brokers(self):
        self.brokers = []
        for i, port in enumerate(self.ports):
            argv = [str(self.binary), "server", "--backend=redis",
                    f"--redis-url=redis://127.0.0.1:{self.redis_port}/0",
                    f"--redis-prefix={self.prefix}", "--redis-require-durable=false",
                    "--sqs-host=127.0.0.1", f"--sqs-port={port}",
                    f"--sqs-region={REGION}", f"--sqs-account-id={ACCOUNT}",
                    f"--sqs-policy-file={self.policy}", "--dashboard-enabled=false",
                    "--metrics-prometheus-enabled=false", "--disable-telemetry"]
            proc = self.spawn(argv, f"broker-{i}.log")
            self.brokers.append(proc)
            self.wait_port(proc, port)
        for i in range(2):
            eventually(lambda: self.client(i).list_queues() is not None)

    def start(self):
        if not OPTIONS.binary:
            subprocess.run(["go", "build", "-o", str(self.binary), "."],
                           cwd=Path(__file__).resolve().parents[1], check=True)
        self.start_redis()
        self.start_brokers()

    def client(self, index=0, key="admin", **config):
        return boto3.client("sqs", endpoint_url=f"http://127.0.0.1:{self.ports[index]}",
                            region_name=REGION, aws_access_key_id=key,
                            aws_secret_access_key=SECRET,
                            config=Config(connect_timeout=2, read_timeout=30,
                                          retries={"max_attempts": 0}, **config))

    @staticmethod
    def stop_process(proc, crash=False):
        if proc and proc.poll() is None:
            proc.kill() if crash else proc.terminate()
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
                return False
        return True

    def restart(self, redis=False):
        for proc in self.brokers:
            self.stop_process(proc, crash=True)
        if redis:
            self.stop_process(self.redis, crash=True)
            self.start_redis()
        self.start_brokers()

    def close(self, failed=False):
        hangs = []
        for i, proc in enumerate(self.brokers):
            if not self.stop_process(proc):
                hangs.append(f"broker-{i}")
        self.stop_process(self.redis)
        for log in self.logs:
            log.close()
        races = [str(path) for path in self.directory.glob("broker-*.log")
                 if "WARNING: DATA RACE" in path.read_text(errors="replace")]
        if failed or hangs or races or OPTIONS.keep:
            print(f"Local critic logs and Redis AOF retained: {self.directory}")
        else:
            shutil.rmtree(self.directory)
        if hangs:
            raise AssertionError(f"Graceful shutdown exceeded eight seconds: {hangs}")
        if races:
            raise AssertionError(f"Go race detector reported races: {races}")


class CriticAcceptance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.stack = Stack()
        try:
            cls.stack.start()
        except Exception:
            cls.stack.close(failed=True)
            raise

    @classmethod
    def tearDownClass(cls):
        cls.stack.close(failed=bool(getattr(cls, "failed", False)))

    def tearDown(self):
        # Keep the evidence if any assertion fails; unittest internals differ
        # between supported Python versions, so use the actual result lists.
        result = getattr(getattr(self, "_outcome", None), "result", None)
        if result and (result.failures or result.errors):
            type(self).failed = True

    def setUp(self):
        self.a = self.stack.client(0)
        self.b = self.stack.client(1)

    def queue(self, label="q", attributes=None, name=None):
        return self.a.create_queue(QueueName=name or f"critic-{label}-{uuid.uuid4().hex[:10]}",
                                   Attributes=attributes or {})["QueueUrl"]

    def arn(self, url):
        return f"arn:aws:sqs:{REGION}:{ACCOUNT}:{url.rsplit('/', 1)[-1]}"

    def attrs(self, url):
        return self.a.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]

    def receive(self, url, client=None, visibility=10, count=1, wait=0):
        return (client or self.a).receive_message(
            QueueUrl=url, MaxNumberOfMessages=count, WaitTimeSeconds=wait,
            VisibilityTimeout=visibility, AttributeNames=["All"],
            MessageAttributeNames=["All"]).get("Messages", [])

    def eventually_receive(self, url, client=None, **kwargs):
        return eventually(lambda: self.receive(url, client=client, **kwargs))

    def assert_client_error(self, callback, codes=None):
        with self.assertRaises(ClientError) as caught:
            callback()
        self.assertGreaterEqual(caught.exception.response["ResponseMetadata"]["HTTPStatusCode"], 400)
        self.assertLess(caught.exception.response["ResponseMetadata"]["HTTPStatusCode"], 500)
        if codes:
            self.assertIn(caught.exception.response["Error"]["Code"], codes)
        return caught.exception

    def allow_error(self, callback):
        # Both a stale-handle error and an idempotent successful no-op are safe.
        try:
            callback()
        except ClientError as error:
            self.assertLess(error.response["ResponseMetadata"]["HTTPStatusCode"], 500)

    def test_01_attributes_roundtrip_and_idempotent_create(self):
        settings = {"VisibilityTimeout": "180", "MessageRetentionPeriod": "120",
                    "ReceiveMessageWaitTimeSeconds": "1", "DelaySeconds": "0",
                    "MaximumMessageSize": "4096"}
        url = self.queue("attributes", settings)
        got = self.attrs(url)
        for key, value in settings.items():
            self.assertEqual(got.get(key), value, key)
        self.assertEqual(got["QueueArn"], self.arn(url))
        same = self.a.create_queue(QueueName=url.rsplit("/", 1)[-1], Attributes=settings)
        self.assertEqual(same["QueueUrl"], url)
        self.assert_client_error(lambda: self.a.create_queue(
            QueueName=url.rsplit("/", 1)[-1], Attributes={"VisibilityTimeout": "181"}))
        self.a.set_queue_attributes(QueueUrl=url, Attributes={"VisibilityTimeout": "30"})
        updated = self.attrs(url)
        self.assertEqual(updated["VisibilityTimeout"], "30")
        self.assertEqual(updated["MessageRetentionPeriod"], "120")
        self.a.tag_queue(QueueUrl=url, Tags={"purpose": "critic"})
        self.assertEqual(self.a.list_queue_tags(QueueUrl=url)["Tags"]["purpose"], "critic")
        self.a.untag_queue(QueueUrl=url, TagKeys=["purpose"])
        self.assertNotIn("purpose", self.a.list_queue_tags(QueueUrl=url).get("Tags", {}))

    def test_02_exclusive_claim_across_two_broker_processes(self):
        url = self.queue("concurrent")
        wanted = set()
        for start in range(0, 50, 10):
            result = self.a.send_message_batch(QueueUrl=url, Entries=[
                {"Id": str(i), "MessageBody": f"body-{i}"} for i in range(start, start + 10)])
            self.assertFalse(result.get("Failed"))
            wanted.update(item["MessageId"] for item in result["Successful"])

        def poll(index):
            client = self.stack.client(index % 2)
            found = []
            for _ in range(8):
                found.extend(self.receive(url, client, visibility=30, count=10))
            return found

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            messages = [m for group in pool.map(poll, range(8)) for m in group]
        ids = [m["MessageId"] for m in messages]
        receipts = [m["ReceiptHandle"] for m in messages]
        self.assertEqual(len(ids), 50)
        self.assertEqual(set(ids), wanted)
        self.assertEqual(len(set(ids)), len(ids), "a message was concurrently claimed twice")
        self.assertEqual(len(set(receipts)), len(receipts))
        for message in messages:
            self.assertEqual(message["Attributes"]["ApproximateReceiveCount"], "1")

    def test_03_unique_receipts_and_stale_owner_cannot_mutate(self):
        url = self.queue("receipt")
        sent = self.a.send_message(QueueUrl=url, MessageBody="receipt-fencing")
        first = self.eventually_receive(url, visibility=1)[0]
        time.sleep(1.15)
        second = self.eventually_receive(url, self.b, visibility=10)[0]
        self.assertEqual(first["MessageId"], second["MessageId"])
        self.assertNotEqual(first["ReceiptHandle"], second["ReceiptHandle"])
        self.allow_error(lambda: self.a.change_message_visibility(
            QueueUrl=url, ReceiptHandle=first["ReceiptHandle"], VisibilityTimeout=0))
        self.assertEqual(self.receive(url), [], "old receipt released a newer owner's message")
        self.allow_error(lambda: self.a.delete_message(QueueUrl=url, ReceiptHandle=first["ReceiptHandle"]))
        self.b.change_message_visibility(QueueUrl=url, ReceiptHandle=second["ReceiptHandle"], VisibilityTimeout=0)
        third = self.eventually_receive(url, visibility=10)[0]
        self.assertEqual(third["MessageId"], sent["MessageId"], "old owner deleted the message")
        self.assertEqual(third["Attributes"]["ApproximateReceiveCount"], "3")
        self.assertEqual(len({first["ReceiptHandle"], second["ReceiptHandle"], third["ReceiptHandle"]}), 3)
        self.a.delete_message(QueueUrl=url, ReceiptHandle=third["ReceiptHandle"])
        self.assertFalse(self.receive(url))

    def test_04_batch_partial_failures_and_duplicate_id_validation(self):
        url = self.queue("batch", {"MaximumMessageSize": "1024"})
        result = self.a.send_message_batch(QueueUrl=url, Entries=[
            {"Id": "good", "MessageBody": "ok"},
            {"Id": "oversize", "MessageBody": "x" * 2048}])
        self.assertEqual({e["Id"] for e in result.get("Successful", [])}, {"good"})
        self.assertEqual({e["Id"] for e in result.get("Failed", [])}, {"oversize"})
        self.assertTrue(result["Failed"][0]["SenderFault"])
        self.assert_client_error(lambda: self.a.send_message_batch(QueueUrl=url, Entries=[
            {"Id": "duplicate", "MessageBody": "MUST-NOT-EXIST-1"},
            {"Id": "duplicate", "MessageBody": "MUST-NOT-EXIST-2"}]))
        message = self.eventually_receive(url)[0]
        changed = self.a.change_message_visibility_batch(QueueUrl=url, Entries=[
            {"Id": "good", "ReceiptHandle": message["ReceiptHandle"], "VisibilityTimeout": 0},
            {"Id": "bad", "ReceiptHandle": "not-a-receipt", "VisibilityTimeout": 0}])
        self.assertEqual({e["Id"] for e in changed.get("Successful", [])}, {"good"})
        self.assertEqual({e["Id"] for e in changed.get("Failed", [])}, {"bad"})
        message = self.eventually_receive(url)[0]
        deleted = self.a.delete_message_batch(QueueUrl=url, Entries=[
            {"Id": "good", "ReceiptHandle": message["ReceiptHandle"]},
            {"Id": "bad", "ReceiptHandle": "not-a-receipt"}])
        self.assertEqual({e["Id"] for e in deleted.get("Successful", [])}, {"good"})
        self.assertEqual({e["Id"] for e in deleted.get("Failed", [])}, {"bad"})
        self.assertFalse(self.receive(url, count=10), "invalid batch partially enqueued duplicate IDs")

    def dlq_pair(self, maximum=2):
        dlq = self.queue("dlq", {"MessageRetentionPeriod": "120"})
        source_name = f"critic-source-{uuid.uuid4().hex[:10]}"
        source_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT}:{source_name}"
        self.a.set_queue_attributes(QueueUrl=dlq, Attributes={"RedriveAllowPolicy": json.dumps({
            "redrivePermission": "byQueue", "sourceQueueArns": [source_arn]})})
        source = self.queue("source", {"VisibilityTimeout": "1", "MessageRetentionPeriod": "60",
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": self.arn(dlq), "maxReceiveCount": str(maximum)})},
            name=source_name)
        return source, dlq

    def to_dlq(self, source, dlq, body="poison", maximum=2):
        sent = self.a.send_message(QueueUrl=source, MessageBody=body,
            MessageAttributes={"marker": {"DataType": "String", "StringValue": "preserve-me"}})
        first = None
        for i in range(maximum):
            message = self.eventually_receive(source, visibility=1)[0]
            first = first or message
            self.assertEqual(message["MessageId"], sent["MessageId"])
            self.assertEqual(message["Attributes"]["ApproximateReceiveCount"], str(i + 1))
            time.sleep(1.1)
        self.assertFalse(self.receive(source, visibility=1))
        dead = self.eventually_receive(dlq, visibility=10)[0]
        self.assertEqual(dead["Body"], body)
        self.assertEqual(dead["MessageAttributes"]["marker"]["StringValue"], "preserve-me")
        self.assertEqual(dead["Attributes"]["SentTimestamp"], first["Attributes"]["SentTimestamp"])
        self.assertEqual(dead["Attributes"]["ApproximateReceiveCount"], "1")
        self.assertEqual(dead["Attributes"]["DeadLetterQueueSourceArn"], self.arn(source))
        return first, dead

    def test_05_poison_dlq_and_redrive(self):
        source, dlq = self.dlq_pair(maximum=2)
        original, dead = self.to_dlq(source, dlq)
        self.a.change_message_visibility(QueueUrl=dlq, ReceiptHandle=dead["ReceiptHandle"], VisibilityTimeout=0)
        task = self.a.start_message_move_task(SourceArn=self.arn(dlq), DestinationArn=self.arn(source),
                                              MaxNumberOfMessagesPerSecond=10)
        handle = task["TaskHandle"]
        def completed():
            tasks = self.a.list_message_move_tasks(SourceArn=self.arn(dlq)).get("Results", [])
            return next((item for item in tasks if item["Status"] == "COMPLETED"), None)
        eventually(completed, timeout=12)
        restored = self.eventually_receive(source, visibility=10)[0]
        self.assertEqual(restored["Body"], "poison")
        self.assertNotEqual(restored["MessageId"], original["MessageId"])
        self.assertGreater(int(restored["Attributes"]["SentTimestamp"]), int(original["Attributes"]["SentTimestamp"]))
        self.assertEqual(restored["Attributes"]["ApproximateReceiveCount"], "1")
        self.assertFalse(self.receive(dlq))

    def test_06_redrive_allow_policy_enforced_on_configuration(self):
        dlq = self.queue("denied-dlq")
        self.a.set_queue_attributes(QueueUrl=dlq, Attributes={"RedriveAllowPolicy": json.dumps({"redrivePermission": "denyAll"})})
        self.assert_client_error(lambda: self.queue("forbidden-source", {"RedrivePolicy": json.dumps({
            "deadLetterTargetArn": self.arn(dlq), "maxReceiveCount": "2"})}))
        self.assert_client_error(lambda: self.queue("self-redrive", {"RedrivePolicy": json.dumps({
            "deadLetterTargetArn": self.arn(dlq).replace(ACCOUNT, "111111111111"), "maxReceiveCount": "2"})}))

    def test_07_acl_actions_names_and_queue_url_account(self):
        allowed = self.queue(name=self.stack.allowed_queue)
        denied = self.queue(name=self.stack.denied_queue)
        worker, lister = self.stack.client(key="worker"), self.stack.client(key="lister")
        worker.send_message(QueueUrl=allowed, MessageBody="allowed")
        self.assert_client_error(lambda: worker.send_message(QueueUrl=denied, MessageBody="denied"))
        self.assert_client_error(lambda: worker.purge_queue(QueueUrl=allowed))
        self.assert_client_error(lambda: worker.create_queue(QueueName="not-authorized"))
        listed = lister.list_queues().get("QueueUrls", [])
        self.assertEqual(listed, [allowed])
        self.assertEqual(lister.get_queue_url(QueueName=self.stack.allowed_queue)["QueueUrl"], allowed)
        self.assert_client_error(lambda: lister.get_queue_url(QueueName=self.stack.denied_queue))
        self.assert_client_error(lambda: worker.receive_message(
            QueueUrl=allowed.replace(ACCOUNT, "111111111111"), WaitTimeSeconds=0))
        message = self.eventually_receive(allowed, worker)[0]
        self.allow_error(lambda: self.a.delete_message(QueueUrl=denied, ReceiptHandle=message["ReceiptHandle"]))
        worker.change_message_visibility(QueueUrl=allowed, ReceiptHandle=message["ReceiptHandle"], VisibilityTimeout=0)
        self.assertEqual(self.eventually_receive(allowed, worker)[0]["Body"], "allowed")

    def test_08_purge_delete_and_queue_recreation_fence_old_receipts(self):
        url = self.queue("purge")
        for start in range(0, 40, 10):
            self.a.send_message_batch(QueueUrl=url, Entries=[
                {"Id": str(i), "MessageBody": str(i), "DelaySeconds": 30 if i % 3 == 0 else 0}
                for i in range(start, start + 10)])
        old = self.eventually_receive(url, count=4)
        self.a.purge_queue(QueueUrl=url)
        self.assertFalse(self.receive(url, count=10))
        attrs = self.attrs(url)
        for key in ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"]:
            self.assertEqual(attrs[key], "0", key)
        self.a.delete_queue(QueueUrl=url)
        self.assert_client_error(lambda: self.a.get_queue_attributes(QueueUrl=url, AttributeNames=["All"]))
        recreated = self.queue(name=url.rsplit("/", 1)[-1])
        self.a.send_message(QueueUrl=recreated, MessageBody="new-generation")
        for message in old:
            self.allow_error(lambda: self.a.delete_message(QueueUrl=recreated, ReceiptHandle=message["ReceiptHandle"]))
            self.allow_error(lambda: self.a.change_message_visibility(QueueUrl=recreated,
                ReceiptHandle=message["ReceiptHandle"], VisibilityTimeout=0))
        self.assertEqual(self.eventually_receive(recreated)[0]["Body"], "new-generation")

    def test_09_broker_and_redis_crash_restart(self):
        url = self.queue("restart")
        self.a.send_message(QueueUrl=url, MessageBody="in-flight")
        inflight = self.eventually_receive(url, visibility=3)[0]
        self.a.send_message(QueueUrl=url, MessageBody="ready")
        self.a.send_message(QueueUrl=url, MessageBody="delayed", DelaySeconds=3)
        source, dlq = self.dlq_pair(maximum=1)
        _, dead = self.to_dlq(source, dlq, body="persisted-dead", maximum=1)
        self.a.change_message_visibility(QueueUrl=dlq, ReceiptHandle=dead["ReceiptHandle"], VisibilityTimeout=0)
        self.stack.restart(redis=True)
        self.a, self.b = self.stack.client(0), self.stack.client(1)
        self.assertEqual(self.eventually_receive(dlq)[0]["Body"], "persisted-dead")
        found = []
        deadline = time.monotonic() + 8
        while len(found) < 3 and time.monotonic() < deadline:
            found.extend(self.receive(url, count=10, visibility=10))
            time.sleep(0.1)
        self.assertCountEqual([m["Body"] for m in found], ["in-flight", "ready", "delayed"])
        retried = next(m for m in found if m["Body"] == "in-flight")
        self.assertNotEqual(retried["ReceiptHandle"], inflight["ReceiptHandle"])

    def test_10_explicit_unsupported_features_and_input_validation(self):
        self.assert_client_error(lambda: self.queue("sse", {"SqsManagedSseEnabled": "true"}))
        self.assert_client_error(lambda: self.queue("kms", {"KmsMasterKeyId": "fake"}))
        self.assert_client_error(lambda: self.queue("fifo", {"FifoQueue": "true"},
                                                    name="critic-unsupported.fifo"))
        self.assert_client_error(lambda: self.queue("invalid", {"VisibilityTimeout": "-1"}))
        url = self.queue("validation")
        before = self.attrs(url)["VisibilityTimeout"]
        self.assert_client_error(lambda: self.a.set_queue_attributes(QueueUrl=url, Attributes={
            "VisibilityTimeout": "42", "NotARealAttribute": "ignored?"}))
        self.assertEqual(self.attrs(url)["VisibilityTimeout"], before,
                         "invalid request partially mutated queue attributes")
        self.assert_client_error(lambda: self.a.receive_message(QueueUrl=url, MaxNumberOfMessages=11))
        self.assert_client_error(lambda: self.a.send_message(QueueUrl=url, MessageBody="x", DelaySeconds=901))

    def test_11_lost_long_poll_response_is_redelivered(self):
        url = self.queue("lost-response")
        endpoint = f"http://127.0.0.1:{self.stack.ports[0]}/"
        payload = json.dumps({"QueueUrl": url, "MaxNumberOfMessages": 1,
                              "WaitTimeSeconds": 3, "VisibilityTimeout": 1})
        request = AWSRequest(method="POST", url=endpoint, data=payload, headers={
            "X-Amz-Target": "AmazonSQS.ReceiveMessage", "Content-Type": "application/x-amz-json-1.0"})
        SigV4Auth(Credentials("admin", SECRET), "sqs", REGION).add_auth(request)
        connection = http.client.HTTPConnection("127.0.0.1", self.stack.ports[0], timeout=2)
        connection.request("POST", "/", body=payload.encode(), headers=dict(request.headers))
        time.sleep(0.15)
        connection.sock.shutdown(socket.SHUT_RDWR)
        connection.close()
        self.b.send_message(QueueUrl=url, MessageBody="response-may-be-lost")
        messages = eventually(lambda: self.receive(url, self.b, visibility=10), timeout=6)
        self.assertEqual(messages[0]["Body"], "response-may-be-lost")

    def test_12_retention_expires_delayed_and_inflight_data(self):
        if not OPTIONS.slow:
            self.skipTest("enable --slow for real retention expiry")
        url = self.queue("retention", {"MessageRetentionPeriod": "60"})
        self.a.send_message(QueueUrl=url, MessageBody="inflight-expiry")
        self.eventually_receive(url, visibility=120)
        self.a.send_message(QueueUrl=url, MessageBody="delayed-expiry", DelaySeconds=90)
        dlq = self.queue("dlq-retention", {"MessageRetentionPeriod": "60"})
        source = self.queue("src-retention", {"MessageRetentionPeriod": "120", "RedrivePolicy": json.dumps({
            "deadLetterTargetArn": self.arn(dlq), "maxReceiveCount": "1"})})
        self.a.send_message(QueueUrl=source, MessageBody="original-timestamp-expiry")
        self.eventually_receive(source, visibility=30)
        started = time.monotonic()
        deadline = started + 61
        while time.monotonic() < started + 31:
            time.sleep(min(0.5, started + 31 - time.monotonic()))
        self.assertFalse(self.receive(source))  # Transfer after 31 seconds in source.
        self.eventually_receive(dlq, visibility=120)
        while time.monotonic() < deadline:
            time.sleep(min(0.5, deadline - time.monotonic()))
        self.assertFalse(self.receive(url, count=10))
        attrs = self.attrs(url)
        for key in ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"]:
            self.assertEqual(attrs[key], "0", key)
            self.assertEqual(self.attrs(dlq)[key], "0", "DLQ expiry must use original timestamp")
        name = url.rsplit("/", 1)[-1]
        messages_key = f"{self.stack.prefix}:{{1}}:{name}:messages"
        eventually(lambda: self.stack.redis_command("HLEN", messages_key) == "0", timeout=10)

    def signed_request(self, payload, at=None):
        endpoint = f"http://127.0.0.1:{self.stack.ports[0]}/"
        request = AWSRequest(method="POST", url=endpoint, data=payload, headers={
            "X-Amz-Target": "AmazonSQS.ListQueues", "Content-Type": "application/x-amz-json-1.0"})
        signer = SigV4Auth(Credentials("admin", SECRET), "sqs", REGION)
        if at:
            with patch("botocore.auth.get_current_datetime", return_value=at):
                signer.add_auth(request)
        else:
            signer.add_auth(request)
        return dict(request.headers)

    def raw_http(self, payload, headers):
        connection = http.client.HTTPConnection("127.0.0.1", self.stack.ports[0], timeout=3)
        try:
            connection.request("POST", "/", body=payload.encode(), headers=headers)
            response = connection.getresponse()
            result = response.status, response.read()
            return result
        finally:
            connection.close()

    def test_13_auth_malformed_expired_and_tampered_requests_fail_safely(self):
        for authorization in ["", "AWS4-HMAC-SHA256 Credential=broken", "AWS4-HMAC-SHA256 Credential",
                              "AWS4-HMAC-SHA256 Credential=a/b/c/d/e, SignedHeaders, Signature=x"]:
            status, _ = self.raw_http("{}", {"Authorization": authorization,
                "X-Amz-Target": "AmazonSQS.ListQueues", "Content-Type": "application/x-amz-json-1.0"})
            self.assertGreaterEqual(status, 400)
            self.assertLess(status, 500)
        now = datetime.now(timezone.utc)
        for invalid_time in [now - timedelta(hours=1), now + timedelta(hours=1)]:
            status, _ = self.raw_http("{}", self.signed_request("{}", at=invalid_time))
            self.assertGreaterEqual(status, 400, "valid but expired/future signature accepted")
            self.assertLess(status, 500)
        status, _ = self.raw_http('{"QueueNamePrefix":"tampered"}', self.signed_request("{}"))
        self.assertGreaterEqual(status, 400, "signature did not bind request body")
        self.assertLess(status, 500)
        self.a.list_queues()  # Malformed inputs must not kill or poison the server.

    def test_14_long_poll_cannot_claim_a_recreated_queue(self):
        url = self.queue("poll-generation")
        started = threading.Event()
        def old_poll():
            started.set()
            try:
                return self.receive(url, self.b, visibility=10, wait=2)
            except ClientError as error:
                self.assertLess(error.response["ResponseMetadata"]["HTTPStatusCode"], 500)
                return []
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(old_poll)
            started.wait(timeout=1)
            time.sleep(0.25)
            self.a.delete_queue(QueueUrl=url)
            recreated = self.queue(name=url.rsplit("/", 1)[-1])
            self.a.send_message(QueueUrl=recreated, MessageBody="belongs-to-new-generation")
            self.assertFalse(pending.result(timeout=5), "old poll consumed newly created queue contents")
        self.assertEqual(self.eventually_receive(recreated)[0]["Body"], "belongs-to-new-generation")

    def test_15_move_task_cancellation_stops_future_transfers(self):
        source, dlq = self.dlq_pair(maximum=1)
        # A configured DLQ may also receive manually submitted messages. An explicit
        # destination makes their original-source provenance unnecessary.
        for start in range(0, 20, 10):
            self.a.send_message_batch(QueueUrl=dlq, Entries=[
                {"Id": str(i), "MessageBody": f"cancel-{i}"} for i in range(start, start + 10)])
        handle = self.a.start_message_move_task(SourceArn=self.arn(dlq), DestinationArn=self.arn(source),
                                                 MaxNumberOfMessagesPerSecond=1)["TaskHandle"]
        self.a.cancel_message_move_task(TaskHandle=handle)
        tasks = self.a.list_message_move_tasks(SourceArn=self.arn(dlq)).get("Results", [])
        self.assertTrue(any(task["Status"] in ["CANCELLED", "CANCELLING"] for task in tasks))
        before = int(self.attrs(source)["ApproximateNumberOfMessages"])
        time.sleep(1.3)
        after = int(self.attrs(source)["ApproximateNumberOfMessages"])
        remaining = int(self.attrs(dlq)["ApproximateNumberOfMessages"])
        self.assertEqual(before, after, "cancelled task continued transferring")
        self.assertEqual(after + remaining, 20, "move/cancel lost or duplicated messages")

    def test_16_move_task_fails_if_source_queue_is_deleted(self):
        source, dlq = self.dlq_pair(maximum=1)
        for start in range(0, 20, 10):
            self.a.send_message_batch(QueueUrl=dlq, Entries=[
                {"Id": str(i), "MessageBody": f"delete-{i}"} for i in range(start, start + 10)])
        self.a.start_message_move_task(SourceArn=self.arn(dlq), DestinationArn=self.arn(source),
                                       MaxNumberOfMessagesPerSecond=1)
        self.a.delete_queue(QueueUrl=dlq)
        def terminal():
            tasks = self.a.list_message_move_tasks(SourceArn=self.arn(dlq)).get("Results", [])
            return next((task for task in tasks if task["Status"] in ["FAILED", "CANCELLED"]), None)
        eventually(terminal, timeout=6)

    def test_17_move_task_cannot_bypass_destination_acl(self):
        dlq = self.queue(name="critic-acl-move-dlq")
        destination = self.queue("move-forbidden-destination", {"RedrivePolicy": json.dumps({
            "deadLetterTargetArn": self.arn(dlq), "maxReceiveCount": "1"})})
        self.a.send_message(QueueUrl=dlq, MessageBody="must-stay")
        mover = self.stack.client(key="mover")
        self.assert_client_error(lambda: mover.start_message_move_task(
            SourceArn=self.arn(dlq), DestinationArn=self.arn(destination), MaxNumberOfMessagesPerSecond=10))
        self.assertFalse(self.receive(destination))
        self.assertEqual(self.eventually_receive(dlq)[0]["Body"], "must-stay")

    def test_18_message_attribute_wire_integrity_and_payload_limit(self):
        url = self.queue("attribute-wire")
        body = "кириллица ✓"
        attributes = {
            "text": {"DataType": "String.custom", "StringValue": "value"},
            "number": {"DataType": "Number", "StringValue": "12.5"},
            "binary": {"DataType": "Binary", "BinaryValue": b"\x00\x01\xfe\xff"},
        }
        digest = bytearray()
        def packed(value):
            return struct.pack(">I", len(value)) + value
        for name, value in sorted(attributes.items()):
            digest.extend(packed(name.encode()))
            digest.extend(packed(value["DataType"].encode()))
            if value["DataType"] == "Binary":
                digest.append(2)
                digest.extend(packed(value["BinaryValue"]))
            else:
                digest.append(1)
                digest.extend(packed(value["StringValue"].encode()))
        expected = hashlib.md5(digest).hexdigest()
        sent = self.a.send_message(QueueUrl=url, MessageBody=body, MessageAttributes=attributes)
        self.assertEqual(sent["MD5OfMessageAttributes"], expected)
        received = self.eventually_receive(url)[0]
        self.assertEqual(received["Body"], body)
        self.assertEqual(received["MessageAttributes"], attributes)
        self.assertEqual(received["MD5OfMessageAttributes"], expected)
        self.assertEqual(received["MD5OfBody"], hashlib.md5(body.encode()).hexdigest())
        limit = self.queue("max-payload")
        large = "x" * 1048576
        self.a.send_message(QueueUrl=limit, MessageBody=large)
        self.assertEqual(self.eventually_receive(limit)[0]["Body"], large,
                         "legal 1MiB message rejected by JSON transport overhead")
        self.assert_client_error(lambda: self.a.send_message(QueueUrl=limit, MessageBody=large + "x"))
        small = self.queue("attributes-size", {"MaximumMessageSize": "1024"})
        self.assert_client_error(lambda: self.a.send_message(QueueUrl=small, MessageBody="x" * 980,
            MessageAttributes={"marker": {"DataType": "String", "StringValue": "v" * 60}}))
        self.assertFalse(self.receive(small))

    def test_19_production_durability_guard_rejects_unsafe_redis(self):
        original = self.stack.redis_command("CONFIG", "GET", "appendfsync").splitlines()[-1]
        try:
            self.stack.redis_command("CONFIG", "SET", "appendfsync", "everysec")
            argv = list(self.stack.brokers[0].args)
            argv = ["--redis-require-durable=true" if argument == "--redis-require-durable=false" else argument
                    for argument in argv]
            probe_port = free_port()
            argv = [f"--sqs-port={probe_port}" if argument.startswith("--sqs-port=") else argument
                    for argument in argv]
            process = self.stack.spawn(argv, "unsafe-production-startup.log")
            try:
                status = process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.stack.stop_process(process)
                self.fail("production profile started against Redis appendfsync=everysec")
            self.assertNotEqual(status, 0, "invalid durability profile exited as success")
            with self.assertRaises(OSError):
                socket.create_connection(("127.0.0.1", probe_port), timeout=0.2)
        finally:
            self.stack.redis_command("CONFIG", "SET", "appendfsync", original)

    def test_20_running_redrive_survives_broker_and_redis_restart(self):
        destination, dlq = self.dlq_pair(maximum=1)
        self.a.send_message_batch(QueueUrl=dlq, Entries=[
            {"Id": str(i), "MessageBody": f"durable-task-{i}"} for i in range(3)])
        self.a.start_message_move_task(SourceArn=self.arn(dlq), DestinationArn=self.arn(destination),
                                       MaxNumberOfMessagesPerSecond=1)
        self.stack.restart(redis=True)
        self.a, self.b = self.stack.client(0), self.stack.client(1)
        def completed():
            tasks = self.a.list_message_move_tasks(SourceArn=self.arn(dlq)).get("Results", [])
            return next((task for task in tasks if task["Status"] == "COMPLETED"), None)
        task = eventually(completed, timeout=8)
        self.assertEqual(task["ApproximateNumberOfMessagesMoved"], 3)
        received = self.receive(destination, count=10)
        self.assertCountEqual([m["Body"] for m in received], [f"durable-task-{i}" for i in range(3)])
        self.assertFalse(self.receive(dlq))

    def test_21_redis_outage_fails_closed_and_existing_brokers_recover(self):
        url = self.queue("redis-outage")
        self.a.send_message(QueueUrl=url, MessageBody="persisted-before-outage")
        self.stack.stop_process(self.stack.redis, crash=True)
        try:
            with self.assertRaises(ClientError) as caught:
                self.a.send_message(QueueUrl=url, MessageBody="MUST-NOT-BE-ACKNOWLEDGED")
            self.assertEqual(caught.exception.response["ResponseMetadata"]["HTTPStatusCode"], 503)
        finally:
            self.stack.start_redis()
        # Existing broker processes retain their clients. Recovery must reload the
        # Lua script after NOSCRIPT without restarting the brokers themselves.
        self.assertEqual(self.eventually_receive(url)[0]["Body"], "persisted-before-outage")
        self.a.send_message(QueueUrl=url, MessageBody="new-after-recovery")
        self.assertEqual(self.eventually_receive(url)[0]["Body"], "new-after-recovery")
        self.assertFalse(self.receive(url))

    def test_22_single_port_queue_urls_with_and_without_override(self):
        for use_override in [False, True]:
            with self.subTest(endpoint_override=use_override):
                port = free_port()
                endpoint = f"http://127.0.0.1:{port}/sqs"
                argv = list(self.stack.brokers[0].args) + [
                    "--use-single-port=true", "--host=127.0.0.1", f"--port={port}"]
                if use_override:
                    argv.append(f"--sqs-endpoint-override={endpoint}")
                process = self.stack.spawn(argv, f"broker-single-port-{int(use_override)}.log")
                self.stack.brokers.append(process)
                self.stack.wait_port(process, port)
                client = boto3.client("sqs", endpoint_url=endpoint, region_name=REGION,
                                      aws_access_key_id="admin", aws_secret_access_key=SECRET,
                                      config=Config(connect_timeout=2, read_timeout=5,
                                                    retries={"max_attempts": 0}))
                name = "critic-single-port-" + uuid.uuid4().hex[:8]
                url = client.create_queue(QueueName=name)["QueueUrl"]
                self.assertEqual(url, f"{endpoint}/{ACCOUNT}/{name}")
                client.send_message(QueueUrl=url, MessageBody="single-port-roundtrip")
                messages = self.receive(url, client=client)
                self.assertEqual(messages[0]["Body"], "single-port-roundtrip")
                client.delete_message(QueueUrl=url, ReceiptHandle=messages[0]["ReceiptHandle"])
                self.assertFalse(self.receive(url, client=client))
                client.delete_queue(QueueUrl=url)
                self.stack.stop_process(process)


def main():
    global OPTIONS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", help="already built smoothmq binary; otherwise go build in temp dir")
    parser.add_argument("--redis-server", default=shutil.which("redis-server") or "redis-server")
    parser.add_argument("--redis-cli", default=shutil.which("redis-cli") or "redis-cli")
    parser.add_argument("--slow", action="store_true", help="also wait for the real 60 second retention period")
    parser.add_argument("--keep", action="store_true", help="retain local state and logs after a successful run")
    parser.add_argument("--test", action="append", help="run only named test methods (repeatable)")
    OPTIONS = parser.parse_args()
    names = OPTIONS.test or unittest.defaultTestLoader.getTestCaseNames(CriticAcceptance)
    suite = unittest.TestSuite(CriticAcceptance(name) for name in names)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())


if __name__ == "__main__":
    main()
