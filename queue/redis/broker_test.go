package redis

import (
	"context"
	"encoding/json"
	"fmt"
	goredis "github.com/redis/go-redis/v9"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

func testBroker(t *testing.T, durable bool) *Broker {
	t.Helper()
	exe, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server is required for integration tests")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	cmd := exec.Command(exe, "--bind", "127.0.0.1", "--port", strconv.Itoa(port), "--dir", t.TempDir(), "--appendonly", "yes", "--appendfsync", "always", "--save", "", "--maxmemory-policy", "noeviction")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	cfg := Config{URL: fmt.Sprintf("redis://127.0.0.1:%d/0", port), Prefix: "test", Region: "eu-central-1", AccountID: "000000000001", RequireDurable: durable}
	var b *Broker
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		b, err = New(cfg)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Shutdown() })
	return b
}
func TestDurableStartupAndUnsafeChangeFailClosed(t *testing.T) {
	b := testBroker(t, true)
	ctx := context.Background()
	if err := b.Create(ctx, 1, "safe", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.client.ConfigSet(ctx, "appendfsync", "everysec").Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := b.Ready(ctx); err == nil {
		t.Fatal("readiness accepted unsafe Redis")
	}
	if _, err := b.Send(ctx, 1, "safe", "must-not-publish", nil, nil); err == nil {
		t.Fatal("write accepted unsafe Redis")
	}
	if n := b.client.HLen(ctx, b.root(1)+":safe:messages").Val(); n != 0 {
		t.Fatalf("unsafe write persisted: %d", n)
	}
}
func TestRetentionCleanupMakesBoundedProgress(t *testing.T) {
	b := testBroker(t, false)
	ctx := context.Background()
	if err := b.Create(ctx, 1, "backlog", nil, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 600; i++ {
		id, err := b.Send(ctx, 1, "backlog", "expired", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		// Isolated fixture advances original enqueue times without shortening production retention limits.
		raw := b.client.HGet(ctx, b.root(1)+":backlog:messages", strconv.FormatInt(id, 10)).Val()
		var m map[string]interface{}
		json.Unmarshal([]byte(raw), &m)
		m["sent"] = 1
		encoded, _ := json.Marshal(m)
		b.client.HSet(ctx, b.root(1)+":backlog:messages", id, string(encoded))
		b.client.ZAdd(ctx, b.root(1)+":backlog:sent", goredis.Z{Score: 1, Member: strconv.FormatInt(id, 10)})
	}
	for i := 0; i < 5; i++ {
		if _, err := b.call(ctx, 1, "maintain", "backlog", nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, suffix := range []string{"messages", "ready", "inflight", "sent"} {
		if n := b.client.Exists(ctx, b.root(1)+":backlog:"+suffix).Val(); n != 0 {
			t.Errorf("expired storage remains: %s", suffix)
		}
	}
}
func TestTaskHistoryIsBoundedAndNewTasksKeepProgressing(t *testing.T) {
	b := testBroker(t, false)
	ctx := context.Background()
	if err := b.Create(ctx, 1, "dead", nil, nil); err != nil {
		t.Fatal(err)
	}
	policy := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":1}`, b.ARNPrefix()+"dead")
	if err := b.Create(ctx, 1, "source", map[string]string{"RedrivePolicy": policy}, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 105; i++ {
		task, err := b.MoveTask(ctx, 1, "start", map[string]interface{}{"sourceArn": b.ARNPrefix() + "dead", "destinationArn": b.ARNPrefix() + "source", "rate": 1})
		if err != nil {
			t.Fatal(i, err)
		}
		if _, err = b.call(ctx, 1, "task-step", "dead", map[string]interface{}{"handle": task["TaskHandle"]}); err != nil {
			t.Fatal(err)
		}
	}
	if count := b.client.HLen(ctx, b.root(1)+":tasks").Val(); count != 100 {
		t.Fatalf("history count %d", count)
	}
	listed, err := b.MoveTask(ctx, 1, "list", map[string]interface{}{"sourceArn": b.ARNPrefix() + "dead"})
	if err != nil {
		t.Fatal(err)
	}
	tasks := listed["Results"].([]interface{})
	if tasks[0].(map[string]interface{})["Status"] != "COMPLETED" {
		t.Fatal("newest task starved behind old history")
	}
}
func TestPolicyCycleAndInvalidAttributesDoNotMutate(t *testing.T) {
	b := testBroker(t, false)
	ctx := context.Background()
	for _, name := range []string{"a", "b"} {
		if err := b.Create(ctx, 1, name, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	policy := func(q string) map[string]string {
		return map[string]string{"RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":1}`, b.ARNPrefix()+q)}
	}
	if err := b.SetAttributes(ctx, 1, "a", policy("b")); err != nil {
		t.Fatal(err)
	}
	if err := b.SetAttributes(ctx, 1, "b", policy("a")); err == nil {
		t.Fatal("cycle accepted")
	}
	if err := b.SetAttributes(ctx, 1, "a", map[string]string{"VisibilityTimeout": "70000", "DelaySeconds": "100"}); err == nil {
		t.Fatal("invalid attribute accepted")
	}
	attrs, err := b.Attributes(ctx, 1, "a")
	if err != nil {
		t.Fatal(err)
	}
	if attrs["VisibilityTimeout"] != "30" || attrs["DelaySeconds"] != "0" {
		t.Fatal("partial attribute mutation")
	}
}
