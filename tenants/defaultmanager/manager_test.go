package defaultmanager

import (
	"github.com/poundifdef/smoothmq/config"
	"os"
	"path/filepath"
	"testing"
)

func TestPolicyFileDoesNotFallBackToDevelopmentAdmin(t *testing.T) {
	keys := []config.AWSKey{{AccessKey: "DEV_ACCESS_KEY_ID", SecretKey: "DEV_SECRET_ACCESS_KEY"}}
	if _, err := NewPolicyManager(keys, "/missing-smoothmq-policy"); err == nil {
		t.Fatal("missing policy accepted")
	}
	file := filepath.Join(t.TempDir(), "policy.json")
	os.WriteFile(file, []byte(`{"keys":[]}`), 0600)
	if _, err := NewPolicyManager(keys, file); err == nil {
		t.Fatal("empty policy accepted")
	}
	os.WriteFile(file, []byte(`{"keys":[{"accessKey":"worker","secretKey":"secret","actions":["sqs:ReceiveMessage"],"queues":["owned-*"]}]}`), 0600)
	m, err := NewPolicyManager(keys, file)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Allowed("worker", "ReceiveMessage", "owned-a") || m.Allowed("worker", "DeleteQueue", "owned-a") || m.Allowed("worker", "ReceiveMessage", "other") || m.Allowed("DEV_ACCESS_KEY_ID", "ReceiveMessage", "owned-a") {
		t.Fatal("queue/action authorization is incorrect")
	}
}
