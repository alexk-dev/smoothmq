package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
	kongyaml "github.com/alecthomas/kong-yaml"
)

func parseTestConfig(t *testing.T, args ...string) (*CLI, error) {
	t.Helper()
	cli := &CLI{}
	parser, err := kong.New(cli, kong.Configuration(kongyaml.Loader))
	if err != nil {
		t.Fatal(err)
	}
	_, err = parser.Parse(args)
	return cli, err
}

func checkSQSOptions(t *testing.T, cli *CLI, connections, port int) {
	t.Helper()
	// Inspect by name so this regression test also runs before the fields exist.
	encoded, err := json.Marshal(cli.Server.SQS)
	if err != nil {
		t.Fatal(err)
	}
	var values map[string]interface{}
	if err := json.Unmarshal(encoded, &values); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]int{"MaxConnections": connections, "HealthPort": port} {
		if values[name] != float64(want) {
			t.Errorf("%s = %v, want %d", name, values[name], want)
		}
	}
}

func TestSQSListenerConfig(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		cli, err := parseTestConfig(t, "server")
		if err != nil {
			t.Fatal(err)
		}
		checkSQSOptions(t, cli, 1024, 3002)
	})
	t.Run("flags and disabled health", func(t *testing.T) {
		cli, err := parseTestConfig(t, "server", "--sqs-max-connections=8", "--sqs-health-port=0")
		if err != nil {
			t.Fatal(err)
		}
		checkSQSOptions(t, cli, 8, 0)
	})
	t.Run("environment", func(t *testing.T) {
		t.Setenv("Q_SQS_MAX_CONNECTIONS", "64")
		t.Setenv("Q_SQS_HEALTH_PORT", "4002")
		cli, err := parseTestConfig(t, "server")
		if err != nil {
			t.Fatal(err)
		}
		checkSQSOptions(t, cli, 64, 4002)
	})
	t.Run("yaml", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte("server:\n  sqs-max-connections: 16\n  sqs-health-port: 4003\n"), 0600); err != nil {
			t.Fatal(err)
		}
		cli, err := parseTestConfig(t, "--config="+path, "server")
		if err != nil {
			t.Fatal(err)
		}
		checkSQSOptions(t, cli, 16, 4003)
	})
	for _, flag := range []string{"sqs-max-connections", "sqs-health-port"} {
		invalid := []int{-1, 65536}
		if flag == "sqs-max-connections" {
			invalid = []int{-1, 0, 65537}
		}
		for _, value := range invalid {
			t.Run(fmt.Sprintf("%s=%d", flag, value), func(t *testing.T) {
				_, err := parseTestConfig(t, "server", fmt.Sprintf("--%s=%d", flag, value))
				if err == nil || !strings.Contains(err.Error(), flag+" must be between") {
					t.Fatalf("expected clear range error for %s, got %v", flag, err)
				}
			})
		}
	}
	t.Run("boundaries", func(t *testing.T) {
		for _, connections := range []int{1, 65536} {
			cli, err := parseTestConfig(t, "server", fmt.Sprintf("--sqs-max-connections=%d", connections), "--sqs-health-port=65535")
			if err != nil {
				t.Fatal(err)
			}
			checkSQSOptions(t, cli, connections, 65535)
		}
	})
}
