package sqs

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/gofiber/fiber/v2"
	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"
	redisqueue "github.com/poundifdef/smoothmq/queue/redis"
	"github.com/poundifdef/smoothmq/tenants/defaultmanager"
	goredis "github.com/redis/go-redis/v9"
)

func listenerConfig(t *testing.T) config.SQSConfig {
	t.Helper()
	cli := &config.CLI{}
	parser, err := kong.New(cli)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parser.Parse([]string{"server", "--sqs-max-connections=8"}); err != nil {
		t.Fatal(err)
	}
	return cli.Server.SQS
}

func healthTestBroker(t *testing.T) (*redisqueue.Broker, *goredis.Client, *exec.Cmd) {
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
	cfg := listenerConfig(t)
	client := goredis.NewClient(&goredis.Options{Addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(port))})
	t.Cleanup(func() { client.Close() })
	var broker *redisqueue.Broker
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		broker, err = redisqueue.New(redisqueue.Config{URL: fmt.Sprintf("redis://127.0.0.1:%d/0", port), Prefix: "health-test", Region: cfg.Region, AccountID: cfg.AccountID, RequireDurable: true})
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { broker.Shutdown() })
	if err := broker.Create(context.Background(), 1, "empty", nil, nil); err != nil {
		t.Fatal(err)
	}
	return broker, client, cmd
}

// Observe entry into actual Redis long polling, without replacing its behavior.
type observedPoller struct {
	*redisqueue.Broker
	entered  chan struct{}
	finished atomic.Int32
}

func (b *observedPoller) Poll(ctx context.Context, tenant int64, queue string, n int, visibility *int, wait time.Duration) ([]*models.Message, error) {
	b.entered <- struct{}{}
	defer b.finished.Add(1)
	return b.Broker.Poll(ctx, tenant, queue, n, visibility, wait)
}

func serveTestApp(t *testing.T, app *fiber.App) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// Wait for Fiber startup, not merely the TCP bind, before cleanup can run.
	started := make(chan struct{})
	app.Hooks().OnListen(func(fiber.ListenData) error { close(started); return nil })
	done := make(chan error, 1)
	go func() { done <- app.Listener(listener) }()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("listener failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("listener did not start")
	}
	t.Cleanup(func() {
		if err := app.ShutdownWithTimeout(5 * time.Second); err != nil {
			t.Error(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("listener did not stop")
		}
	})
	return listener.Addr().String()
}

func assertHealth(t *testing.T, address, path string, want int, state string) {
	t.Helper()
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	response, err := client.Get("http://" + address + path)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != want || string(body) != fmt.Sprintf(`{"state":%q}`, state) {
		t.Fatalf("%s: status=%d body=%s", path, response.StatusCode, body)
	}
}

func TestHealthDuringSQSSaturation(t *testing.T) {
	for _, combined := range []bool{false, true} {
		t.Run(fmt.Sprintf("single-port=%t", combined), func(t *testing.T) {
			broker, _, _ := healthTestBroker(t)
			cfg := listenerConfig(t)
			observed := &observedPoller{Broker: broker, entered: make(chan struct{}, cfg.MaxConnections)}
			if combined {
				cfg.BasePath = "/sqs"
			}
			sqs := NewSQS(observed, defaultmanager.NewDefaultTenantManager(cfg.Keys), cfg)
			app := sqs.App
			if combined {
				app = fiber.New(HTTPConfig(cfg.MaxRequestSize, cfg.MaxConnections))
				app.Mount("/sqs", sqs.App)
			}
			mainAddress := serveTestApp(t, app)
			healthAddress := serveTestApp(t, sqs.HealthApp)
			t.Cleanup(func() {
				if err := sqs.Stop(); err != nil {
					t.Error(err)
				}
			})
			assertHealth(t, mainAddress, cfg.BasePath+"/health/live", 200, "ok")
			assertHealth(t, mainAddress, cfg.BasePath+"/health/ready", 200, "ready")

			connections := make([]net.Conn, 0, cfg.MaxConnections)
			defer func() {
				for _, conn := range connections {
					conn.Close()
				}
			}()
			for i := 0; i < cfg.MaxConnections; i++ {
				conn, err := net.DialTimeout("tcp", mainAddress, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				connections = append(connections, conn)
				body := fmt.Sprintf(`{"QueueUrl":"http://%s%s/%s/empty","WaitTimeSeconds":20}`, mainAddress, cfg.BasePath, cfg.AccountID)
				request, err := http.NewRequest("POST", "http://"+mainAddress+cfg.BasePath+"/", strings.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Content-Type", "application/x-amz-json-1.0")
				request.Header.Set("X-Amz-Target", "AmazonSQS.ReceiveMessage")
				credentials := aws.Credentials{AccessKeyID: cfg.Keys[0].AccessKey, SecretAccessKey: cfg.Keys[0].SecretKey}
				hash := fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
				if err := v4.NewSigner().SignHTTP(context.Background(), credentials, request, hash, "sqs", cfg.Region, time.Now()); err != nil {
					t.Fatal(err)
				}
				if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
				if err := request.Write(conn); err != nil {
					t.Fatal(err)
				}
				select {
				case <-observed.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("signed request did not enter Redis long polling")
				}
			}
			// The next TCP connection must be rejected before any HTTP handler.
			excess, err := net.DialTimeout("tcp", mainAddress, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer excess.Close()
			if err := excess.SetDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			response, err := http.ReadResponse(bufio.NewReader(excess), nil)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != 503 || !strings.Contains(string(body), "Concurrency limit exceeded") {
				t.Fatalf("configured limit not enforced: status=%d body=%s", response.StatusCode, body)
			}
			for i := 0; i < 3; i++ {
				assertHealth(t, healthAddress, "/health/live", 200, "ok")
				assertHealth(t, healthAddress, "/health/ready", 200, "ready")
			}
			if observed.finished.Load() != 0 {
				t.Fatal("long polls finished before saturation checks completed")
			}
			// Shutdown must cancel long polls and close both listeners promptly.
			start := time.Now()
			if err := sqs.Stop(); err != nil {
				t.Fatal(err)
			}
			if err := app.ShutdownWithTimeout(5 * time.Second); err != nil {
				t.Fatal(err)
			}
			if elapsed := time.Since(start); elapsed > 5*time.Second {
				t.Fatalf("slow shutdown: %s", elapsed)
			}
			for _, address := range []string{mainAddress, healthAddress} {
				conn, err := net.DialTimeout("tcp", address, time.Second)
				if err == nil {
					conn.Close()
					t.Errorf("listener still accepting connections: %s", address)
				}
			}
		})
	}
}

func TestHealthReadinessUsesRedis(t *testing.T) {
	broker, client, process := healthTestBroker(t)
	cfg := listenerConfig(t)
	sqs := NewSQS(broker, defaultmanager.NewDefaultTenantManager(cfg.Keys), cfg)
	mainAddress := serveTestApp(t, sqs.App)
	healthAddress := serveTestApp(t, sqs.HealthApp)
	t.Cleanup(func() { sqs.Stop() })
	for _, address := range []string{mainAddress, healthAddress} {
		assertHealth(t, address, "/health/ready", 200, "ready")
	}
	if err := client.ConfigSet(context.Background(), "appendfsync", "everysec").Err(); err != nil {
		t.Fatal(err)
	}
	// Ready deliberately caches the durability check for up to one second.
	time.Sleep(1100 * time.Millisecond)
	for _, address := range []string{mainAddress, healthAddress} {
		assertHealth(t, address, "/health/live", 200, "ok")
		assertHealth(t, address, "/health/ready", 503, "unavailable")
	}
	if err := client.ConfigSet(context.Background(), "appendfsync", "always").Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	assertHealth(t, healthAddress, "/health/ready", 200, "ready")
	if err := process.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	process.Wait()
	for _, address := range []string{mainAddress, healthAddress} {
		assertHealth(t, address, "/health/live", 200, "ok")
		assertHealth(t, address, "/health/ready", 503, "unavailable")
	}
}

func TestHealthListenerDisabled(t *testing.T) {
	for _, test := range []struct {
		enabled bool
		port    int
	}{{false, 0}, {true, 0}, {false, 3002}} {
		cfg := listenerConfig(t)
		cfg.Enabled = test.enabled
		cfg.HealthPort = test.port
		sqs := NewSQS(nil, nil, cfg)
		defer sqs.Stop()
		if sqs.HealthApp != nil {
			t.Fatal("health app created when disabled")
		}
		if err := sqs.StartHealth("127.0.0.1"); err != nil {
			t.Fatal(err)
		}
		if test.enabled {
			address := serveTestApp(t, sqs.App)
			assertHealth(t, address, "/health/live", 200, "ok")
			assertHealth(t, address, "/health/ready", 200, "ready")
		}
	}
}

func TestHealthListenerBindFailure(t *testing.T) {
	cfg := listenerConfig(t)
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	cfg.HealthPort = occupied.Addr().(*net.TCPAddr).Port
	sqs := NewSQS(nil, nil, cfg)
	defer sqs.Stop()
	if err := sqs.StartHealth("127.0.0.1"); err == nil {
		t.Fatal("busy health port did not fail startup")
	}
}
