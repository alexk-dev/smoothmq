package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/dashboard"
	"github.com/poundifdef/smoothmq/models"
	"github.com/poundifdef/smoothmq/protocols/sqs"
	redisqueue "github.com/poundifdef/smoothmq/queue/redis"
	"github.com/poundifdef/smoothmq/tenants/defaultmanager"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func recordTelemetry(message string, disabled bool) {
	if disabled {
		return
	}

	url := "https://telemetry.fly.dev"
	jsonData := []byte(message)

	client := &http.Client{
		Timeout: 100 * time.Millisecond,
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		return
	}

	resp.Body.Close()
}

func Run(tm models.TenantManager, queue models.Queue, cfg config.ServerCommand) {
	if err := cfg.SQS.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if !regexp.MustCompile(`^[0-9]{12}$`).MatchString(cfg.SQS.AccountID) || !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(cfg.SQS.Region) {
		fmt.Fprintln(os.Stderr, "Invalid SQS account ID or region")
		os.Exit(1)
	}
	if cfg.Redis.URLFile != "" {
		data, err := os.ReadFile(cfg.Redis.URLFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Cannot read Redis URL secret")
			os.Exit(1)
		}
		cfg.Redis.URL = strings.TrimSpace(string(data))
	}
	recordTelemetry("start", cfg.DisableTelemetry)

	// Initialize default tenant manager
	if tm == nil {
		manager, err := defaultmanager.NewPolicyManager(cfg.SQS.Keys, cfg.SQS.PolicyFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if cfg.Redis.RequireDurable && (cfg.SQS.PolicyFile == "" || !manager.ProductionSafe()) {
			fmt.Fprintln(os.Stderr, "Production requires a policy file with non-development keys and secrets of at least 32 characters")
			os.Exit(1)
		}
		tm = manager
	}

	// Initialize default queue implementation
	if queue == nil {
		backend, err := redisqueue.New(redisqueue.Config{URL: cfg.Redis.URL, Prefix: cfg.Redis.Prefix, Region: cfg.SQS.Region, AccountID: cfg.SQS.AccountID, RequireDurable: cfg.Redis.RequireDurable})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		queue = backend
	}

	if cfg.Dashboard.Enabled && (cfg.Dashboard.User == "" || cfg.Dashboard.Pass == "") {
		fmt.Fprintln(os.Stderr, "Dashboard requires explicit authentication")
		queue.Shutdown()
		os.Exit(1)
	}
	dashboardServer := dashboard.NewDashboard(queue, tm, cfg.Dashboard)
	if cfg.UseSinglePort {
		cfg.SQS.BasePath = "/sqs"
	}
	sqsServer := sqs.NewSQS(queue, tm, cfg.SQS)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	failures := make(chan error, 4)
	start := func(fn func() error) {
		go func() {
			if err := fn(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				failures <- err
			}
		}()
	}
	var combined *fiber.App
	var metrics *http.Server
	if cfg.UseSinglePort {
		combined = fiber.New(sqs.HTTPConfig(cfg.SQS.MaxRequestSize, cfg.SQS.MaxConnections))
		if cfg.SQS.Enabled {
			combined.Mount("/sqs", sqsServer.App)
		}
		if cfg.Metrics.PrometheusEnabled {
			combined.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))
		}
		if cfg.Dashboard.Enabled {
			combined.Mount("/", dashboardServer.App)
		}
		start(func() error { return combined.Listen(fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)) })
	} else {
		if cfg.SQS.Enabled {
			start(sqsServer.Start)
		}
		if cfg.Dashboard.Enabled {
			start(dashboardServer.Start)
		}
		if cfg.Metrics.PrometheusEnabled {
			mux := http.NewServeMux()
			mux.Handle(cfg.Metrics.PrometheusPath, promhttp.Handler())
			metrics = &http.Server{Addr: fmt.Sprintf("%s:%d", cfg.Metrics.PrometheusHost, cfg.Metrics.PrometheusPort), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
			start(metrics.ListenAndServe)
		}
	}
	if cfg.SQS.Enabled && cfg.SQS.HealthPort != 0 {
		healthHost := cfg.SQS.Host
		if cfg.UseSinglePort {
			healthHost = cfg.Host
		}
		start(func() error { return sqsServer.StartHealth(healthHost) })
	}
	failed := false
	select {
	case <-signals:
	case <-failures:
		failed = true
		log.Error().Str("event", "server_listener_failed").Msg("Listener could not start or stopped unexpectedly")
	}
	sqsServer.Stop() // Cancels long polls before waiting for HTTP shutdown.
	if combined != nil {
		combined.Shutdown()
	}
	dashboardServer.Stop()
	if metrics != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		metrics.Shutdown(ctx)
		cancel()
	}
	queue.Shutdown()
	recordTelemetry("stop", cfg.DisableTelemetry)
	if failed {
		os.Exit(1)
	}
}
