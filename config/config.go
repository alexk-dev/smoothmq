package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/alecthomas/kong"
	kongyaml "github.com/alecthomas/kong-yaml"
)

type CLI struct {
	Server ServerCommand `cmd:"server" help:"Run queue server"`
	Tester TesterCommand `cmd:"tester" help:"Run queue test tool"`

	Config kong.ConfigFlag `name:"config" help:"Configuration file"`
	Log    LogConfig       `embed:"" prefix:"log-" name:"log" envprefix:"LOG_"`
}

type TesterCommand struct {
	SqsEndpoint  string `help:"SQS endpoint" name:"endpoint" default:"http://localhost:3001"`
	Senders      int    `help:"" default:"0"`
	Receivers    int    `help:"" default:"0"`
	Messages     int    `help:"" default:"0"`
	BatchSize    int    `help:"" default:"1"`
	DelaySeconds int    `help:"" default:"0"`
	AccessKey    string `help:"" default:"DEV_ACCESS_KEY_ID"`
	SecretKey    string `help:"" default:"DEV_SECRET_ACCESS_KEY"`
}

type ServerCommand struct {
	SQS       SQSConfig       `embed:"" prefix:"sqs-" envprefix:"Q_SQS_"`
	Dashboard DashboardConfig `embed:"" prefix:"dashboard-" envprefix:"Q_DASHBOARD_"`
	Redis     RedisConfig     `embed:"" prefix:"redis-" envprefix:"Q_REDIS_"`
	Backend   string          `name:"backend" default:"redis" enum:"redis" env:"Q_BACKEND"`
	SQLite    SQLiteConfig    `embed:"" prefix:"sqlite-" envprefix:"Q_SQLITE_"`
	Metrics   MetricsConfig   `embed:"" prefix:"metrics-" name:"metrics" envprefix:"Q_METRICS_"`

	DisableTelemetry bool   `name:"disable-telemetry" default:"true" env:"DISABLE_TELEMETRY"`
	UseSinglePort    bool   `name:"use-single-port" default:"false" env:"Q_SERVER_USE_SINGLE_PORT" help:"Enables having all HTTP services run on a single port with different endpoints"`
	Port             int    `name:"port" default:"8080" env:"PORT" help:"If use-single-port is enabled, this is the port number for the server"`
	Host             string `name:"host" env:"HOST" help:"If use-single-port is enabled, this is the hostname or ip address for the server"`
}

type LogConfig struct {
	Pretty bool   `name:"pretty" default:"true" env:"PRETTY"`
	Level  string `name:"level" enum:"trace,debug,info,warn,error,fatal,panic" default:"info" help:"Log level" env:"LEVEL"`
}

type MetricsConfig struct {
	PrometheusEnabled bool   `name:"prometheus-enabled" default:"true" env:"PROMETHEUS_ENABLED"`
	PrometheusHost    string `name:"prometheus-host" default:"localhost" env:"PROMETHEUS_HOST"`
	PrometheusPort    int    `name:"prometheus-port" default:"2112" env:"PROMETHEUS_PORT"`
	PrometheusPath    string `name:"prometheus-path" default:"/metrics" env:"PROMETHEUS_PATH"`
}

type RedisConfig struct {
	URLFile        string `name:"url-file" env:"URL_FILE" help:"Read Redis URL from a mounted secret file"`
	URL            string `name:"url" default:"redis://localhost:6379/0" env:"URL"`
	Prefix         string `name:"prefix" default:"smoothmq" env:"PREFIX"`
	RequireDurable bool   `name:"require-durable" default:"true" env:"REQUIRE_DURABLE" help:"Require healthy AOF always and noeviction; disable only for development"`
}

type SQLiteConfig struct {
	Path string `name:"path" help:"Path of SQLite file" default:"smoothmq.sqlite" env:"PATH"`
}

type SQSConfig struct {
	MaxConnections   int      `name:"max-connections" default:"1024" env:"MAX_CONNECTIONS" help:"Maximum concurrent SQS HTTP connections (1..65536), including idle connections and long polls"`
	HealthPort       int      `name:"health-port" default:"3002" env:"HEALTH_PORT" help:"Dedicated health HTTP port (0 disables, 1..65535)"`
	BasePath         string   `kong:"-"`
	Region           string   `name:"region" default:"eu-central-1" env:"REGION"`
	AccountID        string   `name:"account-id" default:"000000000001" env:"ACCOUNT_ID"`
	PolicyFile       string   `name:"policy-file" env:"POLICY_FILE" help:"JSON per-key action and queue permissions"`
	Enabled          bool     `name:"enabled" default:"true" help:"Enable SQS protocol for queue" env:"ENABLED"`
	Host             string   `name:"host" default:"localhost" help:"hostname or ip address for SQS protocol" env:"HOST"`
	Port             int      `name:"port" default:"3001" help:"HTTP port for SQS protocol" env:"PORT"`
	Keys             []AWSKey `name:"keys" default:"DEV_ACCESS_KEY_ID:DEV_SECRET_ACCESS_KEY" env:"KEYS"`
	ParseCelery      bool     `name:"parse-celery" default:"true" env:"PARSE_CELERY" help:"Parse Celery messages. Lets you search by celery message ID and task type."`
	MaxRequestSize   int      `name:"max-request-size" default:"8388608" env:"MAX_REQUEST_SIZE" help:"Max size of SQS request in bytes"`
	MaxDelaySeconds  int      `name:"max-delay-seconds" default:"30" env:"MAX_DELAY_SECONDS" help:"Max allowed wait time for long polling"`
	DelayRetryMillis int      `name:"delay-retry-millis" default:"1000" env:"DELAY_RETRY_MILLIS" help:"When long polling, how often to request new items"`
	EndpointOverride string   `name:"endpoint-override" default:"" env:"ENDPOINT_OVERRIDE" help:"Endpoint to advertise in queue URLs. Defaults to HTTP hostname."`
}

// Validate is called by Kong for flags, environment variables and YAML config.
func (c SQSConfig) Validate() error {
	if c.MaxConnections < 1 || c.MaxConnections > 65536 {
		return fmt.Errorf("sqs-max-connections must be between 1 and 65536 (got %d)", c.MaxConnections)
	}
	if c.HealthPort < 0 || c.HealthPort > 65535 {
		return fmt.Errorf("sqs-health-port must be between 0 and 65535 (got %d)", c.HealthPort)
	}
	return nil
}

type AWSKey struct {
	AccessKey string `name:"accesskey"`
	SecretKey string `name:"secretkey"`
}

func (k *AWSKey) Decode(ctx *kong.DecodeContext) error {
	var val string

	err := ctx.Scan.PopValueInto("string", &val)
	if err != nil {
		return err
	}

	tokens := strings.Split(val, ":")
	if len(tokens) != 2 {
		return errors.New("AWS SQS key should be of the form access_key:secret_key")
	}

	k.AccessKey = tokens[0]
	k.SecretKey = tokens[1]

	return nil
}

type DashboardConfig struct {
	Enabled bool   `name:"enabled" help:"Enable web dashboard" default:"false" env:"ENABLED"`
	Host    string `name:"host" help:"hostname or ip address for dashboard" default:"localhost" env:"HOST"`
	Port    int    `name:"port" help:"HTTP port for dashboard" default:"3000" env:"PORT"`
	Dev     bool   `name:"dev" help:"Run dashboard in dev mode, refresh templates from local" default:"false" env:"DEV"`
	User    string `name:"user" help:"Username for auth" default:"" env:"USER"`
	Pass    string `name:"pass" help:"Pass for auth" default:"" env:"PASS"`
}

func Load() (string, *CLI, error) {
	cli := &CLI{}
	c := kong.Parse(cli, kong.Configuration(kongyaml.Loader))

	return c.Command(), cli, c.Error
}
