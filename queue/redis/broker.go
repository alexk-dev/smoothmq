// Package redis implements the durable SQS state machine on a Redis primary.
package redis

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/poundifdef/smoothmq/models"
	goredis "github.com/redis/go-redis/v9"
)

//go:embed engine.lua
var engine string
var queueName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)
var namespace = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

type Config struct {
	URL, Prefix, Region, AccountID string
	RequireDurable                 bool
}
type Broker struct {
	client        *goredis.Client
	cfg           Config
	script        *goredis.Script
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	durabilityMu  sync.Mutex
	durabilityAt  time.Time
	durabilityErr error
}

var _ models.Queue = (*Broker)(nil)
var _ models.Broker = (*Broker)(nil)

func New(cfg Config) (*Broker, error) {
	if !namespace.MatchString(cfg.Prefix) {
		return nil, fmt.Errorf("invalid Redis prefix")
	}
	options, err := goredis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis URL")
	}
	// Do not retry ambiguous writes. Producers/DB outboxes handle at-least-once delivery.
	options.ContextTimeoutEnabled = true
	options.MaxRetries = -1
	options.DialTimeout = 3 * time.Second
	options.ReadTimeout = 5 * time.Second
	options.WriteTimeout = 5 * time.Second
	b := &Broker{client: goredis.NewClient(options), cfg: cfg, script: goredis.NewScript(engine)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = b.Ready(ctx); err != nil {
		b.client.Close()
		return nil, err
	}
	ctxLoop, stop := context.WithCancel(context.Background())
	b.cancel = stop
	b.wg.Add(1)
	go b.maintain(ctxLoop)
	return b, nil
}
func (b *Broker) ARNPrefix() string {
	return "arn:aws:sqs:" + b.cfg.Region + ":" + b.cfg.AccountID + ":"
}
func (b *Broker) root(tenant int64) string { return fmt.Sprintf("%s:{%d}", b.cfg.Prefix, tenant) }
func token() string {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic("secure random unavailable")
	}
	return hex.EncodeToString(raw[:])
}
func qerr(code, msg string) error { return &models.QueueError{Code: code, Message: msg} }
func (b *Broker) Ready(ctx context.Context) error {
	if err := b.client.Ping(ctx).Err(); err != nil {
		return qerr("ServiceUnavailable", "Redis is unavailable")
	}
	if !b.cfg.RequireDurable {
		return nil
	}
	b.durabilityMu.Lock()
	defer b.durabilityMu.Unlock()
	if time.Since(b.durabilityAt) < time.Second {
		return b.durabilityErr
	}
	b.durabilityAt = time.Now()
	b.durabilityErr = b.verifyDurability(ctx)
	return b.durabilityErr
}
func (b *Broker) verifyDurability(ctx context.Context) error {
	settings := map[string]string{}
	commands := map[string]*goredis.MapStringStringCmd{}
	_, err := b.client.Pipelined(ctx, func(pipe goredis.Pipeliner) error {
		for _, name := range []string{"appendonly", "appendfsync", "maxmemory-policy", "stop-writes-on-bgsave-error", "no-appendfsync-on-rewrite", "cluster-enabled"} {
			commands[name] = pipe.ConfigGet(ctx, name)
		}
		return nil
	})
	for name, command := range commands {
		settings[name] = command.Val()[name]
	}
	if err != nil {
		return qerr("ServiceUnavailable", "Cannot verify Redis durability configuration")
	}
	for k, v := range map[string]string{"appendonly": "yes", "appendfsync": "always", "maxmemory-policy": "noeviction", "stop-writes-on-bgsave-error": "yes", "no-appendfsync-on-rewrite": "no"} {
		if settings[k] != v {
			return qerr("ServiceUnavailable", "Redis durability requirement not satisfied: "+k)
		}
	}
	if settings["cluster-enabled"] == "yes" {
		return qerr("ServiceUnavailable", "Redis Cluster is unsupported")
	}
	info, err := b.client.Info(ctx, "persistence").Result()
	if err != nil || !strings.Contains(info, "aof_last_write_status:ok") {
		return qerr("ServiceUnavailable", "Redis AOF is not healthy")
	}
	return nil
}
func (b *Broker) call(ctx context.Context, tenant int64, op, q string, args map[string]interface{}) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if b.cfg.RequireDurable {
		if err := b.Ready(ctx); err != nil {
			return "", err
		}
	}
	if args == nil {
		args = map[string]interface{}{}
	}
	args["arnPrefix"] = b.ARNPrefix()
	payload, err := json.Marshal(args)
	if err != nil {
		return "", err
	}
	result, err := b.script.Run(ctx, b.client, []string{b.root(tenant)}, op, q, string(payload)).Text()
	if err != nil {
		if strings.HasPrefix(err.Error(), "SMQ|") {
			p := strings.SplitN(err.Error(), "|", 3)
			if len(p) == 3 {
				return "", qerr(p[1], p[2])
			}
		}
		return "", qerr("ServiceUnavailable", "Redis operation did not complete; delivery outcome may be unknown")
	}
	return result, nil
}
func (b *Broker) maintain(ctx context.Context) {
	defer b.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var cursor uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Default tenant manager has one account. Per-key queue permissions are enforced by SQS.
			names, next, err := b.client.SScan(ctx, b.root(1)+":queues", cursor, "", 64).Result()
			if err != nil {
				continue
			}
			cursor = next
			for _, name := range names {
				if ctx.Err() != nil {
					return
				}
				b.call(ctx, 1, "maintain", name, nil)
			}
			ids, err := b.client.ZRange(ctx, b.root(1)+":task-index", 0, 99).Result()
			if err != nil {
				continue
			}
			for _, id := range ids {
				raw, err := b.client.HGet(ctx, b.root(1)+":tasks", id).Result()
				if err != nil {
					continue
				}
				var t struct {
					Source string `json:"source"`
					Status string `json:"Status"`
				}
				if json.Unmarshal([]byte(raw), &t) == nil && t.Status == "RUNNING" {
					b.call(ctx, 1, "task-step", t.Source, map[string]interface{}{"handle": id})
				}
			}
		}
	}
}
func (b *Broker) Shutdown() error { b.cancel(); b.wg.Wait(); return b.client.Close() }

var defaults = map[string]string{"VisibilityTimeout": "30", "MessageRetentionPeriod": "345600", "ReceiveMessageWaitTimeSeconds": "0", "DelaySeconds": "0", "MaximumMessageSize": "1048576", "SqsManagedSseEnabled": "false"}

func (b *Broker) validateAttrs(input map[string]string) (map[string]string, error) {
	out := map[string]string{}
	for k, v := range input {
		switch k {
		case "VisibilityTimeout", "MessageRetentionPeriod", "ReceiveMessageWaitTimeSeconds", "DelaySeconds", "MaximumMessageSize":
			ranges := map[string][2]int{"VisibilityTimeout": {0, 43200}, "MessageRetentionPeriod": {60, 1209600}, "ReceiveMessageWaitTimeSeconds": {0, 20}, "DelaySeconds": {0, 900}, "MaximumMessageSize": {1024, 1048576}}
			n, err := strconv.Atoi(v)
			r := ranges[k]
			if err != nil || n < r[0] || n > r[1] {
				return nil, qerr("InvalidAttributeValue", "Invalid "+k)
			}
			out[k] = strconv.Itoa(n)
		case "SqsManagedSseEnabled":
			if v != "false" {
				return nil, qerr("UnsupportedOperation", "AWS SSE is unavailable; configure Redis disk encryption and TLS")
			}
			out[k] = v
		case "RedrivePolicy":
			if v == "" {
				out[k] = v
				continue
			}
			var p struct {
				Target string          `json:"deadLetterTargetArn"`
				Count  json.RawMessage `json:"maxReceiveCount"`
			}
			if json.Unmarshal([]byte(v), &p) != nil || !b.validARN(p.Target) {
				return nil, qerr("InvalidAttributeValue", "Invalid RedrivePolicy")
			}
			count, err := strconv.Atoi(strings.Trim(string(p.Count), `"`))
			if err != nil || count < 1 || count > 1000 {
				return nil, qerr("InvalidAttributeValue", "Invalid maxReceiveCount")
			}
			canonical, _ := json.Marshal(map[string]interface{}{"deadLetterTargetArn": p.Target, "maxReceiveCount": count})
			out[k] = string(canonical)
		case "RedriveAllowPolicy":
			if v == "" {
				out[k] = v
				continue
			}
			var p struct {
				Permission string   `json:"redrivePermission"`
				Sources    []string `json:"sourceQueueArns,omitempty"`
			}
			if json.Unmarshal([]byte(v), &p) != nil {
				return nil, qerr("InvalidAttributeValue", "Invalid RedriveAllowPolicy")
			}
			switch p.Permission {
			case "allowAll", "denyAll":
				if len(p.Sources) > 0 {
					return nil, qerr("InvalidAttributeValue", "Source queues require byQueue")
				}
			case "byQueue":
				if len(p.Sources) < 1 || len(p.Sources) > 10 {
					return nil, qerr("InvalidAttributeValue", "byQueue requires 1..10 sources")
				}
			default:
				return nil, qerr("InvalidAttributeValue", "Invalid redrivePermission")
			}
			for _, source := range p.Sources {
				if !b.validARN(source) {
					return nil, qerr("InvalidAttributeValue", "Invalid source ARN")
				}
			}
			sort.Strings(p.Sources)
			canonical, _ := json.Marshal(p)
			out[k] = string(canonical)
		default:
			return nil, qerr("InvalidAttributeName", "Unsupported attribute: "+k)
		}
	}
	return out, nil
}
func (b *Broker) validARN(s string) bool {
	return strings.HasPrefix(s, b.ARNPrefix()) && queueName.MatchString(strings.TrimPrefix(s, b.ARNPrefix()))
}
func (b *Broker) Create(ctx context.Context, t int64, q string, attrs, tags map[string]string) error {
	if !queueName.MatchString(q) {
		return qerr("InvalidParameterValue", "Only standard queue names (1..80 letters, digits, hyphen, underscore) are supported")
	}
	clean, err := b.validateAttrs(attrs)
	if err != nil {
		return err
	}
	all := map[string]string{}
	for k, v := range defaults {
		all[k] = v
	}
	for k, v := range clean {
		if v != "" {
			all[k] = v
		}
	}
	if tags == nil {
		tags = map[string]string{}
	}
	if len(tags) > 50 {
		return qerr("InvalidParameterValue", "At most 50 tags are allowed")
	}
	_, err = b.call(ctx, t, "create", q, map[string]interface{}{"attrs": all, "requested": clean, "tags": tags, "generation": token()})
	return err
}
func (b *Broker) Attributes(ctx context.Context, t int64, q string) (map[string]string, error) {
	raw, err := b.call(ctx, t, "attrs", q, nil)
	if err != nil {
		return nil, err
	}
	var result map[string]string
	err = json.Unmarshal([]byte(raw), &result)
	return result, err
}
func (b *Broker) SetAttributes(ctx context.Context, t int64, q string, attrs map[string]string) error {
	clean, err := b.validateAttrs(attrs)
	if err != nil {
		return err
	}
	_, err = b.call(ctx, t, "set", q, map[string]interface{}{"attrs": clean})
	return err
}
func (b *Broker) Send(ctx context.Context, t int64, q, body string, kv map[string]string, delay *int) (int64, error) {
	if len(body) == 0 || len(body) > 1048576 {
		return 0, qerr("InvalidParameterValue", "Message body must be 1..1048576 bytes")
	}
	if delay != nil && (*delay < 0 || *delay > 900) {
		return 0, qerr("InvalidParameterValue", "DelaySeconds must be 0..900")
	}
	if kv == nil {
		kv = map[string]string{}
	}
	args := map[string]interface{}{"body": body, "kv": kv}
	if delay != nil {
		args["delay"] = *delay
	}
	raw, err := b.call(ctx, t, "send", q, args)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(raw, 10, 64)
}

type storedMessage struct {
	ID         string            `json:"id"`
	Body       string            `json:"body"`
	KV         map[string]string `json:"kv"`
	Sent       int64             `json:"sent"`
	First      int64             `json:"first"`
	Count      int               `json:"count"`
	Available  int64             `json:"available"`
	Receipt    string            `json:"receipt"`
	Source     string            `json:"source"`
	LeaseStart int64             `json:"leaseStart"`
}

func (m storedMessage) model(t int64) *models.Message {
	id, _ := strconv.ParseInt(m.ID, 10, 64)
	return &models.Message{ID: id, TenantID: t, DeliverAt: int(m.Available / 1000), DeliveredAt: int(m.LeaseStart / 1000), Tries: m.Count, MaxTries: -1, Message: []byte(m.Body), KeyValues: m.KV, ReceiptHandle: m.Receipt, SentTimestamp: m.Sent, FirstReceiveTimestamp: m.First, SourceARN: m.Source}
}
func (b *Broker) Receive(ctx context.Context, t int64, q string, n int, visibility *int) ([]*models.Message, error) {
	return b.receive(ctx, t, q, n, visibility, "")
}
func (b *Broker) receive(ctx context.Context, t int64, q string, n int, visibility *int, generation string) ([]*models.Message, error) {
	if n < 1 || n > 10 {
		return nil, qerr("InvalidParameterValue", "MaxNumberOfMessages must be 1..10")
	}
	if visibility != nil && (*visibility < 0 || *visibility > 43200) {
		return nil, qerr("InvalidParameterValue", "VisibilityTimeout must be 0..43200")
	}
	tokens := make([]string, n)
	for i := range tokens {
		tokens[i] = token()
	}
	args := map[string]interface{}{"count": n, "tokens": tokens}
	if visibility != nil {
		args["visibility"] = *visibility
	}
	if generation != "" {
		args["generation"] = generation
	}
	raw, err := b.call(ctx, t, "receive", q, args)
	if err != nil {
		return nil, err
	}
	var stored []storedMessage
	if err = json.Unmarshal([]byte(raw), &stored); err != nil {
		return nil, err
	}
	out := make([]*models.Message, 0, len(stored))
	for _, m := range stored {
		out = append(out, m.model(t))
	}
	return out, nil
}

// Poll remembers the queue generation, including across Delete/Create or Purge.
func (b *Broker) Poll(ctx context.Context, t int64, q string, n int, visibility *int, wait time.Duration) ([]*models.Message, error) {
	generation, err := b.call(ctx, t, "generation", q, nil)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(wait)
	for {
		messages, err := b.receive(ctx, t, q, n, visibility, generation)
		if err != nil || len(messages) > 0 || !time.Now().Before(deadline) {
			return messages, err
		}
		pause := time.NewTimer(min(100*time.Millisecond, time.Until(deadline)))
		select {
		case <-ctx.Done():
			pause.Stop()
			return nil, ctx.Err()
		case <-pause.C:
		}
	}
}
func (b *Broker) Acknowledge(ctx context.Context, t int64, q, receipt string) error {
	_, err := b.call(ctx, t, "ack", q, map[string]interface{}{"receipt": receipt})
	return err
}
func (b *Broker) Visibility(ctx context.Context, t int64, q, receipt string, seconds int) error {
	if seconds < 0 || seconds > 43200 {
		return qerr("InvalidParameterValue", "VisibilityTimeout must be 0..43200")
	}
	_, err := b.call(ctx, t, "visibility", q, map[string]interface{}{"receipt": receipt, "visibility": seconds})
	return err
}
func (b *Broker) Purge(ctx context.Context, t int64, q string) error {
	_, err := b.call(ctx, t, "purge", q, map[string]interface{}{"generation": token()})
	return err
}
func (b *Broker) Tags(ctx context.Context, t int64, q string, add map[string]string, remove []string) (map[string]string, error) {
	if add == nil {
		add = map[string]string{}
	}
	if remove == nil {
		remove = []string{}
	}
	raw, err := b.call(ctx, t, "tags", q, map[string]interface{}{"add": add, "remove": remove})
	if err != nil {
		return nil, err
	}
	var out map[string]string
	err = json.Unmarshal([]byte(raw), &out)
	return out, err
}
func (b *Broker) MoveTask(ctx context.Context, t int64, op string, args map[string]interface{}) (map[string]interface{}, error) {
	source, _ := args["sourceArn"].(string)
	if !b.validARN(source) {
		return nil, qerr("InvalidAddress", "Invalid source ARN")
	}
	q := strings.TrimPrefix(source, b.ARNPrefix())
	if op == "start" {
		dest, _ := args["destinationArn"].(string)
		if dest != "" && !b.validARN(dest) {
			return nil, qerr("InvalidAddress", "Invalid destination ARN")
		}
		args["destination"] = strings.TrimPrefix(dest, b.ARNPrefix())
		args["handle"] = q + ":" + token()
		rate, ok := args["rate"].(int)
		if !ok || rate == 0 {
			rate = 100
		}
		if rate < 1 || rate > 500 {
			return nil, qerr("InvalidParameterValue", "Move rate must be 1..500")
		}
		args["rate"] = rate
	}
	raw, err := b.call(ctx, t, "task-"+op, q, args)
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	err = json.Unmarshal([]byte(raw), &out)
	return out, err
}

// Dashboard adapters. Message edits/deletions cannot bypass receipt ownership.
func (b *Broker) CreateQueue(t int64, p models.QueueProperties) error {
	return b.Create(context.Background(), t, p.Name, map[string]string{"VisibilityTimeout": strconv.Itoa(p.VisibilityTimeout)}, nil)
}
func (b *Broker) GetQueue(t int64, q string) (models.QueueProperties, error) {
	attrs, err := b.Attributes(context.Background(), t, q)
	v, _ := strconv.Atoi(attrs["VisibilityTimeout"])
	return models.QueueProperties{Name: q, VisibilityTimeout: v, MaxRetries: -1, RateLimit: -1}, err
}
func (b *Broker) UpdateQueue(t int64, q string, p models.QueueProperties) error {
	if p.MaxRetries != -1 || p.RateLimit != -1 {
		return qerr("UnsupportedOperation", "Configure SQS RedrivePolicy; legacy rate/retry settings are unsupported")
	}
	return b.SetAttributes(context.Background(), t, q, map[string]string{"VisibilityTimeout": strconv.Itoa(p.VisibilityTimeout)})
}
func (b *Broker) DeleteQueue(t int64, q string) error {
	_, err := b.call(context.Background(), t, "delete", q, nil)
	return err
}
func (b *Broker) ListQueues(t int64) ([]string, error) {
	raw, err := b.call(context.Background(), t, "list", "", nil)
	if err != nil {
		return nil, err
	}
	var out []string
	err = json.Unmarshal([]byte(raw), &out)
	sort.Strings(out)
	return out, err
}
func (b *Broker) Enqueue(t int64, q, body string, kv map[string]string, delay int) (int64, error) {
	return b.Send(context.Background(), t, q, body, kv, &delay)
}
func (b *Broker) Dequeue(t int64, q string, n, v int) ([]*models.Message, error) {
	var visibility *int
	if v >= 0 {
		visibility = &v
	}
	return b.Receive(context.Background(), t, q, n, visibility)
}
func (b *Broker) UpdateMessage(int64, string, int64, *models.Message) error {
	return qerr("UnsupportedOperation", "Use a current SQS receipt to change visibility")
}
func (b *Broker) Delete(int64, string, int64) error {
	return qerr("UnsupportedOperation", "Use a current SQS receipt to acknowledge")
}
func (b *Broker) Peek(t int64, q string, id int64) *models.Message {
	raw, err := b.call(context.Background(), t, "inspect", q, map[string]interface{}{"id": strconv.FormatInt(id, 10)})
	if err != nil || raw == "{}" {
		return nil
	}
	var m storedMessage
	if json.Unmarshal([]byte(raw), &m) != nil {
		return nil
	}
	return m.model(t)
}
func (b *Broker) Stats(t int64, q string) models.QueueStats {
	attrs, err := b.Attributes(context.Background(), t, q)
	if err != nil {
		return models.QueueStats{Counts: map[models.MessageStatus]int{}}
	}
	n, _ := strconv.Atoi(attrs["ApproximateNumberOfMessages"])
	d, _ := strconv.Atoi(attrs["ApproximateNumberOfMessagesDelayed"])
	f, _ := strconv.Atoi(attrs["ApproximateNumberOfMessagesNotVisible"])
	return models.QueueStats{TotalMessages: n + d + f, Counts: map[models.MessageStatus]int{models.MessageStatusQueued: n + d, models.MessageStatusDequeued: f}}
}
func (b *Broker) Filter(t int64, q string, f models.FilterCriteria) []int64 {
	raw, err := b.call(context.Background(), t, "browse", q, nil)
	if err != nil {
		return nil
	}
	var messages []storedMessage
	if json.Unmarshal([]byte(raw), &messages) != nil {
		return nil
	}
	out := []int64{}
	for _, m := range messages {
		model := m.model(t)
		if f.MessageID != 0 && model.ID != f.MessageID {
			continue
		}
		if model.ID < f.MinMessageID {
			continue
		}
		matched := true
		for k, v := range f.KV {
			if m.KV[k] != v {
				matched = false
			}
		}
		if matched {
			out = append(out, model.ID)
		}
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}
	return out
}

// ErrorCode is useful to callers that distinguish transport failures from 4xx.
func ErrorCode(err error) string {
	var e *models.QueueError
	if errors.As(err, &e) {
		return e.Code
	}
	return "ServiceUnavailable"
}
