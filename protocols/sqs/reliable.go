package sqs

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gofiber/fiber/v2"
	"github.com/poundifdef/smoothmq/models"
)

type brokerEntry struct {
	ID                     string `json:"Id"`
	MessageBody            string
	ReceiptHandle          string
	VisibilityTimeout      *int
	DelaySeconds           *int
	MessageAttributes      map[string]MessageAttributeValue
	MessageDeduplicationId string
	MessageGroupId         string
}
type brokerRequest struct {
	QueueUrl                     string
	QueueName                    string
	QueueNamePrefix              string
	QueueOwnerAWSAccountId       string
	Attributes                   map[string]string
	Tags                         map[string]string
	TagKeys                      []string
	AttributeNames               []string
	MessageSystemAttributeNames  []string
	MessageAttributeNames        []string
	MessageBody                  string
	MessageAttributes            map[string]MessageAttributeValue
	MessageDeduplicationId       string
	MessageGroupId               string
	MessageSystemAttributes      map[string]json.RawMessage
	DelaySeconds                 *int
	VisibilityTimeout            *int
	WaitTimeSeconds              *int
	MaxNumberOfMessages          *int
	ReceiptHandle                string
	ReceiveRequestAttemptId      string
	Entries                      []brokerEntry
	SourceArn                    string
	DestinationArn               string
	TaskHandle                   string
	MaxNumberOfMessagesPerSecond int
	MaxResults                   int
	MaxResultsList               *int `json:"MaxResultsList,omitempty"`
	NextToken                    string
}

var batchID = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)
var attributeName = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,256}$`)

func invalid(message string) error { return NewSQSError(400, "InvalidParameterValue", message) }
func (s *SQS) allowed(c *fiber.Ctx, action, queue string) bool {
	auth, ok := s.tenantManager.(models.Authorizer)
	if !ok {
		return false
	}
	access, _ := c.Locals("accessKey").(string)
	return auth.Allowed(access, action, queue)
}
func (s *SQS) require(c *fiber.Ctx, action, queue string) error {
	if !s.allowed(c, action, queue) {
		return NewSQSError(403, "AccessDenied", "This key is not authorized for this operation and queue")
	}
	return nil
}
func (s *SQS) queueName(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return "", invalid("Invalid QueueUrl")
	}
	prefix := s.cfg.BasePath
	if s.cfg.EndpointOverride != "" {
		base, err := url.Parse(s.cfg.EndpointOverride)
		if err != nil {
			return "", invalid("Invalid endpoint configuration")
		}
		prefix = strings.TrimRight(base.Path, "/")
	}
	if prefix != "" && !strings.HasPrefix(u.Path, prefix+"/") {
		return "", invalid("Invalid QueueUrl path prefix")
	}
	path := strings.Split(strings.Trim(strings.TrimPrefix(u.Path, prefix), "/"), "/")
	if len(path) != 2 || path[0] != s.cfg.AccountID || !batchID.MatchString(path[1]) {
		return "", invalid("QueueUrl must belong to this account and contain a standard queue name")
	}
	return path[1], nil
}
func (s *SQS) arnName(arn string) (string, error) {
	p := "arn:aws:sqs:" + s.cfg.Region + ":" + s.cfg.AccountID + ":"
	if !strings.HasPrefix(arn, p) || !batchID.MatchString(strings.TrimPrefix(arn, p)) {
		return "", invalid("ARN must belong to this account and region")
	}
	return strings.TrimPrefix(arn, p), nil
}
func (s *SQS) reliableAction(c *fiber.Ctx, t int64, b models.Broker) error {
	target, _ := c.Locals("sqsTarget").(string)
	if !strings.HasPrefix(target, "AmazonSQS.") {
		return NewSQSError(400, "UnsupportedOperation", "Invalid SQS target")
	}
	method := strings.TrimPrefix(target, "AmazonSQS.")
	if err := validateJSONBudget(c.Body()); err != nil {
		return err
	}
	var r brokerRequest
	decoder := json.NewDecoder(bytes.NewReader(c.Body()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil {
		return invalid("Malformed or unsupported request fields")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return invalid("Expected one JSON object")
	}
	ctx := s.ctx
	if r.MessageDeduplicationId != "" || r.MessageGroupId != "" || r.ReceiveRequestAttemptId != "" || len(r.MessageSystemAttributes) > 0 {
		return NewSQSError(400, "UnsupportedOperation", "FIFO and message system attributes on SendMessage are unsupported")
	}
	if r.NextToken != "" {
		return invalid("Pagination tokens are unsupported")
	}
	if method == "ListQueues" {
		queues, err := s.queue.ListQueues(t)
		if err != nil {
			return err
		}
		out := []string{}
		for _, q := range queues {
			if strings.HasPrefix(q, r.QueueNamePrefix) && s.allowed(c, method, q) {
				out = append(out, s.queueURL(c, t, q))
			}
		}
		if len(out) == 0 && !s.allowed(c, method, "*") { // A scoped grant can legitimately see an empty list.
			access, _ := c.Locals("accessKey").(string)
			if v, ok := s.tenantManager.(interface{ HasAction(string, string) bool }); !ok || !v.HasAction(access, method) {
				return NewSQSError(403, "AccessDenied", "ListQueues is not authorized")
			}
		}
		if r.MaxResults > 0 && len(out) > r.MaxResults {
			return invalid("Result exceeds MaxResults; use a narrower prefix")
		}
		return c.JSON(map[string]interface{}{"QueueUrls": out})
	}
	if method == "StartMessageMoveTask" || method == "ListMessageMoveTasks" || method == "CancelMessageMoveTask" {
		if method == "CancelMessageMoveTask" {
			parts := strings.Split(r.TaskHandle, ":")
			if len(parts) != 2 || !batchID.MatchString(parts[0]) {
				return invalid("Invalid TaskHandle")
			}
			r.SourceArn = "arn:aws:sqs:" + s.cfg.Region + ":" + s.cfg.AccountID + ":" + parts[0]
		}
		name, err := s.arnName(r.SourceArn)
		if err != nil {
			return err
		}
		if err = s.require(c, method, name); err != nil {
			return err
		}
		op := "list"
		args := map[string]interface{}{"sourceArn": r.SourceArn}
		if method == "StartMessageMoveTask" {
			op = "start"
			dest := "*"
			if r.DestinationArn != "" {
				dest, err = s.arnName(r.DestinationArn)
				if err != nil {
					return err
				}
			}
			for _, action := range []string{"ReceiveMessage", "DeleteMessage"} {
				if err = s.require(c, action, name); err != nil {
					return err
				}
			}
			if err = s.require(c, "SendMessage", dest); err != nil {
				return err
			}
			args["destinationArn"] = r.DestinationArn
			args["rate"] = r.MaxNumberOfMessagesPerSecond
		} else if method == "CancelMessageMoveTask" {
			op = "cancel"
			args["handle"] = r.TaskHandle
		}
		result, err := b.MoveTask(ctx, t, op, args)
		if err != nil {
			return err
		}
		if items, ok := result["Results"].([]interface{}); ok {
			max := r.MaxResults
			if max == 0 {
				max = 1
			}
			if max < 1 || max > 10 {
				return invalid("MaxResults must be 1..10")
			}
			if len(items) > max {
				items = items[:max]
			}
			for _, item := range items {
				task := item.(map[string]interface{})
				for _, k := range []string{"source", "destination", "generation", "lastStep"} {
					delete(task, k)
				}
			}
			result["Results"] = items
		}
		return c.JSON(result)
	}
	name := r.QueueName
	if method != "CreateQueue" && method != "GetQueueUrl" {
		var err error
		name, err = s.queueName(r.QueueUrl)
		if err != nil {
			return err
		}
	} else if !batchID.MatchString(name) {
		return invalid("Invalid standard queue name")
	}
	if err := s.require(c, method, name); err != nil {
		return err
	}
	switch method {
	case "CreateQueue":
		if err := validateTags(r.Tags); err != nil {
			return err
		}
		if err := b.Create(ctx, t, name, r.Attributes, r.Tags); err != nil {
			return err
		}
		return c.JSON(CreateQueueResponse{QueueUrl: s.queueURL(c, t, name)})
	case "GetQueueUrl":
		if r.QueueOwnerAWSAccountId != "" && r.QueueOwnerAWSAccountId != s.cfg.AccountID {
			return NewSQSError(403, "AccessDenied", "Queue belongs to another account")
		}
		if _, err := b.Attributes(ctx, t, name); err != nil {
			return err
		}
		return c.JSON(GetQueueURLResponse{QueueURL: s.queueURL(c, t, name)})
	case "GetQueueAttributes":
		attrs, err := b.Attributes(ctx, t, name)
		if err != nil {
			return err
		}
		out := map[string]string{}
		for _, k := range r.AttributeNames {
			if k == "All" {
				out = attrs
				break
			}
			if v, ok := attrs[k]; ok {
				out[k] = v
			} else if k != "RedrivePolicy" && k != "RedriveAllowPolicy" {
				return NewSQSError(400, "InvalidAttributeName", "Unsupported attribute: "+k)
			}
		}
		return c.JSON(GetQueueAttributesResponse{Attributes: out})
	case "SetQueueAttributes":
		if len(r.Attributes) == 0 {
			return invalid("Attributes are required")
		}
		if err := b.SetAttributes(ctx, t, name, r.Attributes); err != nil {
			return err
		}
		return c.JSON(fiber.Map{})
	case "DeleteQueue":
		if err := s.queue.DeleteQueue(t, name); err != nil {
			return err
		}
		return c.JSON(fiber.Map{})
	case "PurgeQueue":
		if err := b.Purge(ctx, t, name); err != nil {
			return err
		}
		return c.JSON(fiber.Map{})
	case "ListQueueTags", "TagQueue", "UntagQueue":
		add := map[string]string{}
		var remove []string
		if method == "TagQueue" {
			if err := validateTags(r.Tags); err != nil {
				return err
			}
			add = r.Tags
		}
		if method == "UntagQueue" {
			remove = r.TagKeys
		}
		tags, err := b.Tags(ctx, t, name, add, remove)
		if err != nil {
			return err
		}
		if method == "ListQueueTags" {
			return c.JSON(fiber.Map{"Tags": tags})
		}
		return c.JSON(fiber.Map{})
	case "SendMessage":
		response, err := s.send(ctx, b, t, name, brokerEntry{MessageBody: r.MessageBody, DelaySeconds: r.DelaySeconds, MessageAttributes: r.MessageAttributes})
		if err != nil {
			return err
		}
		return c.JSON(response)
	case "ReceiveMessage":
		n := 1
		if r.MaxNumberOfMessages != nil {
			n = *r.MaxNumberOfMessages
		}
		if n < 1 || n > 10 {
			return invalid("MaxNumberOfMessages must be 1..10")
		}
		if r.VisibilityTimeout != nil && (*r.VisibilityTimeout < 0 || *r.VisibilityTimeout > 43200) {
			return invalid("VisibilityTimeout must be 0..43200")
		}
		wait := 0
		if r.WaitTimeSeconds != nil {
			wait = *r.WaitTimeSeconds
		} else {
			attrs, err := b.Attributes(ctx, t, name)
			if err != nil {
				return err
			}
			wait, _ = strconv.Atoi(attrs["ReceiveMessageWaitTimeSeconds"])
		}
		if wait < 0 || wait > 20 {
			return invalid("WaitTimeSeconds must be 0..20")
		}
		var messages []*models.Message
		var err error
		if poller, ok := b.(interface {
			Poll(context.Context, int64, string, int, *int, time.Duration) ([]*models.Message, error)
		}); ok {
			messages, err = poller.Poll(ctx, t, name, n, r.VisibilityTimeout, time.Duration(wait)*time.Second)
		} else {
			messages, err = b.Receive(ctx, t, name, n, r.VisibilityTimeout)
		}
		if err != nil {
			return err
		}
		result := make([]Message, 0, len(messages))
		for _, m := range messages {
			reply := Message{MessageId: strconv.FormatInt(m.ID, 10), ReceiptHandle: m.ReceiptHandle, Body: string(m.Message), MD5OfBody: md5hex(m.Message)}
			attrs := map[string]string{"ApproximateReceiveCount": strconv.Itoa(m.Tries), "SentTimestamp": strconv.FormatInt(m.SentTimestamp, 10), "ApproximateFirstReceiveTimestamp": strconv.FormatInt(m.FirstReceiveTimestamp, 10)}
			if m.SourceARN != "" {
				attrs["DeadLetterQueueSourceArn"] = m.SourceARN
			}
			reply.Attributes = map[string]string{}
			for _, k := range append(r.AttributeNames, r.MessageSystemAttributeNames...) {
				if k == "All" {
					reply.Attributes = attrs
					break
				}
				if v, ok := attrs[k]; ok {
					reply.Attributes[k] = v
				}
			}
			var values map[string]MessageAttributeValue
			json.Unmarshal([]byte(m.KeyValues["__sqs_attributes"]), &values)
			selected := map[string]MessageAttributeValue{}
			for k, v := range values {
				for _, filter := range r.MessageAttributeNames {
					if filter == "All" || filter == ".*" || k == filter || (strings.HasSuffix(filter, ".*") && strings.HasPrefix(k, strings.TrimSuffix(filter, "*"))) {
						selected[k] = v
						break
					}
				}
			}
			if len(selected) > 0 {
				reply.MessageAttributes = map[string]MessageAttribute{}
				for k, v := range selected {
					binary, _ := base64.StdEncoding.DecodeString(v.BinaryValue)
					reply.MessageAttributes[k] = MessageAttribute{DataType: v.DataType, StringValue: v.StringValue, BinaryValue: binary}
				}
				reply.MD5OfMessageAttributes = attributeMD5(selected)
			}
			result = append(result, reply)
		}
		return c.JSON(ReceiveMessageResponse{Messages: result})
	case "DeleteMessage":
		if err := b.Acknowledge(ctx, t, name, r.ReceiptHandle); err != nil {
			return err
		}
		return c.JSON(fiber.Map{})
	case "ChangeMessageVisibility":
		if r.VisibilityTimeout == nil {
			return invalid("VisibilityTimeout is required")
		}
		if err := b.Visibility(ctx, t, name, r.ReceiptHandle, *r.VisibilityTimeout); err != nil {
			return err
		}
		return c.JSON(fiber.Map{})
	case "SendMessageBatch", "DeleteMessageBatch", "ChangeMessageVisibilityBatch":
		if len(r.Entries) < 1 || len(r.Entries) > 10 {
			return NewSQSError(400, "TooManyEntriesInBatchRequest", "A batch must contain 1..10 entries")
		}
		seen := map[string]bool{}
		total := 0
		for _, entry := range r.Entries {
			if !batchID.MatchString(entry.ID) {
				return NewSQSError(400, "InvalidBatchEntryId", "Invalid batch entry ID")
			}
			if seen[entry.ID] {
				return NewSQSError(400, "BatchEntryIdsNotDistinct", "Batch entry IDs must be distinct")
			}
			seen[entry.ID] = true
			total += len(entry.MessageBody)
			for k, v := range entry.MessageAttributes {
				total += len(k) + len(v.DataType) + len(v.StringValue)
				if v.BinaryValue != "" {
					data, _ := base64.StdEncoding.DecodeString(v.BinaryValue)
					total += len(data)
				}
			}
		}
		if total > 1048576 {
			return NewSQSError(400, "BatchRequestTooLong", "Combined batch payload exceeds 1 MiB")
		}
		// Verify queue existence before mutations. Individual entry errors remain partial failures.
		if _, err := b.Attributes(ctx, t, name); err != nil {
			return err
		}
		successful := []interface{}{}
		failed := []BatchResultErrorEntry{}
		for _, entry := range r.Entries {
			var err error
			var response map[string]interface{}
			switch method {
			case "SendMessageBatch":
				response, err = s.send(ctx, b, t, name, entry)
			case "DeleteMessageBatch":
				err = b.Acknowledge(ctx, t, name, entry.ReceiptHandle)
			case "ChangeMessageVisibilityBatch":
				if entry.VisibilityTimeout == nil {
					err = invalid("VisibilityTimeout is required")
				} else {
					err = b.Visibility(ctx, t, name, entry.ReceiptHandle, *entry.VisibilityTimeout)
				}
			}
			if err != nil {
				e := asSQSError(err)
				failed = append(failed, BatchResultErrorEntry{ID: entry.ID, Code: e.Type, Message: e.Message, SenderFault: e.Code < 500})
			} else {
				if response == nil {
					response = map[string]interface{}{}
				}
				response["Id"] = entry.ID
				successful = append(successful, response)
			}
		}
		return c.JSON(fiber.Map{"Successful": successful, "Failed": failed})
	default:
		return NewSQSError(400, "UnsupportedOperation", "SQS operation is unsupported")
	}
}
func md5hex(value []byte) string { sum := md5.Sum(value); return hex.EncodeToString(sum[:]) }
func (s *SQS) send(ctx context.Context, b models.Broker, t int64, q string, e brokerEntry) (map[string]interface{}, error) {
	if e.MessageGroupId != "" || e.MessageDeduplicationId != "" {
		return nil, NewSQSError(400, "UnsupportedOperation", "FIFO fields are unsupported")
	}
	if !utf8.ValidString(e.MessageBody) || len(e.MessageBody) == 0 {
		return nil, invalid("MessageBody must be nonempty UTF-8")
	}
	for _, r := range e.MessageBody {
		if r != 9 && r != 10 && r != 13 && (r < 32 || r == 0xfffe || r == 0xffff) {
			return nil, NewSQSError(400, "InvalidMessageContents", "Message contains unsupported characters")
		}
	}
	if len(e.MessageAttributes) > 10 {
		return nil, invalid("At most ten message attributes are allowed")
	}
	size := len(e.MessageBody)
	for k, v := range e.MessageAttributes {
		if !attributeName.MatchString(k) || strings.HasPrefix(k, ".") || strings.HasSuffix(k, ".") || strings.Contains(k, "..") || strings.HasPrefix(strings.ToLower(k), "aws.") || strings.HasPrefix(strings.ToLower(k), "amazon.") {
			return nil, invalid("Invalid message attribute name")
		}
		kind := strings.Split(v.DataType, ".")[0]
		if len(v.StringListValue) > 0 || len(v.BinaryListValue) > 0 {
			return nil, invalid("List message attributes are unsupported")
		}
		if kind != "String" && kind != "Number" && kind != "Binary" {
			return nil, invalid("Invalid message attribute type")
		}
		if kind == "Binary" {
			data, err := base64.StdEncoding.DecodeString(v.BinaryValue)
			if err != nil || len(data) == 0 {
				return nil, invalid("Invalid binary attribute")
			}
			size += len(data)
		} else {
			if v.StringValue == "" || !utf8.ValidString(v.StringValue) {
				return nil, invalid("Invalid string attribute")
			}
			size += len(v.StringValue)
		}
		size += len(k) + len(v.DataType)
	}
	attrs, err := b.Attributes(ctx, t, q)
	if err != nil {
		return nil, err
	}
	max, _ := strconv.Atoi(attrs["MaximumMessageSize"])
	if size > max {
		return nil, invalid("Message and attributes exceed MaximumMessageSize")
	}
	encoded, _ := json.Marshal(e.MessageAttributes)
	id, err := b.Send(ctx, t, q, e.MessageBody, map[string]string{"__sqs_attributes": string(encoded), "__sqs_size": strconv.Itoa(size)}, e.DelaySeconds)
	if err != nil {
		return nil, err
	}
	result := map[string]interface{}{"MessageId": strconv.FormatInt(id, 10), "MD5OfMessageBody": md5hex([]byte(e.MessageBody))}
	if len(e.MessageAttributes) > 0 {
		result["MD5OfMessageAttributes"] = attributeMD5(e.MessageAttributes)
	}
	return result, nil
}
func attributeMD5(attrs map[string]MessageAttributeValue) string {
	var buffer bytes.Buffer
	write := func(value []byte) { binary.Write(&buffer, binary.BigEndian, uint32(len(value))); buffer.Write(value) }
	names := make([]string, 0, len(attrs))
	for n := range attrs {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		v := attrs[n]
		write([]byte(n))
		write([]byte(v.DataType))
		if strings.HasPrefix(v.DataType, "Binary") {
			buffer.WriteByte(2)
			data, _ := base64.StdEncoding.DecodeString(v.BinaryValue)
			write(data)
		} else {
			buffer.WriteByte(1)
			write([]byte(v.StringValue))
		}
	}
	return md5hex(buffer.Bytes())
}
func validateTags(tags map[string]string) error {
	if len(tags) > 50 {
		return invalid("At most 50 tags are allowed")
	}
	for k, v := range tags {
		if k == "" || len(k) > 128 || len(v) > 256 || strings.HasPrefix(strings.ToLower(k), "aws:") {
			return invalid("Invalid queue tag")
		}
	}
	return nil
}

// Avoid including Redis connection errors, secrets or raw request bodies in responses.
func asSQSError(err error) *SQSError {
	if e, ok := err.(*SQSError); ok {
		return e
	}
	if e, ok := err.(*models.QueueError); ok {
		status := 400
		if e.Code == "ServiceUnavailable" {
			status = 503
		}
		if e.Code == "AccessDenied" {
			status = 403
		}
		return NewSQSError(status, e.Code, e.Message)
	}
	return NewSQSError(503, "ServiceUnavailable", "Queue operation could not complete")
}

// Bound container expansion before allocating typed slices/maps. A legal large
// MessageBody is one token, so escaped payloads keep their full supported size.
func validateJSONBudget(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	depth := 0
	for count := 0; ; count++ {
		token, err := decoder.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return invalid("Malformed JSON")
		}
		if count >= 4096 {
			return invalid("Request JSON structure is too complex")
		}
		if count == 0 && token != json.Delim('{') {
			return invalid("Expected a JSON object")
		}
		if delimiter, ok := token.(json.Delim); ok {
			switch delimiter {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
			if depth > 16 {
				return invalid("Request JSON nesting is too deep")
			}
		}
	}
}
