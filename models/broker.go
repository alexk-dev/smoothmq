package models

import "context"

// Broker is the SQS contract. Administrative dashboard operations deliberately
// cannot be used as delivery acknowledgements: those require a receipt token.
type Broker interface {
	Create(context.Context, int64, string, map[string]string, map[string]string) error
	Attributes(context.Context, int64, string) (map[string]string, error)
	SetAttributes(context.Context, int64, string, map[string]string) error
	Send(context.Context, int64, string, string, map[string]string, *int) (int64, error)
	Receive(context.Context, int64, string, int, *int) ([]*Message, error)
	Acknowledge(context.Context, int64, string, string) error
	Visibility(context.Context, int64, string, string, int) error
	Purge(context.Context, int64, string) error
	Tags(context.Context, int64, string, map[string]string, []string) (map[string]string, error)
	MoveTask(context.Context, int64, string, map[string]interface{}) (map[string]interface{}, error)
	Ready(context.Context) error
}

type QueueError struct{ Code, Message string }

func (e *QueueError) Error() string { return e.Code + ": " + e.Message }

// Authorizer is separate from signature verification: authenticated keys do not
// implicitly receive administrator rights to every queue.
type Authorizer interface {
	Allowed(accessKey, action, queue string) bool
}
