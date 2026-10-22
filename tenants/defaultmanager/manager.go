package defaultmanager

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"
)

type KeyPolicy struct {
	AccessKey string   `json:"accessKey"`
	SecretKey string   `json:"secretKey"`
	Actions   []string `json:"actions"`
	Queues    []string `json:"queues"`
}
type DefaultTenantManager struct{ keys map[string]KeyPolicy }

func NewPolicyManager(keys []config.AWSKey, file string) (*DefaultTenantManager, error) {
	entries := []KeyPolicy{}
	if file != "" {
		raw, err := os.ReadFile(file)
		if err != nil {
			return nil, errors.New("cannot read SQS policy file")
		}
		var data struct {
			Keys []KeyPolicy `json:"keys"`
		}
		if json.Unmarshal(raw, &data) != nil {
			return nil, errors.New("invalid SQS policy JSON")
		}
		entries = data.Keys
	} else {
		for _, key := range keys {
			entries = append(entries, KeyPolicy{AccessKey: key.AccessKey, SecretKey: key.SecretKey, Actions: []string{"*"}, Queues: []string{"*"}})
		}
	}
	if len(entries) == 0 {
		return nil, errors.New("SQS credentials must be configured")
	}
	tm := &DefaultTenantManager{keys: map[string]KeyPolicy{}}
	for _, p := range entries {
		if p.AccessKey == "" || p.SecretKey == "" || len(p.Actions) == 0 || len(p.Queues) == 0 {
			return nil, errors.New("SQS policies require credentials and explicit action/queue grants")
		}
		if _, ok := tm.keys[p.AccessKey]; ok {
			return nil, errors.New("duplicate SQS access key")
		}
		for _, pattern := range append(append([]string{}, p.Actions...), p.Queues...) {
			if _, err := path.Match(pattern, ""); err != nil {
				return nil, errors.New("invalid SQS policy pattern")
			}
		}
		tm.keys[p.AccessKey] = p
	}
	return tm, nil
}
func NewDefaultTenantManager(keys []config.AWSKey) models.TenantManager {
	tm, err := NewPolicyManager(keys, "")
	if err != nil {
		panic(err)
	}
	return tm
}
func (tm *DefaultTenantManager) GetTenant(*http.Request) (int64, error) { return 1, nil }
func (tm *DefaultTenantManager) GetAWSSecretKey(accessKey, region string) (int64, string, error) {
	p, ok := tm.keys[accessKey]
	if !ok {
		return 0, "", errors.New("invalid key")
	}
	return 1, p.SecretKey, nil
}
func matches(patterns []string, value string, action bool) bool {
	for _, p := range patterns {
		if action {
			p = strings.TrimPrefix(p, "sqs:")
		}
		if ok, _ := path.Match(p, value); ok {
			return true
		}
	}
	return false
}
func (tm *DefaultTenantManager) Allowed(key, action, queue string) bool {
	p, ok := tm.keys[key]
	return ok && matches(p.Actions, action, true) && matches(p.Queues, queue, false)
}
func (tm *DefaultTenantManager) HasAction(key, action string) bool {
	p, ok := tm.keys[key]
	return ok && matches(p.Actions, action, true)
}
func (tm *DefaultTenantManager) ProductionSafe() bool {
	for _, p := range tm.keys {
		if p.AccessKey == "DEV_ACCESS_KEY_ID" || len(p.SecretKey) < 32 {
			return false
		}
	}
	return true
}
