package sqs

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestJSONContainerBudget(t *testing.T) {
	for name, body := range map[string]string{
		"many entries": `{"Entries":[` + strings.Repeat(`{},`, 4096) + `{}]}`,
		"deep nesting": `{"Attributes":` + strings.Repeat(`[`, 17) + `0` + strings.Repeat(`]`, 17) + `}`,
	} {
		t.Run(name, func(t *testing.T) {
			if validateJSONBudget([]byte(body)) == nil {
				t.Fatal("unbounded JSON structure accepted")
			}
		})
	}
	// Unicode escaping can enlarge a valid body sixfold. It is still one scalar.
	large := []byte(`{"MessageBody":"` + strings.Repeat(`\u0061`, 1<<20) + `"}`)
	if err := validateJSONBudget(large); err != nil {
		t.Fatal(err)
	}
	var decoded brokerRequest
	if err := json.NewDecoder(bytes.NewReader(large)).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.MessageBody) != 1<<20 {
		t.Fatal("payload was truncated")
	}
}
