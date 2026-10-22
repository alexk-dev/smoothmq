package sqs

import (
	"net/http"
	"testing"
)

func FuzzParseAuthorizationHeader(f *testing.F) {
	for _, input := range []string{"", "AWS4-HMAC-SHA256", "AWS4-HMAC-SHA256 Credential=x", "AWS4-HMAC-SHA256 Credential=x/a/b/c,Signature=x", "anything a=b", "AWS4-HMAC-SHA256 SignedHeaders=host;host,Signature=00"} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, value string) {
		r, _ := http.NewRequest("POST", "http://localhost/", nil)
		r.Header.Set("Authorization", value)
		ParseAuthorizationHeader(r)
	})
}
