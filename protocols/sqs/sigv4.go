package sqs

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

type AuthHeader struct {
	Algorithm, AccessKey, Date, Signature, Region, Service string
	SignedHeaders                                          []string
}

func ParseAuthorizationHeader(r *http.Request) (AuthHeader, error) {
	var out AuthHeader
	parts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(parts) != 2 || parts[0] != "AWS4-HMAC-SHA256" {
		return out, errors.New("invalid authorization algorithm")
	}
	out.Algorithm = parts[0]
	seen := map[string]bool{}
	for _, component := range strings.Split(parts[1], ",") {
		kv := strings.SplitN(strings.TrimSpace(component), "=", 2)
		if len(kv) != 2 || seen[kv[0]] {
			return out, errors.New("invalid authorization fields")
		}
		seen[kv[0]] = true
		switch kv[0] {
		case "Credential":
			scope := strings.Split(kv[1], "/")
			if len(scope) != 5 || scope[0] == "" || len(scope[1]) != 8 || scope[2] == "" || scope[3] != "sqs" || scope[4] != "aws4_request" {
				return out, errors.New("invalid credential scope")
			}
			out.AccessKey = scope[0]
			out.Date = scope[1]
			out.Region = scope[2]
			out.Service = scope[3]
		case "SignedHeaders":
			out.SignedHeaders = strings.Split(kv[1], ";")
		case "Signature":
			out.Signature = kv[1]
		default:
			return out, errors.New("unknown authorization field")
		}
	}
	if len(seen) != 3 || len(out.Signature) != 64 || len(out.SignedHeaders) == 0 {
		return out, errors.New("incomplete signature")
	}
	if _, err := hex.DecodeString(out.Signature); err != nil {
		return out, errors.New("invalid signature encoding")
	}
	required := map[string]bool{"host": false, "x-amz-date": false, "x-amz-target": false}
	previous := ""
	for _, h := range out.SignedHeaders {
		if h == "" || h != strings.ToLower(h) || h <= previous {
			return out, errors.New("invalid signed headers")
		}
		previous = h
		if _, ok := required[h]; ok {
			required[h] = true
		}
	}
	for _, present := range required {
		if !present {
			return out, errors.New("required header is unsigned")
		}
	}
	return out, nil
}
func sign(key []byte, message string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(message))
	return h.Sum(nil)
}
func getSignatureKey(key, date, region, service string) []byte {
	return sign(sign(sign(sign([]byte("AWS4"+key), date), region), service), "aws4_request")
}
func hashAndEncode(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func ValidateAWSRequest(a AuthHeader, key string, r *http.Request) error {
	timestamp, err := time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
	if err != nil || timestamp.UTC().Format("20060102") != a.Date || time.Since(timestamp) > 15*time.Minute || time.Until(timestamp) > 5*time.Minute {
		return errors.New("expired or invalid signature time")
	}
	payloadHash := sha256.New()
	if _, err := io.Copy(payloadHash, r.Body); err != nil {
		return err
	}
	defer r.Body.Close()
	var headers strings.Builder
	for _, name := range a.SignedHeaders {
		value := r.Header.Get(name)
		if name == "host" && value == "" {
			value = r.Host
		}
		if value == "" {
			return errors.New("signed header missing")
		}
		headers.WriteString(name + ":" + strings.Join(strings.Fields(value), " ") + "\n")
	}
	// Query encoding is sorted per SigV4. SQS JSON ordinarily has no query string.
	query := r.URL.Query()
	for k := range query {
		sort.Strings(query[k])
	}
	canonicalQuery := strings.ReplaceAll(query.Encode(), "+", "%20")
	uri := r.URL.EscapedPath()
	if uri == "" {
		uri = "/"
	}
	canonical := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s", r.Method, uri, canonicalQuery, headers.String(), strings.Join(a.SignedHeaders, ";"), hex.EncodeToString(payloadHash.Sum(nil)))
	scope := a.Date + "/" + a.Region + "/sqs/aws4_request"
	signed := a.Algorithm + "\n" + r.Header.Get("X-Amz-Date") + "\n" + scope + "\n" + hashAndEncode(canonical)
	expected := sign(getSignatureKey(key, a.Date, a.Region, "sqs"), signed)
	got, _ := hex.DecodeString(a.Signature)
	if !hmac.Equal(expected, got) {
		return errors.New("invalid signature")
	}
	return nil
}
