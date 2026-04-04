package mongo

import (
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"net/http"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

// TestNormalizeDocForReplyWithBlobs_InlinesMogBlobPointer runs the corresponding test case.
func TestNormalizeDocForReplyWithBlobs_InlinesMogBlobPointer(t *testing.T) {
	data := []byte("hello-blob")
	sha1hex := fmt.Sprintf("%x", sha1Sum(data))

	h := &Handler{
		blobHTTPBase:       "http://blob.local",
		blobInlineReads:    true,
		blobInlineMaxBytes: 1024,
		blobInlineStrict:   true,
		blobHTTPTransport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodGet {
				return &http.Response{
					StatusCode: http.StatusMethodNotAllowed,
					Body:       io.NopCloser(bytes.NewReader(nil)),
					Header:     make(http.Header),
					Request:    r,
				}, nil
			}
			if r.URL.Path != "/_blobs/media/"+sha1hex {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
					Header:     make(http.Header),
					Request:    r,
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(data)),
				Header:     make(http.Header),
				Request:    r,
			}, nil
		}),
	}

	doc := bson.M{
		"_id": bson.NewObjectId(),
		"blob": bson.M{
			"__mog_blob__": bson.M{
				"table": "media",
				"sha1":  sha1hex,
				"len":   int64(len(data)),
				"kind":  int32(0),
			},
		},
	}

	if err := h.normalizeDocForReplyWithBlobs(context.Background(), doc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, ok := doc["blob"].(bson.Binary)
	if !ok {
		t.Fatalf("expected blob to be bson.Binary, got %T", doc["blob"])
	}
	if string(got.Data) != string(data) {
		t.Fatalf("unexpected blob bytes: %#v", got.Data)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

// RoundTrip is a helper used by the adapter.
func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// sha1Sum is a helper used by the adapter.
func sha1Sum(b []byte) [20]byte {
	return sha1.Sum(b)
}
