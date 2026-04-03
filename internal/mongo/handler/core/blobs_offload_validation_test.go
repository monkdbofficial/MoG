package mongo

import (
	"context"
	"strings"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

// TestOffloadBlobsInDoc_ErrorsOnOversizeInlineBinaryWhenDisabled runs the corresponding test case.
func TestOffloadBlobsInDoc_ErrorsOnOversizeInlineBinaryWhenDisabled(t *testing.T) {
	h := &Handler{
		blobTable: "", // blob offload disabled
	}

	// Base64 length exceeds Lucene term limit (32766) when payload is > ~24575 bytes.
	payload := make([]byte, 25000)
	doc := bson.M{
		"_id":     bson.NewObjectId(),
		"payload": normalizeValueForStorage(payload),
	}

	err := h.offloadBlobsInDoc(context.Background(), nil, "c", "1", doc)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "MOG_BLOB_TABLE") {
		t.Fatalf("expected error to mention MOG_BLOB_TABLE, got: %v", err)
	}
}

// TestOffloadBlobsInDoc_AllowsSmallInlineBinaryWhenDisabled runs the corresponding test case.
func TestOffloadBlobsInDoc_AllowsSmallInlineBinaryWhenDisabled(t *testing.T) {
	h := &Handler{
		blobTable: "", // blob offload disabled
	}

	payload := make([]byte, 1024)
	doc := bson.M{
		"_id":     bson.NewObjectId(),
		"payload": normalizeValueForStorage(payload),
	}

	if err := h.offloadBlobsInDoc(context.Background(), nil, "c", "1", doc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
