package mongo

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"
)

// TestNormalizeRowValue_JSONBytesArray runs the corresponding test case.
func TestNormalizeRowValue_JSONBytesArray(t *testing.T) {
	got := normalizeRowValue([]byte(`["a","b"]`))
	arr, ok := got.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T (%v)", got, got)
	}
	if !reflect.DeepEqual(arr, []interface{}{"a", "b"}) {
		t.Fatalf("unexpected array: %#v", arr)
	}
}

// TestNormalizeRowValue_JSONBytesObject runs the corresponding test case.
func TestNormalizeRowValue_JSONBytesObject(t *testing.T) {
	got := normalizeRowValue([]byte(`{"a":1}`))
	m, ok := got.(bson.M)
	if !ok {
		t.Fatalf("expected bson.M, got %T (%v)", got, got)
	}
	if m["a"] != float64(1) {
		t.Fatalf("unexpected object: %#v", m)
	}
}

// TestNormalizeDocForReply_RehydratesObjectIDsAndTimestamps runs the corresponding test case.
func TestNormalizeDocForReply_RehydratesObjectIDsAndTimestamps(t *testing.T) {
	doc := bson.M{
		"_id":      "69ca02e4a989bbdeccf98cff",
		"event_id": "69ca02e4a989bbdeccf98cf4",
		"ts":       "2026-03-30T04:58:12.944000Z",
		"events": []interface{}{
			bson.M{
				"event_id": "69ca02e4a989bbdeccf98cf5",
				"at":       "2026-03-27T04:21:12.940000Z",
				"tags":     []interface{}{"payload", "thermal", "nav"},
			},
		},
	}

	normalizeDocForReply(doc)

	if _, ok := doc["_id"].(bson.ObjectId); !ok {
		t.Fatalf("expected _id to be bson.ObjectId, got %T (%v)", doc["_id"], doc["_id"])
	}
	if _, ok := doc["event_id"].(bson.ObjectId); !ok {
		t.Fatalf("expected event_id to be bson.ObjectId, got %T (%v)", doc["event_id"], doc["event_id"])
	}
	if _, ok := doc["ts"].(time.Time); !ok {
		t.Fatalf("expected ts to be time.Time, got %T (%v)", doc["ts"], doc["ts"])
	}

	events, ok := doc["events"].([]interface{})
	if !ok || len(events) != 1 {
		t.Fatalf("expected events array, got %T (%v)", doc["events"], doc["events"])
	}
	ev, ok := events[0].(bson.M)
	if !ok {
		t.Fatalf("expected event to be bson.M, got %T (%v)", events[0], events[0])
	}
	if _, ok := ev["event_id"].(bson.ObjectId); !ok {
		t.Fatalf("expected nested event_id to be bson.ObjectId, got %T (%v)", ev["event_id"], ev["event_id"])
	}
	if _, ok := ev["at"].(time.Time); !ok {
		t.Fatalf("expected nested at to be time.Time, got %T (%v)", ev["at"], ev["at"])
	}
}

// TestNormalizeDocForReply_ExtJSONNumberLongInBsonM runs the corresponding test case.
func TestNormalizeDocForReply_ExtJSONNumberLongInBsonM(t *testing.T) {
	doc := bson.M{
		"_id": bson.NewObjectId(),
		"age": bson.M{"$numberLong": "25"},
	}
	normalizeDocForReply(doc)
	if got, ok := doc["age"].(int64); !ok || got != int64(25) {
		t.Fatalf("expected int64(25), got %T (%v)", doc["age"], doc["age"])
	}
}

// TestNormalizeDocForStorage_ExtJSONNumberLongInBsonM runs the corresponding test case.
func TestNormalizeDocForStorage_ExtJSONNumberLongInBsonM(t *testing.T) {
	doc := bson.M{
		"_id":           bson.NewObjectId(),
		"mongo_user_id": bson.M{"$numberLong": "1"},
	}
	normalizeDocForStorage(doc)
	if got, ok := doc["mongo_user_id"].(int64); !ok || got != int64(1) {
		t.Fatalf("expected int64(1), got %T (%v)", doc["mongo_user_id"], doc["mongo_user_id"])
	}
}

// TestNormalizeDocForStorage_ExtJSONNumberLongNumericValue runs the corresponding test case.
func TestNormalizeDocForStorage_ExtJSONNumberLongNumericValue(t *testing.T) {
	doc := bson.M{
		"_id":           bson.NewObjectId(),
		"mongo_user_id": bson.M{"$numberLong": float64(1)},
	}
	normalizeDocForStorage(doc)
	if got, ok := doc["mongo_user_id"].(int64); !ok || got != int64(1) {
		t.Fatalf("expected int64(1), got %T (%v)", doc["mongo_user_id"], doc["mongo_user_id"])
	}
}

// TestNormalizeDocForStorageAndReply_EscapesDottedAndDollarKeys runs the corresponding test case.
func TestNormalizeDocForStorageAndReply_EscapesDottedAndDollarKeys(t *testing.T) {
	doc := bson.M{
		"_id": bson.NewObjectId(),
		"weird_keys": bson.M{
			"dotted.key":  "x",
			"dollar$sign": "y",
		},
	}

	normalizeDocForStorage(doc)
	if _, ok := doc["weird_keys"].(bson.M); !ok {
		t.Fatalf("expected weird_keys to remain an object, got %T", doc["weird_keys"])
	}
	// No '.' keys should remain after storage normalization.
	wk := doc["weird_keys"].(bson.M)
	for k := range wk {
		if strings.Contains(k, ".") || strings.Contains(k, "$") {
			t.Fatalf("expected key to be escaped for storage, got %q", k)
		}
	}

	normalizeDocForReply(doc)
	wk2, ok := doc["weird_keys"].(bson.M)
	if !ok {
		t.Fatalf("expected weird_keys to be bson.M, got %T", doc["weird_keys"])
	}
	if wk2["dotted.key"] != "x" {
		t.Fatalf("expected dotted.key to roundtrip, got %#v", wk2)
	}
	if wk2["dollar$sign"] != "y" {
		t.Fatalf("expected dollar$sign to roundtrip, got %#v", wk2)
	}
}

// TestNormalize_MixedArrayIsWrappedForStorageAndUnwrappedForReply runs the corresponding test case.
func TestNormalize_MixedArrayIsWrappedForStorageAndUnwrappedForReply(t *testing.T) {
	doc := bson.M{
		"_id": bson.NewObjectId(),
		"arrays": bson.M{
			"mixed": []interface{}{true, 3.14, "x", bson.M{"nested": 1}, []interface{}{1, 2}},
		},
	}

	normalizeDocForStorage(doc)
	arrs, ok := doc["arrays"].(bson.M)
	if !ok {
		t.Fatalf("expected arrays to be bson.M, got %T", doc["arrays"])
	}
	mixed, ok := arrs["mixed"].([]interface{})
	if !ok || len(mixed) == 0 {
		t.Fatalf("expected mixed to be []interface{}, got %T (%v)", arrs["mixed"], arrs["mixed"])
	}
	if _, ok := mixed[0].(bson.M); !ok {
		t.Fatalf("expected storage-wrapped array elements, got %T", mixed[0])
	}

	normalizeDocForReply(doc)
	arrs2, ok := doc["arrays"].(bson.M)
	if !ok {
		t.Fatalf("expected arrays to be bson.M after reply normalize, got %T", doc["arrays"])
	}
	mixed2, ok := arrs2["mixed"].([]interface{})
	if !ok || len(mixed2) != 5 {
		t.Fatalf("expected mixed to be unwrapped back to 5 elements, got %T (%v)", arrs2["mixed"], arrs2["mixed"])
	}
	if _, ok := mixed2[0].(bool); !ok {
		t.Fatalf("expected first element bool after unwrap, got %T (%v)", mixed2[0], mixed2[0])
	}
}

// TestNormalizeDocForStorage_UnsupportedBsonTypesAreMadeSQLSafe runs the corresponding test case.
func TestNormalizeDocForStorage_UnsupportedBsonTypesAreMadeSQLSafe(t *testing.T) {
	doc := bson.M{
		"_id": bson.NewObjectId(),
		"raw": bson.M{
			"bin":   bson.Binary{Kind: 0x80, Data: []byte{0x00, 0x01, 0x02}},
			"bytes": []byte{0x01, 0x02, 0x03},
			"code":  bson.JavaScript{Code: "function(x){return x+1;}", Scope: bson.M{"a": 1}},
			"ptr":   bson.DBPointer{Namespace: "missions", Id: bson.NewObjectId()},
			"regex": bson.RegEx{Pattern: "^MOG_", Options: "i"},
			"min":   bson.MinKey,
			"max":   bson.MaxKey,
			"raw":   bson.Raw{Kind: 0x13, Data: make([]byte, 16)},
		},
	}

	normalizeDocForStorage(doc)
	raw, ok := doc["raw"].(bson.M)
	if !ok {
		t.Fatalf("expected raw to be bson.M, got %T", doc["raw"])
	}
	// These should no longer contain bson.Binary/bson.JavaScript/bson.DBPointer/etc.
	for k, v := range raw {
		switch v.(type) {
		case bson.Binary, bson.JavaScript, bson.DBPointer, bson.RegEx, bson.Raw:
			t.Fatalf("expected %s to be normalized to JSON-safe types, got %T", k, v)
		}
	}
}

// TestNormalizeDocForStorage_BytesRoundTripAsBinary runs the corresponding test case.
func TestNormalizeDocForStorage_BytesRoundTripAsBinary(t *testing.T) {
	doc := bson.M{
		"_id":  bson.NewObjectId(),
		"data": []byte{0x00, 0x01, 0x02, 0x03},
	}

	normalizeDocForStorage(doc)
	if _, ok := doc["data"].(bson.M); !ok {
		t.Fatalf("expected data to be wrapped as bson.M, got %T", doc["data"])
	}

	normalizeDocForReply(doc)
	bin, ok := doc["data"].(bson.Binary)
	if !ok {
		t.Fatalf("expected data to be bson.Binary after reply normalize, got %T", doc["data"])
	}
	if bin.Kind != 0 {
		t.Fatalf("expected kind=0, got %d", bin.Kind)
	}
	if len(bin.Data) != 4 || bin.Data[0] != 0x00 || bin.Data[3] != 0x03 {
		t.Fatalf("unexpected data bytes: %#v", bin.Data)
	}
}
