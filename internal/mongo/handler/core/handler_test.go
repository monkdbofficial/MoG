package mongo

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/handler/shared"
	mwire "mog/internal/mongo/wire"
	"mog/internal/translator"
)

func opMsgRoundTrip(t *testing.T, h *Handler, cmd bson.M) bson.M {
	t.Helper()

	doc, err := bson.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal cmd: %v", err)
	}

	req := &mwire.OpMsg{
		Header: mwire.MsgHeader{
			RequestID: 1,
			OpCode:    wiremessage.OpMsg,
		},
		Sections: []mwire.Section{mwire.SectionBody{Document: doc}},
	}
	wire, err := req.Marshal()
	if err != nil {
		t.Fatalf("marshal opmsg: %v", err)
	}

	header, body, err := mwire.ReadOp(bytes.NewReader(wire))
	if err != nil {
		t.Fatalf("read op: %v", err)
	}

	respWire, err := h.Handle(context.Background(), header, body)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	respHeader, respBody, err := mwire.ReadOp(bytes.NewReader(respWire))
	if err != nil {
		t.Fatalf("read resp op: %v", err)
	}
	if respHeader.OpCode != wiremessage.OpMsg {
		t.Fatalf("expected OpMsg response, got %v", respHeader.OpCode)
	}

	resp, err := mwire.ParseOpMsg(respHeader, respBody)
	if err != nil {
		t.Fatalf("parse resp opmsg: %v", err)
	}
	if len(resp.Sections) == 0 {
		t.Fatalf("expected at least one section")
	}
	sec, ok := resp.Sections[0].(mwire.SectionBody)
	if !ok {
		t.Fatalf("expected section body")
	}

	var out bson.M
	if err := bson.Unmarshal(sec.Document, &out); err != nil {
		t.Fatalf("unmarshal resp: %v", err)
	}
	return out
}

func TestOpMsgPingOK(t *testing.T) {
	scram, _ := NewScramSha256("u", "p")
	h := NewHandler(nil, translator.New(), scram)
	out := opMsgRoundTrip(t, h, bson.M{"ping": 1, "$db": "admin"})
	if out["ok"] != 1.0 {
		t.Fatalf("expected ok=1, got %#v", out)
	}
}

func TestOpMsgUnsupportedReturnsCommandNotFound(t *testing.T) {
	scram, _ := NewScramSha256("u", "p")
	h := NewHandler(nil, translator.New(), scram)
	out := opMsgRoundTrip(t, h, bson.M{"definitelyNotACommand": 1, "$db": "admin"})
	if out["ok"] != 0.0 {
		t.Fatalf("expected ok=0, got %#v", out)
	}
	if out["codeName"] == "" {
		t.Fatalf("expected codeName set, got %#v", out)
	}
}

func TestOpMsgConnectionStatusShowPrivileges(t *testing.T) {
	scram, _ := NewScramSha256("u", "p")
	h := NewHandler(nil, translator.New(), scram)
	out := opMsgRoundTrip(t, h, bson.M{"connectionStatus": 1, "showPrivileges": true, "$db": "admin"})
	if out["ok"] != 1.0 {
		t.Fatalf("expected ok=1, got %#v", out)
	}

	authInfo, ok := out["authInfo"].(bson.M)
	if !ok {
		t.Fatalf("expected authInfo document, got %#v", out["authInfo"])
	}
	if _, ok := authInfo["authenticatedUserPrivileges"]; !ok {
		t.Fatalf("expected authenticatedUserPrivileges present, got %#v", authInfo)
	}
}

func TestOpMsgHostInfoOK(t *testing.T) {
	scram, _ := NewScramSha256("u", "p")
	h := NewHandler(nil, translator.New(), scram)
	out := opMsgRoundTrip(t, h, bson.M{"hostInfo": 1, "$db": "admin"})
	if out["ok"] != 1.0 {
		t.Fatalf("expected ok=1, got %#v", out)
	}

	sys, ok := out["system"].(bson.M)
	if !ok {
		t.Fatalf("expected system document, got %#v", out["system"])
	}
	if _, ok := sys["hostname"]; !ok {
		t.Fatalf("expected hostname, got %#v", sys)
	}
}

func TestOpMsgHelloWireVersion(t *testing.T) {
	scram, _ := NewScramSha256("u", "p")
	h := NewHandler(nil, translator.New(), scram)
	out := opMsgRoundTrip(t, h, bson.M{"hello": 1, "$db": "admin"})
	if out["ok"] != 1.0 {
		t.Fatalf("expected ok=1, got %#v", out)
	}
	var maxWV int
	switch v := out["maxWireVersion"].(type) {
	case int:
		maxWV = v
	case int32:
		maxWV = int(v)
	case int64:
		maxWV = int(v)
	default:
		t.Fatalf("unexpected maxWireVersion type: %T (%#v)", out["maxWireVersion"], out["maxWireVersion"])
	}
	if maxWV < reportedMaxWire {
		t.Fatalf("expected maxWireVersion>=%d, got %#v", reportedMaxWire, out["maxWireVersion"])
	}
}

func TestOpMsgCollStatsOK(t *testing.T) {
	scram, _ := NewScramSha256("u", "p")
	h := NewHandler(nil, translator.New(), scram)
	out := opMsgRoundTrip(t, h, bson.M{"collStats": "c", "$db": "d"})
	if out["ok"] != 1.0 {
		t.Fatalf("expected ok=1, got %#v", out)
	}
	if out["ns"] != "d.c" {
		t.Fatalf("expected ns=d.c, got %#v", out["ns"])
	}
}

func TestOpMsgListIndexesOK(t *testing.T) {
	scram, _ := NewScramSha256("u", "p")
	h := NewHandler(nil, translator.New(), scram)
	out := opMsgRoundTrip(t, h, bson.M{"listIndexes": "c", "$db": "d"})
	if out["ok"] != 1.0 {
		t.Fatalf("expected ok=1, got %#v", out)
	}
	cursor, ok := out["cursor"].(bson.M)
	if !ok {
		t.Fatalf("expected cursor doc, got %#v", out["cursor"])
	}
	if _, ok := cursor["firstBatch"]; !ok {
		t.Fatalf("expected firstBatch, got %#v", cursor)
	}
}

func TestPhysicalCollectionName(t *testing.T) {
	got, err := physicalCollectionName("test_monk", "test")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "test_monk__test" {
		t.Fatalf("expected namespaced collection, got %q", got)
	}
}

func TestCoerceBsonM(t *testing.T) {
	m, ok := shared.CoerceBsonM(map[string]interface{}{"a": 1})
	if !ok || m["a"] != 1 {
		t.Fatalf("expected map coercion, got %#v ok=%v", m, ok)
	}
	d := bson.D{{Name: "b", Value: "x"}}
	m, ok = shared.CoerceBsonM(d)
	if !ok || m["b"] != "x" {
		t.Fatalf("expected D coercion, got %#v ok=%v", m, ok)
	}
}

type recordingExec struct {
	execs []string
}

func (r *recordingExec) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return nil, errors.New("Query not implemented in recordingExec")
}

func (r *recordingExec) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return nil
}

func (r *recordingExec) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	r.execs = append(r.execs, sql)
	return pgconn.CommandTag{}, nil
}

func TestEnsureCollectionTableExec_UpgradesDataColumnWhenCacheInitialized(t *testing.T) {
	scram, _ := NewScramSha256("u", "p")
	h := NewHandler(nil, translator.New(), scram)
	h.storeRawMongoJSON = true

	physical := "test_upgrade_data_col"
	defer h.schemaCache().clear(physical)
	h.schemaCache().markInitialized(physical)

	exec := &recordingExec{}
	if err := h.ensureCollectionTableExec(context.Background(), exec, physical); err != nil {
		t.Fatalf("ensureCollectionTableExec: %v", err)
	}

	if len(exec.execs) != 1 {
		t.Fatalf("expected 1 exec call, got %d (%#v)", len(exec.execs), exec.execs)
	}
	want := "ALTER TABLE doc." + physical + " ADD COLUMN data OBJECT(DYNAMIC)"
	if exec.execs[0] != want {
		t.Fatalf("expected %q, got %q", want, exec.execs[0])
	}
	if !h.schemaCache().hasColumn(physical, "data") {
		t.Fatalf("expected schema cache to include data column")
	}
}
