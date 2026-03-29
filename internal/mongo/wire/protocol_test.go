package wire

import (
	"bytes"
	"testing"

	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
	"gopkg.in/mgo.v2/bson"
)

func TestOpMsgDocumentSequenceRoundTrip(t *testing.T) {
	bodyDoc, err := bson.Marshal(bson.M{"insert": "c", "$db": "d"})
	if err != nil {
		t.Fatalf("marshal body doc: %v", err)
	}
	doc1, err := bson.Marshal(bson.M{"a": 1})
	if err != nil {
		t.Fatalf("marshal seq doc: %v", err)
	}

	req := &OpMsg{
		Header: MsgHeader{RequestID: 1, OpCode: wiremessage.OpMsg},
		Sections: []Section{
			SectionBody{Document: bodyDoc},
			SectionDocumentSequence{Identifier: "documents", Documents: [][]byte{doc1}},
		},
	}

	wire, err := req.Marshal()
	if err != nil {
		t.Fatalf("marshal opmsg: %v", err)
	}

	header, body, err := ReadOp(bytes.NewReader(wire))
	if err != nil {
		t.Fatalf("read op: %v", err)
	}

	parsed, err := ParseOpMsg(header, body)
	if err != nil {
		t.Fatalf("parse opmsg: %v", err)
	}
	if len(parsed.Sections) != 2 {
		t.Fatalf("expected 2 sections, got %d", len(parsed.Sections))
	}

	seq, ok := parsed.Sections[1].(SectionDocumentSequence)
	if !ok {
		t.Fatalf("expected section 1 to be document sequence, got %T", parsed.Sections[1])
	}
	if seq.Identifier != "documents" {
		t.Fatalf("expected identifier=documents, got %q", seq.Identifier)
	}
	if len(seq.Documents) != 1 {
		t.Fatalf("expected 1 document, got %d", len(seq.Documents))
	}
	var m bson.M
	if err := bson.Unmarshal(seq.Documents[0], &m); err != nil {
		t.Fatalf("unmarshal doc: %v", err)
	}
	if m["a"] != 1 {
		t.Fatalf("expected a=1, got %#v", m)
	}
}
