package mongo

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/handler/shared"
	mwire "mog/internal/mongo/wire"
)

// newMsg builds an OP_MSG reply for a command response document.
//
// Reply builders and common admin/handshake documents live in this file.
func (h *Handler) newMsg(requestID int32, doc bson.M) ([]byte, error) {
	bytes, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}

	op := &mwire.OpMsg{
		Header: mwire.MsgHeader{
			RequestID:  0, // We could increment this
			ResponseTo: requestID,
			OpCode:     wiremessage.OpMsg,
		},
		FlagBits: 0,
		Sections: []mwire.Section{mwire.SectionBody{Document: bytes}},
	}

	return op.Marshal()
}

// newMsgError is a helper used by the adapter.
func (h *Handler) newMsgError(requestID int32, code int32, codeName, errmsg string) ([]byte, error) {
	return h.newMsg(requestID, bson.M{
		"ok":       0.0,
		"errmsg":   errmsg,
		"code":     code,
		"codeName": codeName,
	})
}

// newReply is a helper used by the adapter.
func (h *Handler) newReply(requestID int32, doc bson.M) ([]byte, error) {
	bytes, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}

	op := &mwire.OpReply{
		Header: mwire.MsgHeader{
			RequestID:  0,
			ResponseTo: requestID,
			OpCode:     wiremessage.OpReply,
		},
		ResponseFlags:  0,
		CursorID:       0,
		StartingFrom:   0,
		NumberReturned: 1,
		Documents:      [][]byte{bytes},
	}

	return op.Marshal()
}

// newReplyError is a helper used by the adapter.
func (h *Handler) newReplyError(requestID int32, code int32, codeName, errmsg string) ([]byte, error) {
	return h.newReply(requestID, bson.M{
		"ok":       0.0,
		"errmsg":   errmsg,
		"code":     code,
		"codeName": codeName,
	})
}

// hostInfoDoc is a helper used by the adapter.
func hostInfoDoc() bson.M {
	hostname, _ := os.Hostname()
	return bson.M{
		"system": bson.M{
			"hostname": hostname,
			"cpuArch":  runtime.GOARCH,
			"os": bson.M{
				"type": runtime.GOOS,
				"name": runtime.GOOS,
			},
		},
		"ok": 1.0,
	}
}

// helloDoc is a helper used by the adapter.
func helloDoc(authenticated bool) bson.M {
	doc := bson.M{
		"ismaster":                     true,
		"isWritablePrimary":            true,
		"helloOk":                      true,
		"maxBsonObjectSize":            16777216,
		"maxMessageSizeBytes":          48000000,
		"maxWriteBatchSize":            100000,
		"localTime":                    time.Now(),
		"logicalSessionTimeoutMinutes": 30,
		"minWireVersion":               0,
		"maxWireVersion":               reportedMaxWire,
		"readOnly":                     false,
		"ok":                           1.0,
	}
	if !authenticated {
		doc["saslSupportedMechs"] = []string{"SCRAM-SHA-256"}
	}
	return doc
}

// maybeSpeculativeAuth is a helper used by the adapter.
func (h *Handler) maybeSpeculativeAuth(cmd bson.M, resp bson.M) error {
	if h.authenticated || h.scramConv != nil {
		return nil
	}
	raw, ok := cmd["speculativeAuthenticate"]
	if !ok || raw == nil {
		return nil
	}
	spec, ok := shared.CoerceBsonM(raw)
	if !ok {
		return nil
	}
	if _, ok := spec["saslStart"]; !ok {
		return nil
	}
	// Only SCRAM-SHA-256 is supported currently.
	mech, _ := spec["mechanism"].(string)
	if mech != "" && mech != "SCRAM-SHA-256" {
		return nil
	}
	payload, err := payloadFromCommand(spec)
	if err != nil {
		return nil
	}

	h.scramConv = h.scram.Start()
	serverFirst, err := h.scramConv.Step(payload)
	if err != nil {
		// If speculative auth fails, don't fail hello; client will retry with normal saslStart.
		h.scramConv = nil
		return nil
	}
	resp["speculativeAuthenticate"] = bson.M{
		"conversationId": 1,
		"done":           false,
		"payload":        []byte(serverFirst),
		"ok":             1.0,
	}
	return nil
}

// serverStatusDoc is a helper used by the adapter.
func serverStatusDoc() bson.M {
	hostname, _ := os.Hostname()
	now := time.Now()
	return bson.M{
		"host":           hostname,
		"version":        reportedMongoVersion,
		"process":        "mongod",
		"pid":            int64(0),
		"uptime":         float64(0),
		"uptimeMillis":   int64(0),
		"uptimeEstimate": float64(0),
		"localTime":      now,
		"connections": bson.M{
			"current":      int32(0),
			"available":    int32(1000),
			"totalCreated": int64(0),
		},
		"ok": 1.0,
	}
}

// cmdLineOptsDoc is a helper used by the adapter.
func cmdLineOptsDoc() bson.M {
	return bson.M{
		"argv":   []string{},
		"parsed": bson.M{},
		"ok":     1.0,
	}
}

// payloadFromCommand is a helper used by the adapter.
func payloadFromCommand(cmd bson.M) (string, error) {
	payload, ok := cmd["payload"]
	if !ok {
		return "", fmt.Errorf("missing SCRAM payload")
	}

	switch p := payload.(type) {
	case string:
		return p, nil
	case []byte:
		return string(p), nil
	case bson.Binary:
		return string(p.Data), nil
	default:
		return "", fmt.Errorf("unsupported SCRAM payload type: %T", payload)
	}
}
