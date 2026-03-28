package mongo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

// MsgHeader represents the standard MongoDB message header.
type MsgHeader struct {
	Size       int32
	RequestID  int32
	ResponseTo int32
	OpCode     wiremessage.OpCode
}

// OpMsg represents an OP_MSG message (opcode 2013).
type OpMsg struct {
	Header   MsgHeader
	FlagBits uint32
	Sections []Section
}

type OpQuery struct {
	Header             MsgHeader
	Flags              int32
	FullCollectionName string
	NumberToSkip       int32
	NumberToReturn     int32
	Query              []byte
}

type OpReply struct {
	Header         MsgHeader
	ResponseFlags  int32
	CursorID       int64
	StartingFrom   int32
	NumberReturned int32
	Documents      [][]byte
}

// Section represents a section in an OP_MSG.
type Section interface {
	Kind() byte
}

// SectionBody represents a Kind 0 section (BSON Body).
type SectionBody struct {
	Document []byte
}

func (s SectionBody) Kind() byte { return 0 }

// SectionDocumentSequence represents a Kind 1 section (Document Sequence).
// It is used by drivers to send arrays (e.g. "documents", "updates") outside the command body.
type SectionDocumentSequence struct {
	Identifier string
	Documents  [][]byte
}

func (s SectionDocumentSequence) Kind() byte { return 1 }

// ReadOp reads a single wire operation from the reader.
func ReadOp(r io.Reader) (MsgHeader, []byte, error) {
	var header [16]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return MsgHeader{}, nil, fmt.Errorf("failed to read header: %w", err)
	}

	h := MsgHeader{
		Size:       int32(binary.LittleEndian.Uint32(header[0:4])),
		RequestID:  int32(binary.LittleEndian.Uint32(header[4:8])),
		ResponseTo: int32(binary.LittleEndian.Uint32(header[8:12])),
		OpCode:     wiremessage.OpCode(binary.LittleEndian.Uint32(header[12:16])),
	}

	if h.Size < 16 {
		return h, nil, fmt.Errorf("invalid message size: %d", h.Size)
	}

	body := make([]byte, h.Size-16)
	if _, err := io.ReadFull(r, body); err != nil {
		return h, nil, fmt.Errorf("failed to read body: %w", err)
	}

	return h, body, nil
}

// ParseOpMsg parses the body of an OP_MSG.
func ParseOpMsg(header MsgHeader, body []byte) (*OpMsg, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("OpMsg body too short")
	}

	flagBits := binary.LittleEndian.Uint32(body[0:4])
	sections := []Section{}
	offset := 4

	for offset < len(body) {
		kind := body[offset]
		offset++

		switch kind {
		case 0:
			if offset+4 > len(body) {
				return nil, fmt.Errorf("OpMsg section body too short")
			}
			docSize := int(binary.LittleEndian.Uint32(body[offset : offset+4]))
			if offset+docSize > len(body) {
				return nil, fmt.Errorf("OpMsg section body doc size too large")
			}
			doc := body[offset : offset+docSize]
			sections = append(sections, SectionBody{Document: doc})
			offset += docSize
		case 1:
			// Kind 1: int32 size (includes size itself), cstring identifier, then one or more BSON docs.
			if offset+4 > len(body) {
				return nil, fmt.Errorf("OpMsg section sequence too short")
			}
			sectionSize := int(binary.LittleEndian.Uint32(body[offset : offset+4]))
			if sectionSize < 5 || offset+sectionSize > len(body) {
				return nil, fmt.Errorf("OpMsg section sequence size invalid")
			}
			section := body[offset : offset+sectionSize]
			offset += sectionSize

			if len(section) < 5 {
				return nil, fmt.Errorf("OpMsg section sequence malformed")
			}

			// identifier starts at byte 4 (after size) and is null-terminated
			idStart := 4
			idEnd := bytes.IndexByte(section[idStart:], 0x00)
			if idEnd < 0 {
				return nil, fmt.Errorf("OpMsg section sequence missing identifier terminator")
			}
			identifier := string(section[idStart : idStart+idEnd])
			pos := idStart + idEnd + 1

			var docs [][]byte
			for pos < len(section) {
				if pos+4 > len(section) {
					return nil, fmt.Errorf("OpMsg section sequence trailing bytes")
				}
				docSize := int(binary.LittleEndian.Uint32(section[pos : pos+4]))
				if docSize < 5 || pos+docSize > len(section) {
					return nil, fmt.Errorf("OpMsg section sequence doc size invalid")
				}
				doc := section[pos : pos+docSize]
				docs = append(docs, doc)
				pos += docSize
			}
			sections = append(sections, SectionDocumentSequence{Identifier: identifier, Documents: docs})
		default:
			// For now, we skip other section kinds or return error
			return nil, fmt.Errorf("unsupported OpMsg section kind: %d", kind)
		}

		// Check for optional checksum (if flagBits has bit 0 set)
		// For simplicity, we assume no checksum or ignore it if at the end
		if offset+4 == len(body) && (flagBits&1 != 0) {
			break
		}
	}

	return &OpMsg{
		Header:   header,
		FlagBits: flagBits,
		Sections: sections,
	}, nil
}

func ParseOpQuery(header MsgHeader, body []byte) (*OpQuery, error) {
	if len(body) < 12 {
		return nil, fmt.Errorf("OpQuery body too short")
	}

	flags := int32(binary.LittleEndian.Uint32(body[0:4]))
	offset := 4

	nameEnd := bytes.IndexByte(body[offset:], 0x00)
	if nameEnd < 0 {
		return nil, fmt.Errorf("OpQuery missing collection name terminator")
	}
	fullCollectionName := string(body[offset : offset+nameEnd])
	offset += nameEnd + 1

	if offset+8 > len(body) {
		return nil, fmt.Errorf("OpQuery body missing skip/return fields")
	}

	numberToSkip := int32(binary.LittleEndian.Uint32(body[offset : offset+4]))
	offset += 4
	numberToReturn := int32(binary.LittleEndian.Uint32(body[offset : offset+4]))
	offset += 4

	if offset+4 > len(body) {
		return nil, fmt.Errorf("OpQuery missing query document")
	}

	querySize := int(binary.LittleEndian.Uint32(body[offset : offset+4]))
	if querySize < 5 || offset+querySize > len(body) {
		return nil, fmt.Errorf("OpQuery invalid query document size")
	}
	query := body[offset : offset+querySize]

	return &OpQuery{
		Header:             header,
		Flags:              flags,
		FullCollectionName: fullCollectionName,
		NumberToSkip:       numberToSkip,
		NumberToReturn:     numberToReturn,
		Query:              query,
	}, nil
}

// Marshal marshals an OpMsg into bytes.
func (op *OpMsg) Marshal() ([]byte, error) {
	var body []byte
	// FlagBits (4 bytes)
	flagBitsBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(flagBitsBytes, op.FlagBits)
	body = append(body, flagBitsBytes...)

	// Sections
	for _, section := range op.Sections {
		body = append(body, section.Kind())
		switch s := section.(type) {
		case SectionBody:
			body = append(body, s.Document...)
		case SectionDocumentSequence:
			// section bytes: int32 size, cstring identifier, then docs
			var sectionBytes []byte
			sectionBytes = append(sectionBytes, 0, 0, 0, 0) // placeholder for size
			sectionBytes = append(sectionBytes, []byte(s.Identifier)...)
			sectionBytes = append(sectionBytes, 0x00)
			for _, doc := range s.Documents {
				sectionBytes = append(sectionBytes, doc...)
			}
			binary.LittleEndian.PutUint32(sectionBytes[0:4], uint32(len(sectionBytes)))
			body = append(body, sectionBytes...)
		}
	}

	totalSize := 16 + len(body)
	header := make([]byte, 16)
	binary.LittleEndian.PutUint32(header[0:4], uint32(totalSize))
	binary.LittleEndian.PutUint32(header[4:8], uint32(op.Header.RequestID))
	binary.LittleEndian.PutUint32(header[8:12], uint32(op.Header.ResponseTo))
	binary.LittleEndian.PutUint32(header[12:16], uint32(op.Header.OpCode))

	return append(header, body...), nil
}

func (op *OpReply) Marshal() ([]byte, error) {
	var body []byte

	responseFlags := make([]byte, 4)
	binary.LittleEndian.PutUint32(responseFlags, uint32(op.ResponseFlags))
	body = append(body, responseFlags...)

	cursorID := make([]byte, 8)
	binary.LittleEndian.PutUint64(cursorID, uint64(op.CursorID))
	body = append(body, cursorID...)

	startingFrom := make([]byte, 4)
	binary.LittleEndian.PutUint32(startingFrom, uint32(op.StartingFrom))
	body = append(body, startingFrom...)

	numberReturned := make([]byte, 4)
	binary.LittleEndian.PutUint32(numberReturned, uint32(op.NumberReturned))
	body = append(body, numberReturned...)

	for _, doc := range op.Documents {
		body = append(body, doc...)
	}

	totalSize := 16 + len(body)
	header := make([]byte, 16)
	binary.LittleEndian.PutUint32(header[0:4], uint32(totalSize))
	binary.LittleEndian.PutUint32(header[4:8], uint32(op.Header.RequestID))
	binary.LittleEndian.PutUint32(header[8:12], uint32(op.Header.ResponseTo))
	binary.LittleEndian.PutUint32(header[12:16], uint32(wiremessage.OpReply))

	return append(header, body...), nil
}
