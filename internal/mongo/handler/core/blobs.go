package mongo

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/logging"
)

// MonkDB/CrateDB string indexing is backed by Lucene, which enforces a maximum
// term byte length (see Lucene's IndexWriter.MAX_TERM_LENGTH).
// A base64-encoded blob has no natural token boundaries, so analyzers often emit
// the full value as a single term, tripping the limit.
const luceneMaxTermBytes = 32766

func (h *Handler) blobHTTPClient() *http.Client {
	if h != nil && h.blobHTTPTransport != nil {
		return &http.Client{Timeout: h.httpTimeout(), Transport: h.blobHTTPTransport}
	}
	return &http.Client{Timeout: h.httpTimeout()}
}

func validateNoOversizeInlineBinary(v interface{}) error {
	var walk func(v interface{}) error
	walk = func(v interface{}) error {
		switch t := v.(type) {
		case bson.M:
			if b64, ok := t[mogBinKey].(string); ok && b64 != "" && len(b64) > luceneMaxTermBytes {
				return fmt.Errorf(
					"inline BSON binary is too large to store without blob offload: base64 value is %d bytes (Lucene term limit %d). Set MOG_BLOB_TABLE (and MOG_BLOB_HTTP_BASE) to enable offload, or reduce payload size",
					len(b64),
					luceneMaxTermBytes,
				)
			}
			for _, vv := range t {
				if err := walk(vv); err != nil {
					return err
				}
			}
			return nil
		case map[string]interface{}:
			return walk(bson.M(t))
		case []interface{}:
			for _, el := range t {
				if err := walk(el); err != nil {
					return err
				}
			}
			return nil
		default:
			return nil
		}
	}
	return walk(v)
}

func (h *Handler) ensureBlobInfra(ctx context.Context) error {
	if !h.blobEnabled() {
		return nil
	}
	h.blobOnce.Do(func() {
		if h.pool == nil {
			h.blobInitErr = fmt.Errorf("database pool is not configured")
			return
		}
		// Best-effort create blob table (DDL succeeds even if metadata is disabled).
		if h.blobShards <= 0 {
			h.blobShards = 3
		}
		ddl := fmt.Sprintf("CREATE BLOB TABLE %s CLUSTERED INTO %d SHARDS", h.blobTable, h.blobShards)
		if _, err := h.pool.Exec(ctx, ddl); err != nil {
			// Ignore "already exists" style errors.
			if !(isDuplicateObject(err) || strings.Contains(strings.ToLower(err.Error()), "already exists")) {
				h.blobInitErr = err
				return
			}
		}

		if h.blobMetaEnable {
			if err := h.ensureDocSchema(ctx); err != nil {
				h.blobInitErr = err
				return
			}
			metaTable := strings.TrimSpace(h.blobMetaTable)
			if metaTable == "" {
				metaTable = "doc.blob_metadata"
			}
			// Use MonkDB-compatible types.
			metaDDL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  file_id TEXT PRIMARY KEY,
  blob_sha1 TEXT,
  logical_path TEXT,
  content_type TEXT,
  uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
)`, metaTable)
			if _, err := h.pool.Exec(ctx, metaDDL); err != nil {
				h.blobInitErr = err
				return
			}
		}
	})
	return h.blobInitErr
}

func (h *Handler) offloadBlobsInDoc(ctx context.Context, exec DBExecutor, physical string, docID string, doc bson.M) error {
	if doc == nil {
		return nil
	}
	if !h.blobEnabled() {
		// Without blob offload, large base64-wrapped binaries can trigger backend "immense term"
		// errors due to Lucene term size limits. Fail early with a clearer message.
		return validateNoOversizeInlineBinary(doc)
	}
	if err := h.ensureBlobInfra(ctx); err != nil {
		return err
	}

	seen := map[string]struct{}{} // sha1hex -> uploaded (per doc)
	var walk func(path string, v interface{}) (interface{}, error)

	walk = func(path string, v interface{}) (interface{}, error) {
		switch t := v.(type) {
		case bson.M:
			// Detect binary wrapper produced by normalizeValueForStorage.
			if b64, ok := t[mogBinKey].(string); ok && b64 != "" {
				forceOffload := len(b64) > luceneMaxTermBytes
				kindV := t[mogBinKindKey]
				kind := int32(0)
				switch kk := kindV.(type) {
				case int32:
					kind = kk
				case int64:
					kind = int32(kk)
				case int:
					kind = int32(kk)
				case float64:
					kind = int32(kk)
				}
				data, err := base64.StdEncoding.DecodeString(b64)
				if err != nil {
					return v, nil
				}
				if !forceOffload && h.blobMinBytes > 0 && len(data) < h.blobMinBytes {
					return v, nil
				}

				sum := sha1.Sum(data)
				sha1hex := hex.EncodeToString(sum[:])

				if _, ok := seen[sha1hex]; !ok {
					if err := h.putBlob(ctx, h.blobTable, sha1hex, data); err != nil {
						return nil, err
					}
					seen[sha1hex] = struct{}{}

					if h.blobMetaEnable && exec != nil {
						_ = h.insertBlobMetadataBestEffort(ctx, exec, sha1hex, path, "", physical, docID)
					}
				}

				return bson.M{
					"__mog_blob__": bson.M{
						"table": h.blobTable,
						"sha1":  sha1hex,
						"len":   int64(len(data)),
						"kind":  kind,
					},
				}, nil
			}

			out := bson.M{}
			for k, vv := range t {
				if k == "" {
					continue
				}
				nv, err := walk(joinPath(path, k), vv)
				if err != nil {
					return nil, err
				}
				out[k] = nv
			}
			return out, nil
		case map[string]interface{}:
			m := bson.M(t)
			return walk(path, m)
		case []interface{}:
			out := make([]interface{}, 0, len(t))
			for i, el := range t {
				nv, err := walk(fmt.Sprintf("%s[%d]", path, i), el)
				if err != nil {
					return nil, err
				}
				out = append(out, nv)
			}
			return out, nil
		default:
			return v, nil
		}
	}

	for k, v := range doc {
		if k == "" {
			continue
		}
		nv, err := walk(k, v)
		if err != nil {
			return err
		}
		doc[k] = nv
	}
	return nil
}

func (h *Handler) putBlob(ctx context.Context, table string, sha1hex string, data []byte) error {
	if strings.TrimSpace(table) == "" || sha1hex == "" {
		return fmt.Errorf("invalid blob target")
	}
	base := strings.TrimRight(strings.TrimSpace(h.blobHTTPBase), "/")
	if base == "" {
		base = "http://localhost:6000"
	}
	url := base + "/_blobs/" + table + "/" + sha1hex

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	client := h.blobHTTPClient()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	// MonkDB typically returns 201 on create, 200/409 may happen depending on existence semantics.
	if resp.StatusCode == 200 || resp.StatusCode == 201 || resp.StatusCode == 409 {
		return nil
	}
	return fmt.Errorf("blob upload failed: %s", resp.Status)
}

func (h *Handler) getBlob(ctx context.Context, table string, sha1hex string, maxBytes int) ([]byte, error) {
	if strings.TrimSpace(table) == "" || sha1hex == "" {
		return nil, fmt.Errorf("invalid blob source")
	}
	base := strings.TrimRight(strings.TrimSpace(h.blobHTTPBase), "/")
	if base == "" {
		base = "http://localhost:6000"
	}
	url := base + "/_blobs/" + table + "/" + sha1hex

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	client := h.blobHTTPClient()
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("blob download failed: %s", resp.Status)
	}

	var r io.Reader = resp.Body
	if maxBytes > 0 {
		r = io.LimitReader(resp.Body, int64(maxBytes)+1)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if maxBytes > 0 && len(data) > maxBytes {
		return nil, fmt.Errorf("blob download exceeded max bytes (%d)", maxBytes)
	}
	return data, nil
}

func (h *Handler) insertBlobMetadataBestEffort(ctx context.Context, exec DBExecutor, sha1hex string, logicalPath string, contentType string, physical string, docID string) error {
	if exec == nil {
		return nil
	}
	metaTable := strings.TrimSpace(h.blobMetaTable)
	if metaTable == "" {
		metaTable = "doc.blob_metadata"
	}

	// file_id must be unique: include doc/table/path for traceability, but keep it bounded.
	fileID := sha1hex
	if docID != "" {
		fileID = sha1hex + ":" + docID
	}
	if physical != "" {
		fileID = sha1hex + ":" + physical
		if docID != "" {
			fileID = sha1hex + ":" + physical + ":" + docID
		}
	}
	if len(fileID) > 512 {
		fileID = sha1hex
	}

	_, err := exec.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (file_id, blob_sha1, logical_path, content_type, uploaded_at) VALUES ($1, $2, $3, $4, $5)", metaTable),
		fileID, sha1hex, logicalPath, contentType, time.Now().UTC(),
	)
	if err != nil && logging.Logger() != nil {
		logging.Logger().Debug("blob metadata insert failed", zap.Error(err))
	}
	return nil
}

func joinPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	if key == "" {
		return prefix
	}
	return prefix + "." + key
}

func (h *Handler) normalizeDocForReplyWithBlobs(ctx context.Context, doc bson.M) error {
	normalizeDocForReply(doc)
	return h.inlineBlobsForReply(ctx, doc)
}

func (h *Handler) inlineBlobsForReply(ctx context.Context, doc bson.M) error {
	if !h.blobInlineReads || doc == nil {
		return nil
	}

	cache := map[string][]byte{} // "table:sha1" -> bytes

	var walk func(v interface{}) (interface{}, error)
	walk = func(v interface{}) (interface{}, error) {
		switch t := v.(type) {
		case bson.M:
			// Stored BLOB pointer wrapper:
			// {"__mog_blob__": {"table": "...", "sha1": "...", "len": <n>, "kind": <k>}}
			if len(t) == 1 {
				if rawPtr, ok := t["__mog_blob__"]; ok && rawPtr != nil {
					ptr, okPtr := rawPtr.(bson.M)
					if !okPtr {
						if mm, ok2 := rawPtr.(map[string]interface{}); ok2 {
							ptr = bson.M(mm)
							okPtr = true
						}
					}
					if okPtr {
						table, _ := ptr["table"].(string)
						sha1hex, _ := ptr["sha1"].(string)
						if table != "" && sha1hex != "" {
							declLen := int64(0)
							switch n := ptr["len"].(type) {
							case int64:
								declLen = n
							case int32:
								declLen = int64(n)
							case int:
								declLen = int64(n)
							case float64:
								declLen = int64(n)
							}
							max := h.blobInlineMaxBytes
							if max > 0 && declLen > 0 && declLen > int64(max) {
								if h.blobInlineStrict {
									return nil, fmt.Errorf("blob inline blocked: len %d exceeds max %d", declLen, max)
								}
								return v, nil
							}

							kind := byte(0)
							switch kk := ptr["kind"].(type) {
							case int32:
								kind = byte(kk)
							case int64:
								kind = byte(kk)
							case int:
								kind = byte(kk)
							case float64:
								kind = byte(int64(kk))
							}

							key := table + ":" + sha1hex
							data, ok := cache[key]
							if !ok {
								b, err := h.getBlob(ctx, table, sha1hex, max)
								if err != nil {
									if h.blobInlineStrict {
										return nil, err
									}
									if logging.Logger() != nil {
										logging.Logger().Debug("blob inline download failed", zap.Error(err), zap.String("table", table), zap.String("sha1", sha1hex))
									}
									return v, nil
								}
								data = b
								cache[key] = b
							}
							return bson.Binary{Kind: kind, Data: data}, nil
						}
					}
				}
			}

			out := bson.M{}
			for k, vv := range t {
				nv, err := walk(vv)
				if err != nil {
					return nil, err
				}
				out[k] = nv
			}
			return out, nil
		case map[string]interface{}:
			m := bson.M(t)
			return walk(m)
		case []interface{}:
			out := make([]interface{}, 0, len(t))
			for _, el := range t {
				nv, err := walk(el)
				if err != nil {
					return nil, err
				}
				out = append(out, nv)
			}
			return out, nil
		default:
			return v, nil
		}
	}

	for k, v := range doc {
		nv, err := walk(v)
		if err != nil {
			return err
		}
		doc[k] = nv
	}
	return nil
}
