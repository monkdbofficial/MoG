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
	if !h.blobEnabled() || doc == nil {
		return nil
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
				if h.blobMinBytes > 0 && len(data) < h.blobMinBytes {
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

	client := &http.Client{Timeout: h.httpTimeout()}
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

