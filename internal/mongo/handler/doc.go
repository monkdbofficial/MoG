// Package mongo implements a MongoDB-compatible wire protocol handler backed by SQL storage.
//
// This package is internal to the server and is intentionally compatibility-focused:
// it aims to match MongoDB driver expectations (wire message shapes, error documents,
// and common command behavior) while translating operations into SQL for the backend.
package mongo
