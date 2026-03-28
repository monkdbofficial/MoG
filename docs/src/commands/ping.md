# `ping`

## Usage

```javascript
db.runCommand({ ping: 1 })
```

## Behavior

Returns `{ ok: 1.0 }`. Used by drivers for monitoring/health checks.

## Code pointers

- `internal/mongo/handler.go`
