# `hostInfo`

## Usage

```javascript
db.runCommand({ hostInfo: 1 })
```

## Behavior

Returns a minimal host info document for client/tooling compatibility.

## Code pointers

- `internal/mongo/handler.go`
