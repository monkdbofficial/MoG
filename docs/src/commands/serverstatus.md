# `serverStatus`

## Usage

```javascript
db.runCommand({ serverStatus: 1 })
```

## Behavior

Returns a minimal server status document for compatibility.

## Code pointers

- `internal/mongo/handler.go`
