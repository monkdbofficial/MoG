# `connectionStatus`

## Usage

```javascript
db.runCommand({ connectionStatus: 1 })
```

## Behavior

Returns an `authInfo` document that many drivers expect.

## Code pointers

- `internal/mongo/handler.go`
