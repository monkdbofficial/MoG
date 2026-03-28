# `getLog` (`startupWarnings`)

## Usage

```javascript
db.runCommand({ getLog: "startupWarnings" })
```

## Behavior

Returns an empty warning log for compatibility. Other variants are not supported.

## Code pointers

- `internal/mongo/handler.go`
