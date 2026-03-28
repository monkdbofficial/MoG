# `buildInfo`

## Usage

```javascript
db.runCommand({ buildInfo: 1 })
```

## Behavior

- Returns a Mongo-ish version and a short adapter identifier via `gitVersion`.

## Code pointers

- Handler: `internal/mongo/handler.go`
- Adapter version string: `internal/mongo/version.go`
