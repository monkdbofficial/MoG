# `hello` / `isMaster`

## Usage

```javascript
db.runCommand({ hello: 1 })
// or
db.runCommand({ isMaster: 1 })
```

## Behavior

- Sent by most clients immediately after connecting.
- MoG returns a “server hello” document with a supported wire version range.
- If speculative auth is included, MoG attempts it.

## Code pointers

- Command routing: `internal/mongo/handler.go`
