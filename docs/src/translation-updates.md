# Update engine

MoG has two layers for updates:

1. SQL-level update translation for common cases (`$set`, `$inc`) via `internal/translator`.
2. Additional update semantics (Mongo-ish behavior) implemented inside `internal/mongo` for wire-protocol commands.

Useful code pointers:

- Update routing: `internal/mongo/handler.go`
- Update helpers: `internal/mongo/handler_update.go`, `internal/mongo/update_engine.go`

## Notes

MongoDB update semantics are deep (array filters, positional updates, complex modifiers). Expect partial coverage unless explicitly implemented.
