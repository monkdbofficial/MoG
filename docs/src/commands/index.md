# Commands

MoG implements a pragmatic subset of the MongoDB command surface. This section documents each supported command as its own page so the left sidebar can expand/collapse.

Conventions used in this reference:

- **Usage** shows `db.runCommand(...)` (mongosh) and/or driver snippets.
- **Behavior** describes what MoG does for compatibility.
- **SQL mapping** shows the shape of SQL MoG emits (when applicable).
- **Code pointers** link to the implementation entry points.

Start here:

- CRUD: [`find`](find.md), [`insert`](insert.md), [`update`](update.md), [`delete`](delete.md), [`count`](count.md)
- Aggregation: [`aggregate`](aggregate.md)
- Auth: [`saslStart`/`saslContinue`](auth-scram.md)

> Source of truth: `internal/mongo/handler.go`
