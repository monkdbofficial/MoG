# Transactions (compatibility only)

## Commands

- `startTransaction`
- `commitTransaction`
- `abortTransaction`

## Behavior

MonkDB does not provide MongoDB-style transaction semantics. MoG accepts these commands for **driver compatibility**, but you should assume **no transactional guarantees** (atomicity/isolation/rollback) at the MongoDB level.

What this means in practice:

- Clients that send `startTransaction` / `commitTransaction` / `abortTransaction` won’t immediately fail.
- MoG will continue executing operations as normal SQL statements against MonkDB.
- Even if MoG attempts to wrap operations with a backend transaction handle, you should treat it as **best-effort** and validate behavior against your MonkDB setup.

## Implementation notes

- MoG tracks a transaction handle (`h.tx`) for the session when these commands are used.
- This exists to keep drivers moving forward, not to emulate full MongoDB transactions.

## Code pointers

- `internal/mongo/handler.go`
