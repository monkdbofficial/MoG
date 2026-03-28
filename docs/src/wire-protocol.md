# Wire protocol

MoG implements a subset of the MongoDB wire protocol sufficient to support common drivers.

## Message types

- Primary: `OP_MSG`
- Some legacy compatibility paths may exist depending on client behavior.

## Authentication

MoG supports SCRAM-SHA-256 via:

- `saslStart`
- `saslContinue`

Implementation details:

- Credentials are currently stored in-memory per-process
- The SCRAM salt is fixed in code (development-friendly, not production-grade)

See `internal/mongo/auth.go` and `internal/mongo/handler.go`.
