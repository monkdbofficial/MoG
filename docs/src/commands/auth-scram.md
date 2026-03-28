# `saslStart` / `saslContinue` (SCRAM-SHA-256)

## What it is

MongoDB drivers authenticate using SCRAM via a SASL exchange:

- `saslStart` begins the conversation
- `saslContinue` completes it

## Behavior

- MoG implements server-side SCRAM-SHA-256.
- After the exchange completes, MoG marks the connection authenticated.

!!! note "Auth implementation notes"
    The current implementation uses an in-memory credential store and a fixed salt (see `internal/mongo/auth.go`). If you need stronger guarantees, you’ll want to extend this to use a real credential backend and per-user salts/iterations.

## Code pointers

- SCRAM implementation: `internal/mongo/auth.go`
- Command handling: `internal/mongo/handler.go`
