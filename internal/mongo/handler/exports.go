package mongo

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	core "mog/internal/mongo/handler/core"
	"mog/internal/translator"
)

type Handler = core.Handler

type ScramSha256 = core.ScramSha256
type Conversation = core.Conversation

// NewHandler constructs a new value.
func NewHandler(pool *pgxpool.Pool, t *translator.Translator, scram *ScramSha256) *Handler {
	return core.NewHandler(pool, t, scram)
}

// NewScramSha256 constructs a new value.
func NewScramSha256(username, password string) (*ScramSha256, error) {
	return core.NewScramSha256(username, password)
}

// WithRemoteAddr returns a derived value.
func WithRemoteAddr(ctx context.Context, remoteAddr string) context.Context {
	return core.WithRemoteAddr(ctx, remoteAddr)
}

// RemoteAddr is a helper used by the adapter.
func RemoteAddr(ctx context.Context) string {
	return core.RemoteAddr(ctx)
}
