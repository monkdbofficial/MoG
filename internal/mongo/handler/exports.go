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

func NewHandler(pool *pgxpool.Pool, t *translator.Translator, scram *ScramSha256) *Handler {
	return core.NewHandler(pool, t, scram)
}

func NewScramSha256(username, password string) (*ScramSha256, error) {
	return core.NewScramSha256(username, password)
}

func WithRemoteAddr(ctx context.Context, remoteAddr string) context.Context {
	return core.WithRemoteAddr(ctx, remoteAddr)
}

func RemoteAddr(ctx context.Context) string {
	return core.RemoteAddr(ctx)
}
