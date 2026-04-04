package mongo

import "context"

type ctxKey int

const (
	ctxKeyRemoteAddr ctxKey = iota
)

// WithRemoteAddr returns a derived value.
func WithRemoteAddr(ctx context.Context, remoteAddr string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, ctxKeyRemoteAddr, remoteAddr)
}

// RemoteAddr is a helper used by the adapter.
func RemoteAddr(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v := ctx.Value(ctxKeyRemoteAddr); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
