package mongo

import (
	"context"

	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/handler/opmsg"
)

func (h *Handler) opMsgCmdAggregate(ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	return opmsg.CmdAggregate(h.opmsgDeps(), ctx, requestID, cmd)
}

func (h *Handler) opMsgCmdCount(ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	return opmsg.CmdCount(h.opmsgDeps(), ctx, requestID, cmd)
}

func (h *Handler) opMsgCmdDelete(ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	return opmsg.CmdDelete(h.opmsgDeps(), ctx, requestID, cmd)
}

func (h *Handler) opMsgCmdFind(ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	return opmsg.CmdFind(h.opmsgDeps(), ctx, requestID, cmd)
}

func (h *Handler) opMsgCmdInsert(ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	return opmsg.CmdInsert(h.opmsgDeps(), ctx, requestID, cmd)
}

func (h *Handler) opMsgCmdUpdate(ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	return opmsg.CmdUpdate(h.opmsgDeps(), ctx, requestID, cmd)
}
