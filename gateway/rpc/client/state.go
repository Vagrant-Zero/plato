package client

import (
	"context"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/state/rpc/service"
	"time"
)

func CancelConn(ctx *context.Context, endpoint string, connID uint64, payLoad []byte) error {
	rpcCtx, _ := context.WithTimeout(*ctx, 100*time.Millisecond)
	_, err := stateClient.CancelConn(rpcCtx, &service.StateRequest{
		Endpoint: endpoint,
		ConnID:   connID,
		Data:     payLoad,
	})
	if err != nil {
		return err
	}
	return nil
}

func SendMsg(ctx *context.Context, endpoint string, connID uint64, payLoad []byte) error {
	rpcCtx, _ := context.WithTimeout(*ctx, 100*time.Millisecond)
	logger.CtxInfof(*ctx, "[gateway.stateClient] SendMsg, connID=%v, payload=%v", connID, string(payLoad))
	_, err := stateClient.SendMsg(rpcCtx, &service.StateRequest{
		Endpoint: endpoint,
		ConnID:   connID,
		Data:     payLoad,
	})
	if err != nil {
		return err
	}
	return nil
}
