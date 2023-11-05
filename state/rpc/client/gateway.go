package client

import (
	"context"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/gateway/rpc/service"
	"time"
)

func DelConn(ctx *context.Context, fd int32, playLoad []byte) error {
	rpcCtx, _ := context.WithTimeout(*ctx, 100*time.Millisecond)
	gatewayClient.DelConn(rpcCtx, &service.GatewayRequest{
		Fd:   fd,
		Data: playLoad,
	})
	return nil
}

func Push(ctx *context.Context, fd int32, playLoad []byte) error {
	rpcCtx, _ := context.WithTimeout(*ctx, 100*time.Millisecond)
	resp, err := gatewayClient.Push(rpcCtx, &service.GatewayRequest{
		Fd:   fd,
		Data: playLoad,
	})
	if err != nil {
		logger.CtxErrorf(rpcCtx, "state.gatewayClient push msg err, err=%v", err)
		return err
	}
	logger.CtxInfof(rpcCtx, "state.gatewayClient push msg resp, resp=%+v", resp)
	return nil
}
