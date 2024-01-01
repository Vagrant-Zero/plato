package client

import (
	"context"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/gateway/rpc/service"
	"time"
)

func DelConn(ctx *context.Context, connID uint64, payLoad []byte) error {
	rpcCtx, _ := context.WithTimeout(*ctx, 100*time.Millisecond)
	gatewayClient.DelConn(rpcCtx, &service.GatewayRequest{
		ConnID: connID,
		Data:   payLoad,
	})
	return nil
}

func Push(ctx *context.Context, connID uint64, payLoad []byte) error {
	rpcCtx, _ := context.WithTimeout(*ctx, 100*time.Millisecond)
	resp, err := gatewayClient.Push(rpcCtx, &service.GatewayRequest{
		ConnID: connID,
		Data:   payLoad,
	})
	if err != nil {
		logger.CtxErrorf(rpcCtx, "message.gatewayClient push msg err, err=%v", err)
		return err
	}
	logger.CtxInfof(rpcCtx, "message.gatewayClient push msg resp, resp=%+v", resp)
	return nil
}
