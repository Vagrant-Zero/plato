package client

import (
	"context"
	"github.com/hardcore-os/plato/state/rpc/service"
	"time"
)

func CancelConn(ctx *context.Context, endpoint string, fd int32, playLoad []byte) error {
	rpcCtx, _ := context.WithTimeout(*ctx, 100*time.Millisecond)
	_, err := stateClient.CancelConn(rpcCtx, &service.StateRequest{
		Endpoint: endpoint,
		Fd:       fd,
		Data:     playLoad,
	})
	if err != nil {
		return err
	}
	return nil
}

func SendMsg(ctx *context.Context, endpoint string, fd int32, playLoad []byte) error {
	rpcCtx, _ := context.WithTimeout(*ctx, 100*time.Millisecond)
	_, err := stateClient.SendMsg(rpcCtx, &service.StateRequest{
		Endpoint: endpoint,
		Fd:       fd,
		Data:     playLoad,
	})
	if err != nil {
		return err
	}
	return nil
}
