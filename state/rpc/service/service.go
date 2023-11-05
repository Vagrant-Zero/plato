package service

import (
	"context"
	"github.com/bytedance/gopkg/util/logger"
)

const (
	CancelConnCmd = 1
	SendMsgCmd    = 2
)

type CmdContext struct {
	Ctx      *context.Context
	Cmd      int32
	Endpoint string
	FD       int
	PlayLoad []byte
}

type Service struct {
	UnimplementedStateServer
	CmdChannel chan *CmdContext
}

func (s *Service) CancelConn(ctx context.Context, sr *StateRequest) (*StateResponse, error) {
	c := context.TODO()
	s.CmdChannel <- &CmdContext{
		Ctx:      &c,
		Cmd:      CancelConnCmd,
		Endpoint: sr.GetEndpoint(),
		FD:       int(sr.GetFd()),
	}

	return &StateResponse{
		Code: 0,
		Msg:  "success",
	}, nil
}

func (s *Service) SendMsg(ctx context.Context, sr *StateRequest) (*StateResponse, error) {
	logger.CtxInfof(ctx, "[state] receive SendMsg request ok")
	c := context.TODO()
	s.CmdChannel <- &CmdContext{
		Ctx:      &c,
		Cmd:      SendMsgCmd,
		FD:       int(sr.GetFd()),
		Endpoint: sr.GetEndpoint(),
		PlayLoad: sr.GetData(),
	}

	logger.CtxInfof(ctx, "[state] sendMsg to channel ok")

	return &StateResponse{
		Code: 0,
		Msg:  "success",
	}, nil
}
