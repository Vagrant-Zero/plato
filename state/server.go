package state

import (
	"context"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/common/config"
	"github.com/hardcore-os/plato/common/prpc"
	"github.com/hardcore-os/plato/state/rpc/client"
	"github.com/hardcore-os/plato/state/rpc/service"
	"google.golang.org/grpc"
)

var cmdChannel chan *service.CmdContext

func RunMain(path string) {
	config.Init(path)
	cmdChannel = make(chan *service.CmdContext, config.GetStateCmdChannelNum())

	s := prpc.NewPServer(
		prpc.WithServiceName(config.GetStateServiceName()),
		prpc.WithIP(config.GetStateServiceAddr()),
		prpc.WithPort(config.GetStateServerPort()),
		prpc.WithWeight(config.GetStateRPCWeight()),
	)
	s.RegisterService(func(server *grpc.Server) {
		service.RegisterStateServer(server, &service.Service{CmdChannel: cmdChannel})
	})
	// init rpc client
	client.Init()
	// start cmd handler
	go cmdHandler()
	// start rpc server
	s.Start(context.TODO())
}

func cmdHandler() {
	for cmd := range cmdChannel {
		switch cmd.Cmd {
		case service.CancelConnCmd:
			logger.CtxInfof(*cmd.Ctx, "cancelConn endpoint:%s, fd:%d, data:%+v", cmd.Endpoint, cmd.FD, cmd.PlayLoad)
		case service.SendMsgCmd:
			logger.CtxInfof(*cmd.Ctx, "cmdHandler: %v", string(cmd.PlayLoad))
			client.Push(cmd.Ctx, int32(cmd.FD), cmd.PlayLoad)
		default:
			logger.CtxInfof(*cmd.Ctx, "invalid cmd type, type=%v", cmd.Cmd)
		}

	}
}
