package gateway

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"net"

	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/common/config"
	"github.com/hardcore-os/plato/common/prpc"
	"github.com/hardcore-os/plato/common/tcp"
	"github.com/hardcore-os/plato/gateway/rpc/client"
	"github.com/hardcore-os/plato/gateway/rpc/service"
)

var (
	cmdChannel chan *service.CmdContext
)

func RunMain(path string) {
	config.Init(path)
	startTCPServer()
	startRPCServer()
}

func startTCPServer() {
	ctx := context.Background()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{Port: config.GetGatewayTCPServerPort()})
	if err != nil {
		logger.CtxInfof(ctx, "StartTCPEPollServer err:%s", err.Error())
		panic(err)
	}
	initWorkPoll()
	InitEpoll(ln, runProc)
	logger.CtxInfof(context.Background(), "-------------IM Gateway started------------")
}

func startRPCServer() {
	ctx := context.Background()
	cmdChannel = make(chan *service.CmdContext, config.GetGatewayCmdChannelNum())
	s := prpc.NewPServer(
		prpc.WithServiceName(config.GetGatewayServiceName()),
		prpc.WithIP(config.GetGatewayServiceAddr()),
		prpc.WithPort(config.GetGatewayRPCServerPort()),
		prpc.WithWeight(config.GetGatewayRPCWeight()),
	)
	logger.CtxInfof(ctx, "[gateway] serviceName:%s Addr:%s:%d weight:%d", config.GetGatewayServiceName(), config.GetGatewayServiceAddr(), config.GetGatewayRPCServerPort(), config.GetGatewayRPCWeight())
	s.RegisterService(func(server *grpc.Server) {
		service.RegisterGatewayServer(server, &service.Service{CmdChannel: cmdChannel})
	})
	// start client
	client.Init()
	// start cmdHandler
	go cmdHandler()
	// start rpc server
	s.Start(context.TODO())
}

func runProc(c *connection, ep *epoller) {
	ctx := context.Background()
	// step1: 读取一个完整的消息包
	dataBuf, err := tcp.ReadData(c.conn)
	if err != nil {
		if errors.Is(err, io.EOF) {
			logger.CtxInfof(context.Background(), "connection[%v] closed", c.RemoteAddr())
			ep.remove(c)
			client.CancelConn(&ctx, getEndpoint(), c.id, nil)
		}
		return
	}
	err = wPool.Submit(func() {
		// step2:交给 message server rpc 处理
		client.SendMsg(&ctx, getEndpoint(), c.id, dataBuf)
	})

	if err != nil {
		logger.CtxInfof(context.Background(), "runProc.err: %+v", err.Error())
		fmt.Errorf("runProc.err: %+v\n", err.Error())
	}
}

func cmdHandler() {
	for cmd := range cmdChannel {
		// async submit task to goroutine pool
		switch cmd.Cmd {
		case service.DelConnCmd:
			wPool.Submit(func() {
				closeConn(cmd)
			})
		case service.PushCmd:
			wPool.Submit(func() {
				sendMsgByCmd(cmd)
			})
		default:
			panic("command undefined")
		}
	}
}

func closeConn(cmd *service.CmdContext) {
	if cmdChannel == nil {
		return
	}
	if connPtr, ok := ep.tables.Load(cmd.ConnID); ok {
		conn, _ := connPtr.(*connection)
		conn.Close()
	}
}

func sendMsgByCmd(cmd *service.CmdContext) {
	if cmdChannel == nil {
		return
	}
	if connPtr, ok := ep.tables.Load(cmd.ConnID); ok {
		conn, _ := connPtr.(*connection)
		dp := tcp.DataPgk{
			Len:  uint32(len(cmd.PayLoad)),
			Data: cmd.PayLoad,
		}
		tcp.SendData(conn.conn, dp.Marshal())
	}
}

func getEndpoint() string {
	return fmt.Sprintf("%s:%d", config.GetGatewayServiceAddr(), config.GetGatewayRPCServerPort())
}
