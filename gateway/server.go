package gateway

import (
	"context"
	"errors"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/common/config"
	"github.com/hardcore-os/plato/common/tcp"
	"io"
	"net"
)

func RunMain(path string) {
	ctx := context.Background()
	config.Init(path)
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{Port: config.GetGatewayServerPort()})
	if err != nil {
		logger.CtxInfof(ctx, "StartTCPEPollServer err:%s", err.Error())
	}
	initWorkPoll()
	InitEpoll(ln, runProc)
	logger.CtxInfof(context.Background(), "-------------IM Gateway stated------------")
	select {}
}

func runProc(c *connection, ep *epoller) {
	// step1: 读取一个完整的消息包
	dataBuf, err := tcp.ReadData(c.conn)
	if err != nil {
		if errors.Is(err, io.EOF) {
			logger.CtxInfof(context.Background(), "connection[%v] closed", c.RemoteAddr())
			ep.remove(c)
		}
		return
	}
	err = wPool.Submit(func() {
		// step2:交给 state server rpc 处理
		bytes := tcp.DataPgk{
			Len:  uint32(len(dataBuf)),
			Data: dataBuf,
		}
		tcp.SendData(c.conn, bytes.Marshal())
	})

	if err != nil {
		logger.CtxInfof(context.Background(), "runProc.err: %+v", err.Error())
	}
}
