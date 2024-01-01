package state

import (
	"context"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/golang/protobuf/proto"
	"github.com/hardcore-os/plato/common/config"
	"github.com/hardcore-os/plato/common/prpc"
	"github.com/hardcore-os/plato/state/rpc/client"
	"github.com/hardcore-os/plato/state/rpc/service"
	"google.golang.org/grpc"
	"sync"
	"time"

	"github.com/hardcore-os/plato/common/idl/message"
)

func RunMain(path string) {
	config.Init(path)
	cmdChannel = make(chan *service.CmdContext, config.GetStateCmdChannelNum())
	connToStateTable = sync.Map{}

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
	// init timingWheel
	InitTimer()
	// start cmd handler
	go cmdHandler()
	// start rpc server
	s.Start(context.TODO())
}

func cmdHandler() {
	for cmdCtx := range cmdChannel {
		switch cmdCtx.Cmd {
		case service.CancelConnCmd:
			logger.CtxInfof(*cmdCtx.Ctx, "cancelConn endpoint:%s, fd:%d, data:%+v", cmdCtx.Endpoint, cmdCtx.ConnID, cmdCtx.PayLoad)
		case service.SendMsgCmd:
			logger.CtxInfof(*cmdCtx.Ctx, "cmdHandler: %v", string(cmdCtx.PayLoad))
			msgCmd := &message.MsgCmd{}
			err := proto.Unmarshal(cmdCtx.PayLoad, msgCmd)
			if err != nil {
				logger.CtxErrorf(*cmdCtx.Ctx, "SendMsgCmd: err=%s", err.Error())
				continue
			}
			msgCmdHandler(cmdCtx, msgCmd)
		default:
			logger.CtxInfof(*cmdCtx.Ctx, "invalid cmd type, type=%v", cmdCtx.Cmd)
		}
	}
}

func msgCmdHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
	if msgCmd == nil {
		return
	}
	switch msgCmd.Type {
	case message.CmdType_Login:
		loginMsgHandler(cmdCtx, msgCmd)
	case message.CmdType_Heartbeat:
		heartbeatMsgHandler(cmdCtx, msgCmd)
	case message.CmdType_ReConn:
		reConnMsgHandler(cmdCtx, msgCmd)
	}
}

func loginMsgHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
	loginMsg := &message.LoginMsg{}
	err := proto.Unmarshal(msgCmd.Payload, loginMsg)
	if err != nil {
		logger.CtxErrorf(*cmdCtx.Ctx, "[loginMsgHandler] Unmarshal failed, err=%v", err)
		return
	}
	if loginMsg.Head != nil {
		// 这里会把 login msg 传送给业务层做处理
		logger.CtxInfof(*cmdCtx.Ctx, "loginMsgHandler", loginMsg.Head.DeviceID)
	}

	// create timer
	t := AfterFunc(300*time.Second, func() {
		clearState(cmdCtx.ConnID)
	})

	// init conn message
	connToStateTable.Store(cmdCtx.ConnID, &connState{heartTimer: t, connID: cmdCtx.ConnID})
	sendAckMsg(cmdCtx.ConnID, 0, "login")
}

func heartbeatMsgHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
	heartMsg := &message.HeartbeatMsg{}
	err := proto.Unmarshal(msgCmd.Payload, heartMsg)
	if err != nil {
		logger.CtxErrorf(*cmdCtx.Ctx, "[heartbeatMsgHandler] Unmarshal failed, err=%v", err)
		return
	}
	if data, ok := connToStateTable.Load(cmdCtx.ConnID); ok {
		state, _ := data.(*connState)
		state.resetHeartTimer()
	}
	// 减少通信量，可以暂时不回复心跳的ack
}

func reConnMsgHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
	reConnMsg := &message.ReConnMsg{}
	err := proto.Unmarshal(msgCmd.Payload, reConnMsg)
	if err != nil {
		logger.CtxErrorf(*cmdCtx.Ctx, "[reConnMsgHandler] Unmarshal failed, err=%v", err)
		return
	}
	// 重连的消息头中的connID才是上一次断开连接的connID
	if data, ok := connToStateTable.Load(reConnMsg.Head.ConnID); ok {
		state, _ := data.(*connState)
		state.Lock()
		defer state.Unlock()
		// 重连的消息头中的connID才是上一次断开连接的connID
		if state.reConnTimer != nil {
			state.reConnTimer.Stop()
			state.reConnTimer = nil
		}

		// 从索引中删除 旧的connID
		connToStateTable.Delete(reConnMsg.Head.ConnID)
		// 变更connID, cmdCtx中的connID才是 gateway重连的新连接
		state.connID = cmdCtx.ConnID
		connToStateTable.Store(state.connID, state)
		sendAckMsg(cmdCtx.ConnID, 0, "reconn ok")
	} else {
		sendAckMsg(cmdCtx.ConnID, 1, "reconn failed")
	}
}

func sendAckMsg(connID uint64, code uint32, msg string) {
	ackMsg := &message.ACKMsg{}
	ackMsg.Code = code
	ackMsg.Msg = msg
	ackMsg.ConnID = connID
	ctx := context.TODO()
	downLoad, err := proto.Marshal(ackMsg)
	if err != nil {
		logger.CtxErrorf(ctx, "sendMsg err=%s, connID=%v, code=%v, msg=%v", err, connID, code, msg)
		return
	}
	mc := &message.MsgCmd{}
	mc.Type = message.CmdType_ACK
	mc.Payload = downLoad
	data, err := proto.Marshal(mc)
	if err != nil {
		logger.CtxErrorf(ctx, "sendMsg err=%s, connID=%v, code=%v, msg=%v", err, connID, code, msg)
		return
	}
	client.Push(&ctx, connID, data)
}
