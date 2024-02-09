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
	ctx := context.TODO()
	logger.CtxInfof(ctx, "[state] serviceName:%s Addr:%s:%d weight:%d", config.GetStateServiceName(), config.GetStateServiceAddr(), config.GetStateServerPort(), config.GetStateRPCWeight())
	s.Start(ctx)
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
	case message.CmdType_UP:
		upMsgHandler(cmdCtx, msgCmd)
	case message.CmdType_ACK:
		ackMsgHandler(cmdCtx, msgCmd)
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
	sendAckMsg(message.CmdType_Login, cmdCtx.ConnID, 0, 0, "login")
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
		sendAckMsg(message.CmdType_ReConn, cmdCtx.ConnID, 0, 0, "reconn ok")
	} else {
		sendAckMsg(message.CmdType_ReConn, cmdCtx.ConnID, 0, 1, "reconn failed")
	}
}

func sendAckMsg(ackType message.CmdType, connID, clientID uint64, code uint32, msg string) {
	ackMsg := &message.ACKMsg{}
	ackMsg.Code = code
	ackMsg.Msg = msg
	ackMsg.ConnID = connID
	ackMsg.Type = ackType
	ackMsg.ClientID = clientID
	ctx := context.TODO()
	downLoad, err := proto.Marshal(ackMsg)
	if err != nil {
		logger.CtxErrorf(ctx, "sendAckMsg err=%s, connID=%v, code=%v, msg=%v", err, connID, code, msg)
		return
	}
	sendMsg(connID, message.CmdType_ACK, downLoad)
}

func sendMsg(connID uint64, ty message.CmdType, download []byte) {
	mc := &message.MsgCmd{}
	mc.Type = ty
	mc.Payload = download
	data, err := proto.Marshal(mc)
	ctx := context.TODO()
	if err != nil {
		logger.CtxErrorf(ctx, "sendMsg err=%s, connID=%v, ty=%v, msg=%v", err, connID, ty, string(download))
		return
	}
	client.Push(&ctx, connID, data)
}

// 处理下行消息
func ackMsgHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
	ackMsg := &message.ACKMsg{}
	err := proto.Unmarshal(msgCmd.Payload, ackMsg)
	if err != nil {
		logger.CtxErrorf(*cmdCtx.Ctx, "ackMsgHandler err=%v", err.Error())
		return
	}
	if data, ok := connToStateTable.Load(ackMsg.ConnID); ok {
		state, _ := data.(*connState)
		state.Lock()
		defer state.Unlock()
		if state.msgTimer != nil {
			state.msgTimer.Stop()
			state.msgTimer = nil
		}
	}
}

// 处理上行消息，并进行消息可靠性检查
func upMsgHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
	upMsg := &message.UPMsg{}
	err := proto.Unmarshal(msgCmd.Payload, upMsg)
	if err != nil {
		logger.CtxErrorf(*cmdCtx.Ctx, "upMsgHandler err=%v", err.Error())
		return
	}
	if data, ok := connToStateTable.Load(upMsg.Head.ConnID); ok {
		state, _ := data.(*connState)
		if !state.checkUPMsg(upMsg.Head.ClientID) {
			// todo 如果没有通过检查，当作先直接忽略即可
			return
		}
		// 调用下游业务层rpc，只有当rpc回复成功后才能更新max_clientID
		// 这里先假设成功
		state.addMaxClientID()
		state.msgID++
		sendAckMsg(message.CmdType_UP, upMsg.Head.ConnID, upMsg.Head.ClientID, 0, "ok")

		// todo 先在这里push消息
		pushMsg := &message.PushMsg{
			MsgID:   state.msgID,
			Content: upMsg.UPMsgBody,
		}
		pData, err := proto.Marshal(pushMsg)
		if err != nil {
			logger.CtxErrorf(*cmdCtx.Ctx, "upMsgHandler Marshal pushMsg err=%v, pData=%v", err.Error(), string(pData))
			return
		}
		sendMsg(state.connID, message.CmdType_Push, pData)
		if state.msgTimer != nil {
			state.msgTimer = nil
		}
		// create pushMsg timer
		t := AfterFunc(100*time.Millisecond, func() {
			rePush(cmdCtx.ConnID, pData)
		})
		state.msgTimer = t
	}
}
