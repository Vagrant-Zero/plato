package state

import (
	"context"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/golang/protobuf/proto"
	"github.com/hardcore-os/plato/common/config"
	"github.com/hardcore-os/plato/common/idl/message"
	"github.com/hardcore-os/plato/common/prpc"
	"github.com/hardcore-os/plato/state/rpc/client"
	"github.com/hardcore-os/plato/state/rpc/service"
	"google.golang.org/grpc"
)

func RunMain(path string) {
	// init ctx
	ctx := context.TODO()
	// init config
	config.Init(path)
	// init rpc client
	client.Init()
	// start timeWheel
	InitTimer()
	// start remote cache
	InitCacheState(ctx)
	// start cmdHandler
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.CtxErrorf(ctx, "cmdHandler panic, r=%v", r)
			}
		}()
		cmdHandler()
	}()

	// register rpc server
	s := prpc.NewPServer(
		prpc.WithServiceName(config.GetStateServiceName()),
		prpc.WithIP(config.GetStateServiceAddr()),
		prpc.WithPort(config.GetStateServerPort()),
		prpc.WithWeight(config.GetStateRPCWeight()))
	s.RegisterService(func(server *grpc.Server) {
		service.RegisterStateServer(server, cs.server)
	})
	logger.CtxInfof(ctx, "[state] serviceName:%s Addr:%s:%d weight:%d", config.GetStateServiceName(), config.GetStateServiceAddr(), config.GetStateServerPort(), config.GetStateRPCWeight())
	s.Start(ctx)
}

// 消费信令通道，识别gateway与state server之间的协议路由
func cmdHandler() {
	for cmdCtx := range cs.server.CmdChannel {
		switch cmdCtx.Cmd {
		case service.CancelConnCmd:
			logger.CtxInfof(*cmdCtx.Ctx, "cancelConn endpoint:%s, fd:%d, data:%+v", cmdCtx.Endpoint, cmdCtx.ConnID, cmdCtx.PayLoad)
			cs.connLogout(*cmdCtx.Ctx, cmdCtx.ConnID)
		case service.SendMsgCmd:
			logger.CtxInfof(*cmdCtx.Ctx, "cmdHandler: %v\n", string(cmdCtx.PayLoad))
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

// 识别消息类型，识别客户端与state server之间的协议路由
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
		logger.CtxErrorf(*cmdCtx.Ctx, "[loginMsgHandler] Unmarshal failed, payload=%v, err=%v", string(msgCmd.Payload), err)
		return
	}
	if loginMsg.Head != nil {
		// 这里会把 login msg 传送给业务层做处理
		logger.CtxInfof(*cmdCtx.Ctx, "[loginMsgHandler]:%v", loginMsg.Head.DeviceID)
	}
	err = cs.connLogin(*cmdCtx.Ctx, loginMsg.Head.DeviceID, cmdCtx.ConnID)
	if err != nil {
		logger.CtxErrorf(*cmdCtx.Ctx, "[loginMsgHandler] connStateLogin failed, did=%v, connID=%v, err=%v", loginMsg.Head.DeviceID, cmdCtx.ConnID, err)
		return
	}
	sendAckMsg(message.CmdType_Login, cmdCtx.ConnID, 0, 0, "login ok")
}

func heartbeatMsgHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
	heartMsg := &message.HeartbeatMsg{}
	err := proto.Unmarshal(msgCmd.Payload, heartMsg)
	if err != nil {
		logger.CtxErrorf(*cmdCtx.Ctx, "[heartbeatMsgHandler] Unmarshal failed, err=%v", err)
		return
	}
	cs.resetHeartTimer(cmdCtx.ConnID)
	logger.CtxInfof(*cmdCtx.Ctx, "[heartbeatMsgHandler] reset heartbeat success, connID=%v", cmdCtx.ConnID)
	// TODO 未减少通信量，可以暂时不回复心跳的ack
}

func reConnMsgHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
	reConnMsg := &message.ReConnMsg{}
	err := proto.Unmarshal(msgCmd.Payload, reConnMsg)
	if err != nil {
		logger.CtxErrorf(*cmdCtx.Ctx, "[reConnMsgHandler] Unmarshal failed, err=%v", err)
		return
	}
	var code uint32
	msg := "reconnection OK"
	// 重连的消息头中的connID才是上一次断开连接的connID
	if err = cs.reConn(*cmdCtx.Ctx, reConnMsg.Head.ConnID, cmdCtx.ConnID); err != nil {
		code, msg = 1, "reconnection failed"
		logger.CtxErrorf(*cmdCtx.Ctx, "[reConnMsgHandler] reconnection failed, old connID=%v, newConnID=%v, err=%v", reConnMsg.Head.ConnID, cmdCtx.ConnID, err)
		return
	}
	sendAckMsg(message.CmdType_ReConn, cmdCtx.ConnID, 0, code, msg)
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
	cs.ackLastMsg(*cmdCtx.Ctx, ackMsg.ConnID, ackMsg.SessionID, ackMsg.MsgID)
}

// 处理上行消息，并进行消息可靠性检查
func upMsgHandler(cmdCtx *service.CmdContext, msgCmd *message.MsgCmd) {
	upMsg := &message.UPMsg{}
	err := proto.Unmarshal(msgCmd.Payload, upMsg)
	if err != nil {
		logger.CtxErrorf(*cmdCtx.Ctx, "upMsgHandler err=%v", err.Error())
		return
	}
	if cs.compareAndIncrClientID(*cmdCtx.Ctx, cmdCtx.ConnID, upMsg.Head.ClientID) {
		// 调用下游业务层rpc，只有当rpc回复成功后才能更新max_clientID
		sendAckMsg(message.CmdType_UP, cmdCtx.ConnID, upMsg.Head.ClientID, 0, "OK")
		// TODO 这里应该调用业务层的代码
		pushMsg(*cmdCtx.Ctx, cmdCtx.ConnID, cs.msgID, 0, upMsg.UPMsgBody)
	}
}

func pushMsg(ctx context.Context, connID, sessionID, msgID uint64, data []byte) {
	// TODO 先在这里push消息
	pMsg := &message.PushMsg{
		MsgID:   cs.msgID,
		Content: data,
	}
	pData, err := proto.Marshal(pMsg)
	if err != nil {
		logger.CtxErrorf(ctx, "[pushMsg] pushMsg failed, connID=%v, sessionID=%v, msgID=%v, data=%v, err=%v", connID, sessionID, msgID, string(data), err)
		return
	}
	sendMsg(connID, message.CmdType_Push, pData)
	if err = cs.appendLastMsg(ctx, connID, pMsg); err != nil {
		logger.CtxErrorf(ctx, "[pushMsg] append last msg failed, connID=%v, sessionID=%v, msgID=%v, data=%v, err=%v", connID, sessionID, msgID, string(data), err)
	}
}

func rePush(connID uint64) {
	ctx := context.TODO()
	pMsg, err := cs.getLastMsg(ctx, connID)
	if err != nil {
		logger.CtxErrorf(ctx, "rePushMsg failed, connID=%v", connID)
		return
	}
	msgData, err := proto.Marshal(pMsg)
	if err != nil {
		logger.CtxErrorf(ctx, "rePushMsg marshal msg failed, connID=%v, msg=%+v", connID, pMsg)
		return
	}
	sendMsg(connID, message.CmdType_Push, msgData)
	if st, ok := cs.loadConnIDState(connID); ok {
		st.resetMsgTimer(connID, pMsg.SessionID, pMsg.MsgID)
	}
}
