package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/golang/protobuf/proto"
	"github.com/hardcore-os/plato/common/idl/message"
	"github.com/hardcore-os/plato/common/tcp"
	"net"
	"sync/atomic"
)

type connect struct {
	sendChan, recvChan chan *Message
	conn               *net.TCPConn
	connID             uint64
	ip                 net.IP
	port               int
}

func newConnet(ip net.IP, port int) *connect {
	clientConn := &connect{
		sendChan: make(chan *Message),
		recvChan: make(chan *Message),
		ip:       ip,
		port:     port,
	}
	addr := &net.TCPAddr{
		IP:   ip,
		Port: port,
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		fmt.Printf("DialTcp.err=%+v", err)
		return nil
	}
	clientConn.conn = conn
	return clientConn
}

func handAckMsg(c *connect, data []byte) *Message {
	ackMsg := &message.ACKMsg{}
	_ = proto.Unmarshal(data, ackMsg)
	switch ackMsg.Type {
	case message.CmdType_Login, message.CmdType_ReConn:
		atomic.StoreUint64(&c.connID, ackMsg.ConnID)
	}
	return &Message{
		Type:       MsgTypeAck,
		Name:       "plato",
		FormUserID: "1212121",
		ToUserID:   "222212122",
		Content:    ackMsg.Msg,
	}
}

func handlePushMsg(c *connect, data []byte) *Message {
	pushMsg := &message.PushMsg{}
	_ = proto.Unmarshal(data, pushMsg)
	msg := &Message{}
	_ = json.Unmarshal(pushMsg.Content, msg)
	ackMsg := &message.ACKMsg{
		Type:   message.CmdType_UP,
		ConnID: c.connID,
	}
	ackData, _ := proto.Marshal(ackMsg)
	c.send(message.CmdType_ACK, ackData)
	return msg
}

func (c *connect) reConn() {
	c.conn.Close()
	addr := &net.TCPAddr{
		IP:   c.ip,
		Port: c.port,
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		logger.CtxErrorf(context.TODO(), "reConn DialTcp.err=%+v", err)
	}
	c.conn = conn
}

func (c *connect) send(ty message.CmdType, payload []byte) {
	// 直接发送给接收方
	msgCmd := message.MsgCmd{
		Type:    ty,
		Payload: payload,
	}
	msg, err := proto.Marshal(&msgCmd)
	if err != nil {
		fmt.Printf("[connect] send marshal msg failed, err=%v, type=%v, payload=%v\n", err, ty, string(payload))
		return
	}
	dataPgk := tcp.DataPgk{
		Len:  uint32(len(msg)),
		Data: msg,
	}
	c.conn.Write(dataPgk.Marshal())
}

func (c *connect) recv() <-chan *Message {
	return c.recvChan
}

func (c *connect) close() {
	// 目前没啥值得回收的
	c.conn.Close()
}
