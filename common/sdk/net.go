package sdk

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hardcore-os/plato/common/idl/message"
	"github.com/hardcore-os/plato/common/tcp"
	"net"
)

type connect struct {
	sendChan, recvChan chan *Message
	conn               *net.TCPConn
	connID             uint64
}

func newConnet(ip net.IP, port int, connID uint64) *connect {
	clientConn := &connect{
		sendChan: make(chan *Message),
		recvChan: make(chan *Message),
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
	if connID != 0 {
		clientConn.connID = connID
	}
	return clientConn
}

func handAckMsg(c *connect, data []byte) *Message {
	ackMsg := &message.ACKMsg{}
	_ = proto.Unmarshal(data, ackMsg)
	switch ackMsg.Type {
	case message.CmdType_Login:
		c.connID = ackMsg.ConnID
	}
	return &Message{
		Type:       MsgTypeAck,
		Name:       "plato",
		FormUserID: "1212121",
		ToUserID:   "222212122",
		Content:    ackMsg.Msg,
	}
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
