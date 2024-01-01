package sdk

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hardcore-os/plato/common/idl/message"
	"github.com/hardcore-os/plato/common/tcp"
	"net"
	"time"
)

const (
	MsgTypeText      = "text"
	MsgTypeAck       = "ack"
	MsgTypeReConn    = "reConn"
	MsgTypeHeartbeat = "heartbeat"
	MsgLogin         = "loginMsg"
)

type Chat struct {
	Nick      string
	UserID    string
	SessionID string
	conn      *connect
	closeChan chan struct{}
}

type Message struct {
	Type       string
	Name       string
	FormUserID string
	ToUserID   string
	Content    string
	Session    string
}

func NewChat(ip net.IP, port int, nick, userID, sessionID string, connID uint64, isReConn bool) *Chat {
	chat := &Chat{
		Nick:      nick,
		UserID:    userID,
		SessionID: sessionID,
		conn:      newConnet(ip, port, connID),
		closeChan: make(chan struct{}, 0),
	}
	go chat.loop()
	if isReConn {
		chat.reConn(connID)
	} else {
		chat.login()
	}
	go chat.heartbeat()
	return chat
}

func (chat *Chat) Send(msg *Message) {
	//chat.conn.send(msg)
	chat.conn.recvChan <- msg
}

// Close chat
func (chat *Chat) Close() {
	chat.conn.close()
	close(chat.closeChan)
	close(chat.conn.sendChan)
	close(chat.conn.recvChan)
}

func (chat *Chat) GetConnID() uint64 {
	return chat.conn.connID
}

// Recv receive message
func (chat *Chat) Recv() <-chan *Message {
	return chat.conn.recv()
}

func (chat *Chat) loop() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[chat] loop panic, err=%v\n", r)
		}
	}()
	for {
		select {
		case <-chat.closeChan:
			return
		default:
			mc := &message.MsgCmd{}
			data, err := tcp.ReadData(chat.conn.conn)
			if err != nil {
				fmt.Printf("[chat] loop: read data from conn err=%v, data=%v\n", err, string(data))
				continue
			}
			err = proto.Unmarshal(data, mc)
			if err != nil {
				fmt.Printf("[chat] loop: Unmarshal data failed, err=%v, data=%v\n", string(data))
				continue
			}
			var msg *Message
			switch mc.Type {
			case message.CmdType_ACK:
				msg = handAckMsg(chat.conn, mc.Payload)
			}
			chat.conn.recvChan <- msg
		}
	}
}

func (chat *Chat) login() {
	loginMsg := message.LoginMsg{
		Head: &message.LoginMsgHead{
			DeviceID: 123,
		},
	}
	payload, err := proto.Marshal(&loginMsg)
	if err != nil {
		fmt.Printf("[chat] login, marshal msg failed, err=%v, loginMsg.Head=%+v, loginMsg.Body=%+v\n", err, loginMsg.Head, loginMsg.LoginMsgBody)
		return
	}
	chat.conn.send(message.CmdType_Login, payload)
}

func (chat *Chat) reConn(connID uint64) {
	reConn := message.ReConnMsg{
		Head: &message.ReConnMsgHead{
			ConnID: connID,
		},
	}
	payload, err := proto.Marshal(&reConn)
	if err != nil {
		fmt.Printf("[chat] login, marshal msg failed, err=%v, ReConnMsg.Head=%+v, ReConnMsg.Body=%+v\n", err, reConn.Head, reConn.ReConnMsgBody)
		return
	}
	chat.conn.send(message.CmdType_ReConn, payload)

}

func (chat *Chat) heartbeat() {
	tc := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-chat.closeChan:
			return
		case <-tc.C:
			heartbeat := message.HeartbeatMsg{
				Head: &message.HeartbeatMsgHead{},
			}
			payload, err := proto.Marshal(&heartbeat)
			if err != nil {
				fmt.Printf("[chat] heartbeat, marshal msg failed, err=%v\n", err)
				return
			}
			chat.conn.send(message.CmdType_Heartbeat, payload)
		}
	}
}
