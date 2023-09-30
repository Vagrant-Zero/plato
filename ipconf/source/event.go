package source

import (
	"fmt"
	"github.com/hardcore-os/plato/common/discovery"
)

var eventChan chan *Event

func EventChan() chan *Event {
	return eventChan
}

type EventType string

const (
	AddEvent EventType = "addNode"
	DelEvent EventType = "delNode"

	connectNum   string = "connect_num"
	messageBytes string = "message_bytes"
)

type Event struct {
	Type         EventType
	IP           string
	Port         string
	ConnectNum   float64 // 连接数量
	MessageBytes float64 // 接收的字节数量
}

func NewEvent(ed *discovery.EndpointInfo) *Event {
	if ed == nil || ed.MetaData == nil {
		return nil
	}
	var connNum, msgBytes float64
	if data, ok := ed.MetaData[connectNum]; ok {
		connNum, ok = data.(float64)
		if !ok {
			panic("connNum transfer failed")
		}
	}

	if data, ok := ed.MetaData[messageBytes]; ok {
		msgBytes, ok = data.(float64)
		if !ok {
			panic("messageBytes transfer failed")
		}
	}

	return &Event{
		Type:         AddEvent,
		IP:           ed.IP,
		Port:         ed.Port,
		ConnectNum:   connNum,
		MessageBytes: msgBytes,
	}
}

func (e *Event) Key() string {
	return fmt.Sprintf("%s:%s", e.IP, e.Port)
}
