package source

import (
	"context"

	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/common/discovery"
)

func Init() {
	eventChan = make(chan *Event)
	ctx := context.Background()
	go DataHandler(&ctx)
}

// DataHandler 服务发现处理
func DataHandler(ctx *context.Context) {
	dis := discovery.NewServiceDiscovery(ctx, []string{"localhost:2379"})
	defer dis.Close()
	setFunc := func(key string, value string) {
		if ed, err := discovery.UnMarshal([]byte(value)); err == nil {
			if event := NewEvent(ed); event != nil { // 原文是ed != nil，应该是bug，应该是现在这样写
				event.Type = AddEvent
				eventChan <- event
			}
		} else {
			logger.CtxErrorf(*ctx, "DataHandler.setFunc.err :%s", err.Error())
		}
	}

	delFunc := func(key string, value string) {
		if ed, err := discovery.UnMarshal([]byte(value)); err == nil {
			if event := NewEvent(ed); event != nil {
				event.Type = DelEvent
				eventChan <- event
			}
		} else {
			logger.CtxErrorf(*ctx, "DataHandler.delFunc.err :%s", err.Error())
		}
	}

	err := dis.WatchService("/plato/ip_dispatcher", setFunc, delFunc)
	if err != nil {
		panic(err)
	}
}
