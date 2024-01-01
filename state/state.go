package state

import (
	"context"
	"github.com/hardcore-os/plato/common/timingwheel"
	"github.com/hardcore-os/plato/state/rpc/client"
	"sync"
	"time"

	"github.com/hardcore-os/plato/state/rpc/service"
)

var cmdChannel chan *service.CmdContext
var connToStateTable sync.Map

type connState struct {
	sync.RWMutex
	heartTimer  *timingwheel.Timer
	reConnTimer *timingwheel.Timer
	connID      uint64
}

func (c *connState) resetHeartTimer() {
	c.Lock()
	defer c.Unlock()
	c.heartTimer.Stop()
	c.heartTimer = AfterFunc(5*time.Second, func() {
		clearState(c.connID)
	})
}

func clearState(connID uint64) {
	if data, ok := connToStateTable.Load(connID); ok {
		state, _ := data.(*connState)
		state.Lock()
		defer state.Unlock()
		// prevent fake disconnection, wait for 10s
		state.reConnTimer = AfterFunc(10*time.Second, func() {
			ctx := context.TODO()
			client.DelConn(&ctx, connID, nil)
			// del conn message
			connToStateTable.Delete(connID)
		})
	}
}
