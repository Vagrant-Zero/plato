package state

import (
	"context"
	"fmt"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/common/cache"
	"github.com/hardcore-os/plato/common/router"
	"github.com/hardcore-os/plato/common/timingwheel"
	"github.com/hardcore-os/plato/state/rpc/client"
	"sync"
	"time"
)

type connState struct {
	sync.RWMutex
	heartTimer   *timingwheel.Timer
	reConnTimer  *timingwheel.Timer
	msgTimer     *timingwheel.Timer
	msgTimerLock string
	connID       uint64
	did          uint64
}

func (c *connState) close(ctx context.Context) error {
	c.Lock()
	defer c.Unlock()
	if c.heartTimer != nil {
		c.heartTimer.Stop()
	}
	if c.reConnTimer != nil {
		c.reConnTimer.Stop()
	}
	if c.msgTimer != nil {
		c.msgTimer.Stop()
	}
	// TODO 这里如何保证事务性，值得思考一下，或者说有没有必要保证
	// TODO 这里也可以使用lua或者pipeline 来尽可能合并两次redis的操作 通常在大规模的应用中这是有效的
	// TODO 这里是要好好思考一下，网络调用次数的时间&空间复杂度的

	// TODO 可以加一个simple retry，redis的del操作是幂等的，但是也没有解决事务的问题
	slotKey := cs.getLoginSlotKey(c.connID)
	meta := cs.loginSlotMarshal(c.did, c.connID)
	err := cache.SREM(ctx, slotKey, meta)
	if err != nil {
		logger.CtxErrorf(ctx, "[close] cache set remove login meta failed, did=%v, connID=%v, err=%v", c.did, c.connID, err)
		return err
	}
	slot := cs.getConnStateSLot(c.connID)
	key := fmt.Sprintf(cache.MaxClientIDKey, slot, c.connID)
	err = cache.Del(ctx, key)
	if err != nil {
		logger.CtxErrorf(ctx, "[close] cache remove login data failed, slot=%v, connID=%v, err=%v", slot, c.connID, err)
		return err
	}
	err = router.DelRouter(ctx, c.did)
	if err != nil {
		logger.CtxErrorf(ctx, "[close] cache del record failed, did=%v, err=%v", c.did, err)
		return err
	}
	lastMsgKey := fmt.Sprintf(cache.LastMsgKey, slot, c.connID)
	err = cache.Del(ctx, lastMsgKey)
	if err != nil {
		logger.CtxErrorf(ctx, "[close] cache del lastMsg failed, slot=%v, connID=%v, err=%v", slot, c.connID, err)
		return err
	}
	err = client.DelConn(&ctx, c.connID, nil)
	if err != nil {
		logger.CtxErrorf(ctx, "[close] gateway client del conn failed, connID=%v, err=%v", c.connID, err)
		return err
	}
	cs.deleteConnIDState(ctx, c.connID)
	return nil
}

func (c *connState) appendMsg(ctx context.Context, key, msgTimerLock string, msgData []byte) {
	c.Lock()
	defer c.Unlock()
	c.msgTimerLock = msgTimerLock
	if c.msgTimer != nil {
		c.msgTimer.Stop()
		c.msgTimer = nil
	}
	// create timer
	t := AfterFunc(100*time.Millisecond, func() {
		rePush(c.connID)
	})
	c.msgTimer = t
	err := cache.SetBytes(ctx, key, msgData, cache.TTL7D)
	if err != nil {
		logger.CtxErrorf(ctx, "[appendMsg] set rePush cache failed, key=%v, msgData=%v, err=%v", key, msgData, err)
		return
	}
}

func (c *connState) resetMsgTimer(connID, sessionID, msgID uint64) {
	c.Lock()
	defer c.Unlock()
	if c.msgTimer != nil {
		c.msgTimer.Stop()
		c.msgTimer = nil
	}
	c.msgTimerLock = fmt.Sprintf("%d_%d", sessionID, msgID)
	c.msgTimer = AfterFunc(100*time.Millisecond, func() {
		rePush(c.connID)
	})
}

func (c *connState) loadMsgTimer(ctx context.Context) {
	// create timer
	data, err := cs.getLastMsg(ctx, c.connID)
	if err != nil {
		return
	}
	if data == nil {
		return
	}
	c.resetMsgTimer(c.connID, data.SessionID, data.MsgID)
}

func (c *connState) resetHeartTimer() {
	c.Lock()
	defer c.Unlock()
	if c.heartTimer != nil {
		c.heartTimer.Stop()
		c.heartTimer = nil
	}
	c.heartTimer = AfterFunc(5*time.Second, func() {
		c.resetConnTimer()
	})
}

func (c *connState) resetConnTimer() {
	c.Lock()
	defer c.Unlock()
	if c.reConnTimer != nil {
		c.reConnTimer.Stop()
		c.reConnTimer = nil
	}

	c.reConnTimer = AfterFunc(10*time.Second, func() {
		ctx := context.TODO()
		// 整体connID状态登出
		// 不马上清理，而是等待重连，10s之后才会清理连接的数据
		cs.connLogout(ctx, c.connID)
	})
}

func (c *connState) ackLastMsg(ctx context.Context, sessionID, msgID uint64) bool {
	c.Lock()
	defer c.Unlock()
	msgTimerLock := fmt.Sprintf("%d_%d", sessionID, msgID)
	if c.msgTimerLock != msgTimerLock {
		return false
	}
	slot := cs.getConnStateSLot(c.connID)
	key := fmt.Sprintf(cache.LastMsgKey, slot, c.connID)
	if err := cache.Del(ctx, key); err != nil {
		return false
	}
	if c.msgTimer != nil {
		c.msgTimer.Stop()
	}
	return true
}
