package state

import (
	"context"
	"errors"
	"fmt"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/golang/protobuf/proto"
	"github.com/hardcore-os/plato/common/cache"
	"github.com/hardcore-os/plato/common/config"
	"github.com/hardcore-os/plato/common/idl/message"
	"github.com/hardcore-os/plato/common/router"
	"github.com/hardcore-os/plato/state/rpc/service"
	"strconv"
	"strings"
	"sync"
)

var cs *cacheState

// 远程cache状态
type cacheState struct {
	msgID            uint64 // for test
	connToStateTable sync.Map
	server           *service.Service
}

// InitCacheState 初始化全局cache
func InitCacheState(ctx context.Context) {
	cs = &cacheState{}
	cache.InitRedis(ctx)
	router.Init(ctx)
	cs.connToStateTable = sync.Map{}
	cs.initLoginSlot(ctx)
	cs.server = &service.Service{
		CmdChannel: make(chan *service.CmdContext, config.GetStateCmdChannelNum()),
	}
}

// 初始化连接登陆槽
func (cs *cacheState) initLoginSlot(ctx context.Context) error {
	loginSlotRange := config.GetStateServerLoginSlotRange()
	for _, slot := range loginSlotRange {
		loginSlotKey := fmt.Sprintf(cache.LoginSlotSetKey, slot)
		// async
		go func() {
			// 这里可以使用lua脚本进行批处理
			loginSlot, err := cache.SmembersStrSlice(ctx, loginSlotKey)
			if err != nil {
				logger.CtxErrorf(ctx, "loginSlotKey=%v, err=%v", loginSlotKey, err)
				panic(err)
			}
			for _, mate := range loginSlot {
				did, connID := cs.loginSlotUnmarshal(mate)
				cs.connReLogin(ctx, did, connID)
			}
		}()
	}
	return nil
}

func (cs *cacheState) newConnState(did uint64, connID uint64) *connState {
	// create connState object
	st := &connState{
		connID: connID,
		did:    did,
	}
	// start heartbeat timer
	st.resetHeartTimer()
	return st
}

func (cs *cacheState) connLogin(ctx context.Context, did uint64, connID uint64) error {
	st := cs.newConnState(did, connID)
	// store login slot
	slotKey := cs.getLoginSlotKey(connID)
	meta := cs.loginSlotMarshal(did, connID)
	err := cache.SADD(ctx, slotKey, meta)
	if err != nil {
		logger.CtxErrorf(ctx, "[connLogin] sadd login meta into redis failed, did=%v, connID=%v, err=%v", did, connID, err)
		return err
	}

	// add router in router table
	endpoint := fmt.Sprintf("%s:%d", config.GetGatewayServiceAddr(), config.GetStateServerPort())
	err = router.AddRouter(ctx, did, endpoint, connID)
	if err != nil {
		logger.CtxErrorf(ctx, "[connLogin] add record in router table failed , did=%v, connID=%v, err=%v", did, connID, err)
		return err
	}
	//TODO 上行消息 max_client_id 初始化, 现在相当于生命周期在conn维度，后面重构sdk时会调整到会话维度

	// store local state
	cs.storeConnIDState(connID, st)
	return nil
}

func (cs *cacheState) connReLogin(ctx context.Context, did uint64, connID uint64) {
	st := cs.newConnState(did, connID)
	cs.storeConnIDState(connID, st)
	st.loadMsgTimer(ctx)
}

func (cs *cacheState) connLogout(ctx context.Context, connID uint64) (uint64, error) {
	if st, ok := cs.loadConnIDState(connID); ok {
		did := st.did
		return did, st.close(ctx)
	}
	return 0, nil
}

func (cs *cacheState) reConn(ctx context.Context, oldConnID, newConnID uint64) error {
	var (
		did uint64
		err error
	)
	if did, err = cs.connLogout(ctx, oldConnID); err != nil {
		return err
	}
	return cs.connLogin(ctx, did, newConnID) // todo 重连路由是不用更新的?
}

func (cs *cacheState) resetHeartTimer(connID uint64) {
	if st, ok := cs.loadConnIDState(connID); ok {
		st.resetHeartTimer()
	}
}

func (cs *cacheState) loadConnIDState(connID uint64) (*connState, bool) {
	if data, ok := cs.connToStateTable.Load(connID); ok {
		st, _ := data.(*connState)
		return st, true
	}
	return nil, false
}

func (cs *cacheState) deleteConnIDState(ctx context.Context, connID uint64) {
	cs.connToStateTable.Delete(connID)
}

func (cs *cacheState) storeConnIDState(connID uint64, state *connState) {
	cs.connToStateTable.Store(connID, state)
}

// 获取登陆槽位的key
func (cs *cacheState) getLoginSlotKey(connID uint64) string {
	connStateSlotList := config.GetStateServerLoginSlotRange()
	slotSize := uint64(len(connStateSlotList))
	slot := connID % slotSize
	slotKey := fmt.Sprintf(cache.LoginSlotSetKey, connStateSlotList[slot])
	return slotKey
}

func (cs *cacheState) getConnStateSLot(connID uint64) uint64 {
	connStateSlotList := config.GetStateServerLoginSlotRange()
	slotSize := uint64(len(connStateSlotList))
	slot := connID % slotSize
	return slot
}

// 使用lua实现比较并自增
func (cs *cacheState) compareAndIncrClientID(ctx context.Context, connID, oldMaxClientID uint64) bool {
	slot := cs.getConnStateSLot(connID)
	key := fmt.Sprintf(cache.MaxClientIDKey, slot, connID)
	logger.CtxInfof(ctx, "[compareAndIncrClientID] run luaInt key=%v, oldClientID=%v, connID=%v", key, oldMaxClientID, connID)

	var (
		res int
		err error
	)
	if res, err = cache.RunLuaInt(ctx, cache.LuaCompareAndIncrClientID, []string{key}, oldMaxClientID, cache.TTL7D); err != nil {
		logger.CtxErrorf(ctx, "[compareAndIncrClientID] run luaInt failed, key=%v, oldClientID=%v, connID=%v, err=%v", key, oldMaxClientID, connID, err)
		panic(err)
	}
	return res > 0
}

func (cs *cacheState) appendLastMsg(ctx context.Context, connID uint64, pushMsg *message.PushMsg) error {
	if pushMsg == nil {
		logger.CtxWarnf(ctx, "[appendLastMsg] pushMsg is nil, return")
		return nil
	}
	var (
		st *connState
		ok bool
	)
	if st, ok = cs.loadConnIDState(connID); !ok {
		logger.CtxErrorf(ctx, "[appendLastMsg] load connState from map failed, connID=%v", connID)
		return errors.New("connState is nil ")
	}
	slot := cs.getConnStateSLot(connID)
	key := fmt.Sprintf(cache.LastMsgKey, slot, connID)
	// TODO 现在假设一个链接只有一个会话，后面再讲IMserver，会进行重构
	msgTimerLock := fmt.Sprintf("%d_%d", pushMsg.SessionID, pushMsg.MsgID)
	msgData, _ := proto.Marshal(pushMsg)
	st.appendMsg(ctx, key, msgTimerLock, msgData)
	return nil
}

func (cs *cacheState) ackLastMsg(ctx context.Context, connID, sessionID, msgID uint64) {
	var (
		st *connState
		ok bool
	)
	if st, ok = cs.loadConnIDState(connID); ok {
		st.ackLastMsg(ctx, sessionID, msgID)
	}
}

func (cs *cacheState) getLastMsg(ctx context.Context, connID uint64) (*message.PushMsg, error) {
	slot := cs.getConnStateSLot(connID)
	key := fmt.Sprintf(cache.LastMsgKey, slot, connID)
	data, err := cache.GetBytes(ctx, key)
	if err != nil {
		logger.CtxErrorf(ctx, "[getLastMsg] get lastNsg from cache failed, connID = %v, err=%v", connID, err)
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	pMsg := &message.PushMsg{}
	err = proto.Unmarshal(data, pMsg)
	if err != nil {
		logger.CtxErrorf(ctx, "[getLastMsg] unmarshal data to pushMsg failed, connID = %v, data=%v, err=%v", connID, string(data), err)
		return nil, err
	}
	return pMsg, nil
}

func (cs *cacheState) loginSlotUnmarshal(mate string) (uint64, uint64) {
	strSlice := strings.Split(mate, "|")
	if len(strSlice) < 2 {
		return 0, 0
	}
	did, err := strconv.ParseUint(strSlice[0], 10, 64)
	if err != nil {
		return 0, 0
	}
	connID, err := strconv.ParseUint(strSlice[1], 10, 64)
	if err != nil {
		return 0, 0
	}
	return did, connID
}

func (cs *cacheState) loginSlotMarshal(did, connID uint64) string {
	return fmt.Sprintf("%d|%d", did, connID)
}
