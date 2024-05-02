package router

import (
	"context"
	"fmt"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/common/cache"
	"strconv"
	"strings"
	"time"
)

const (
	gatewayRouterKey = "gateway_router_%d" // router key
	ttl7D            = 7 * 24 * 60 * 60 * time.Second
)

type Record struct {
	Endpoint string
	ConnID   uint64
}

func Init(ctx context.Context) {
	cache.InitRedis(ctx)
}

func AddRouter(ctx context.Context, did uint64, endpoint string, connID uint64) error {
	key := fmt.Sprintf(gatewayRouterKey, did)
	value := fmt.Sprintf("%s-%d", endpoint, connID)
	return cache.SetString(ctx, key, value, ttl7D)
}

func DelRouter(ctx context.Context, did uint64) error {
	key := fmt.Sprintf(gatewayRouterKey, did)
	return cache.Del(ctx, key)
}

func QueryRecord(ctx context.Context, did uint64) (*Record, error) {
	key := fmt.Sprintf(gatewayRouterKey, did)
	data, err := cache.GetString(ctx, key)
	if err != nil {
		// data can not be nil when err is not nil
		return nil, err
	}
	ec := strings.Split(data, "-")
	connID, err := strconv.ParseUint(ec[1], 10, 64)
	if err != nil {
		logger.CtxErrorf(ctx, "QueryRecord when transfer connID failed, data=%v, err=%v", data, err)
		return nil, err
	}
	return &Record{
		Endpoint: ec[0],
		ConnID:   connID,
	}, nil
}
