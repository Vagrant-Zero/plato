package discovery

import (
	"context"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/hardcore-os/plato/common/config"
	"go.etcd.io/etcd/clientv3"
	"sync"
)

// ServiceDiscovery 服务发现
type ServiceDiscovery struct {
	cli  *clientv3.Client //etcd client
	lock sync.Mutex
	ctx  *context.Context
}

// NewServiceDiscovery  新建发现服务
func NewServiceDiscovery(ctx *context.Context) *ServiceDiscovery {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.GetEndpointsForDiscovery(),
		DialTimeout: config.GetTimeoutForDiscovery(),
	})
	if err != nil {
		// log.error and then exist with 1
		logger.Fatal(err)
	}
	return &ServiceDiscovery{
		cli: cli,
		ctx: ctx,
	}
}

func (s *ServiceDiscovery) WatchService(prefix string, set, del func(key, value string)) error {
	// 根据前缀获取现有的key
	resp, err := s.cli.Get(*s.ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for _, kv := range resp.Kvs {
		set(string(kv.Key), string(kv.Value))
	}
	// 监视前缀，修改变更的server
	s.watcher(prefix, set, del)
	return nil
}

func (s *ServiceDiscovery) watcher(prefix string, set, del func(key, value string)) {
	rch := s.cli.Watch(*s.ctx, prefix, clientv3.WithPrefix())
	logger.CtxInfof(*s.ctx, "watching prefix:%s now...", prefix)
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT: //修改或者新增
				set(string(ev.Kv.Key), string(ev.Kv.Value))
			case mvccpb.DELETE: //删除
				del(string(ev.Kv.Key), string(ev.Kv.Value))
			}
		}
	}
}

func (s *ServiceDiscovery) Close() error {
	return s.cli.Close()
}
