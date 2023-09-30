package discovery

import (
	"context"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/common/config"
	"go.etcd.io/etcd/clientv3"
)

type ServiceRegister struct {
	cli     *clientv3.Client
	leaseID clientv3.LeaseID
	// 租约keepAlive响应chan
	KeepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	Key           string
	Value         string
	ctx           *context.Context
}

func NewServiceRegister(ctx *context.Context, key string, info *EndpointInfo, lease int64) (*ServiceRegister, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.GetEndpointsForDiscovery(),
		DialTimeout: config.GetTimeoutForDiscovery(),
	})
	if err != nil {
		logger.Fatal(err)
	}

	ser := &ServiceRegister{
		cli:   cli,
		Key:   key,
		Value: info.Marshal(),
		ctx:   ctx,
	}

	// 申请租约设置时间keepalive
	if err := ser.putKeyWithLease(lease); err != nil {
		return nil, err
	}

	return ser, nil
}

// putKeyWithLease 设置族月
func (ser *ServiceRegister) putKeyWithLease(lease int64) error {
	// 设置租约时间
	resp, err := ser.cli.Grant(*ser.ctx, lease)
	if err != nil {
		return err
	}
	// 注册服务并绑定租约
	_, err = ser.cli.Put(*ser.ctx, ser.Key, ser.Value, clientv3.WithLease(resp.ID))
	if err != nil {
		return err
	}
	// 设置续租 定期发送需求请求
	leaseRespChan, err := ser.cli.KeepAlive(*ser.ctx, resp.ID)
	if err != nil {
		return err
	}
	ser.leaseID = resp.ID
	ser.KeepAliveChan = leaseRespChan
	return nil
}

func (ser *ServiceRegister) UpdateValue(value *EndpointInfo) error {
	val := value.Marshal()
	_, err := ser.cli.Put(*ser.ctx, ser.Key, val, clientv3.WithLease(ser.leaseID))
	if err != nil {
		return err
	}
	ser.Value = val
	logger.CtxInfof(*ser.ctx, "ServiceRegister.updateValue leaseID=%d Put key=%s,val=%s, success!", ser.leaseID, ser.Key, ser.Value)
	return nil
}

// ListenLeaseRespChan 监听 续租情况
func (ser *ServiceRegister) ListenLeaseRespChan() {
	for leaseKeepResp := range ser.KeepAliveChan {
		logger.CtxInfof(*ser.ctx, "lease success leaseID:%d, Put key:%s,val:%s reps:+%v",
			ser.leaseID, ser.Key, ser.Value, leaseKeepResp)
	}
	logger.CtxInfof(*ser.ctx, "lease failed !!!  leaseID:%d, Put key:%s,val:%s", ser.leaseID, ser.Key, ser.Value)
}

// Close 注销服务
func (ser *ServiceRegister) Close() error {
	// 撤销租约
	if _, err := ser.cli.Revoke(*ser.ctx, ser.leaseID); err != nil {
		return err
	}
	logger.CtxInfof(*ser.ctx, "lease close !!!  leaseID:%d, Put key:%s,val:%s  success!", ser.leaseID, ser.Key, ser.Value)
	return ser.cli.Close()
}
