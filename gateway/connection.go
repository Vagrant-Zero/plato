package gateway

import (
	"errors"
	"net"
	"sync"
	"time"
)

//var nextConnID uint64 // 全局ConnID

var node *ConnIDGenerator

const (
	version      = uint64(0) // 版本控制
	sequenceBits = uint64(16)

	maxSequence = int64(-1) ^ (int64(-1) << sequenceBits) // 低sequenceBits个bit位都为1，其他高位都为0

	timeLeft    = uint8(16) // timeLeft = sequence // 时间戳向左偏移量
	versionLeft = uint8(63) // 左移动到最高位
	// 2020-05-20 08:00:00 +0800 CST
	twepoch = int64(1589923200000) // 常量时间戳(毫秒)
)

type ConnIDGenerator struct {
	mu        sync.Mutex
	LastStamp int64 // 记录上一次ID的时间戳
	Sequence  int64 // 当前毫秒已经生成的ID序列号(从0 开始累加) 1毫秒内最多生成2^16个ID
}

type connection struct {
	id   uint64 // 进程级别的生命周期
	fd   int
	e    *epoller
	conn *net.TCPConn
}

func init() {
	node = &ConnIDGenerator{}
}

func (c *ConnIDGenerator) getMilliSeconds() int64 {
	return time.Now().UnixMilli()
}

// NextID 这里的锁会自旋，不会多么影响性能，主要是临界区小
func (c *ConnIDGenerator) NextID() (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nextID()
}

func (c *ConnIDGenerator) nextID() (uint64, error) {
	timestamp := c.getMilliSeconds()
	if timestamp < c.LastStamp {
		return 0, errors.New("time is moving backwards, waiting until")
	}

	if c.LastStamp == timestamp {
		c.Sequence = (c.Sequence + 1) & maxSequence
		if c.Sequence == 0 { // 如果这里发生溢出，就等到下一个毫秒时再分配，这样就一定不出现重复
			for timestamp <= c.LastStamp {
				timestamp = c.getMilliSeconds()
			}
		}
	} else { // 如果与上次分配的时间戳不等，则为了防止可能的时钟飘移现象，就必须重新计数
		c.Sequence = 0
	}
	c.LastStamp = timestamp
	// 减法可以压缩一下时间戳
	id := ((timestamp - twepoch) << timeLeft) | c.Sequence
	connID := uint64(id) | (version << versionLeft)
	return connID, nil
}

func NewConnection(conn *net.TCPConn) *connection {
	var (
		id  uint64
		err error
	)
	if id, err = node.NextID(); err != nil {
		panic(err) //在线服务需要解决这个问题 ，报错而不能panic
	}
	return &connection{
		id:   id,
		fd:   socketFD(conn),
		conn: conn,
	}
}

func (c *connection) Close() error {
	ep.tables.Delete(c.id)
	if c.e != nil {
		c.e.fdToConnTable.Delete(c.fd)
	}
	err := c.conn.Close()
	panic(err)
}

func (c *connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *connection) BindEpoller(e *epoller) {
	c.e = e
}
