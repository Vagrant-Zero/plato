package gateway

import (
	"context"
	"fmt"
	"github.com/hardcore-os/plato/common/config"
	"golang.org/x/sys/unix"
	"net"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/bytedance/gopkg/util/logger"
)

// 全局对象
var (
	ep     *ePool // epoll池
	tcpNum int32  // 当前服务允许接入的最大tcp连接数
)

type ePool struct {
	eChan  chan *connection
	tables sync.Map
	eSize  int
	done   chan struct{}

	ln *net.TCPListener
	f  func(conn *connection, ep *epoller) // callback func
}

func InitEpoll(ln *net.TCPListener, f func(conn *connection, ep *epoller)) {
	setLimit()
	ep = NewEpoll(ln, f)
	ep.createAcceptProcess()
	ep.startEPoll()
}

func NewEpoll(ln *net.TCPListener, callback func(conn *connection, ep *epoller)) *ePool {
	return &ePool{
		eChan:  make(chan *connection, config.GetGatewayEpollerChanNum()),
		done:   make(chan struct{}),
		eSize:  config.GetGatewayEpollerNum(),
		tables: sync.Map{},
		ln:     ln,
		f:      callback,
	}
}

// createAcceptProcess 创建一个专门处理 accept 事件的协程，与当前cpu的核数对应，能够发挥最大功效
func (e *ePool) createAcceptProcess() {
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			ctx := context.Background()
			for {
				conn, err := e.ln.AcceptTCP()
				// 限流熔断
				if !checkTcp() {
					_ = conn.Close()
					continue
				}
				setTcpConfig(conn)
				if err != nil {
					if ne, ok := err.(net.Error); ok && ne.Temporary() {
						logger.CtxErrorf(ctx, "accept temp err: %v", ne)
						continue
					}
					fmt.Errorf("accept error: %v", err)
				}
				c := NewConnection(conn)
				ep.addTask(c)
			}
		}()
	}
}

func (e *ePool) startEPoll() {
	for i := 0; i < e.eSize; i++ {
		go e.startEProc(i)
	}
}

// startEProc 轮询器池 处理器
// epoll 的监听和处理逻辑
func (e *ePool) startEProc(id int) {
	epl, err := NewEpoller(id)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	// listen events
	go func() {
		for {
			select {
			case <-e.done:
				return
			case conn := <-e.eChan:
				// supply logs in below method
				addTcpNum()
				if addConnErr := epl.add(conn); addConnErr != nil {
					logger.CtxErrorf(ctx, "epoll_%d failed to add connection %v\n", id, addConnErr)
					_ = conn.Close()
					continue
				}
				logger.CtxInfof(ctx, "EpollerPool new connection[%v], epoll_%d, tcpSize:%d", conn.RemoteAddr(), id, tcpNum)
			}
		}
	}()

	// wait for events
	for {
		select {
		case <-e.done:
			return
		default:
			connections, waitErr := epl.wait(200) // 200ms 一次轮询避免 忙轮询
			if waitErr != nil && waitErr != syscall.EINTR {
				logger.CtxErrorf(ctx, "failed to epoll_%d wait %v\n", id, waitErr)
				continue
			}

			for _, conn := range connections {
				if conn == nil {
					break
				}
				e.f(conn, epl)
			}
		}
	}
}

func (e *ePool) addTask(conn *connection) {
	e.eChan <- conn
}

// epoller 对象 轮询器
type epoller struct {
	fd            int
	fdToConnTable sync.Map
	id            int // id of epoll
}

func NewEpoller(id int) (*epoller, error) {
	fd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &epoller{
		fd: fd,
		id: id,
	}, nil
}

// TODO: 默认水平触发模式,可采用非阻塞FD,优化边沿触发模式
func (epl *epoller) add(conn *connection) error {
	// Extract file descriptor associated with the connection
	fd := conn.fd
	// the fd of epl is the epoll fd
	// the fd of conn is the connection fd
	err := unix.EpollCtl(epl.fd, syscall.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLHUP, Fd: int32(fd)})
	if err != nil {
		return err
	}
	// epoll实例存储 fd -> conn 的映射
	epl.fdToConnTable.Store(conn.fd, conn)
	// 全局epoll池存储 connID -> conn的映射
	ep.tables.Store(conn.id, conn)
	// 每个连接绑定所属的epoll实例
	conn.BindEpoller(epl)
	return nil
}

func (epl *epoller) remove(conn *connection) error {
	subTcpNum()
	fd := conn.fd
	err := unix.EpollCtl(epl.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		return err
	}
	ep.tables.Delete(conn.id)
	epl.fdToConnTable.Delete(conn.fd)
	return nil
}

func (epl *epoller) wait(millSec int) ([]*connection, error) {
	events := make([]unix.EpollEvent, config.GetGatewayEpollWaitQueueSize())
	n, err := unix.EpollWait(epl.fd, events, millSec)
	if err != nil {
		return nil, err
	}
	var connections []*connection
	for i := 0; i < n; i++ {
		// 单个epoll实例是根据fd来反查conn的
		// 全局的conn是根据epoll池中的connID来查询的
		if conn, ok := epl.fdToConnTable.Load(int(events[i].Fd)); ok {
			connections = append(connections, conn.(*connection))
		}
	}
	return connections, nil
}

func socketFD(conn *net.TCPConn) int {
	tcpConn := reflect.Indirect(reflect.ValueOf(*conn).FieldByName("conn"))
	fdVal := tcpConn.FieldByName("fd")
	pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")
	return int(pfdVal.FieldByName("Sysfd").Int())
}

func setLimit() {
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		panic(err)
	}
	rLimit.Cur = rLimit.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		panic(err)
	}
	logger.CtxInfof(context.Background(), "set cur limit: %d", rLimit.Cur)
}

func addTcpNum() {
	atomic.AddInt32(&tcpNum, 1)
}

func getTcpNum() int32 {
	return atomic.LoadInt32(&tcpNum)
}

func subTcpNum() {
	atomic.AddInt32(&tcpNum, -1)
}

func checkTcp() bool {
	num := getTcpNum()
	maxTcpNum := config.GetGatewayMaxTcpNum()
	return num <= maxTcpNum
}

func setTcpConfig(c *net.TCPConn) {
	_ = c.SetKeepAlive(true)
}
