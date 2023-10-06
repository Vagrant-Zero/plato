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
	eChan chan *net.TCPConn
	eSize int
	done  chan struct{}

	ln *net.TCPListener
	f  func(conn *net.TCPConn, ep *epoller) // callback func
}

func InitEpoll(ln *net.TCPListener, f func(conn *net.TCPConn, ep *epoller)) {
	setLimit()
	ep = NewEpoll(ln, f)
	ep.createAcceptProcess()
	ep.startEPoll()
}

func NewEpoll(ln *net.TCPListener, callback func(conn *net.TCPConn, ep *epoller)) *ePool {
	return &ePool{
		eChan: make(chan *net.TCPConn, config.GetGatewayEpollerChanNum()),
		done:  make(chan struct{}),
		eSize: config.GetGatewayEpollerNum(),
		ln:    ln,
		f:     callback,
	}
}

// createAcceptProcess 创建一个专门处理 accept 事件的协程，与当前cpu的核数对应，能够发挥最大功效
func (ep *ePool) createAcceptProcess() {
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			ctx := context.Background()
			for {
				conn, err := ep.ln.AcceptTCP()
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
				ep.addTask(conn)
			}
		}()
	}
}

func (ep *ePool) startEPoll() {
	for i := 0; i < ep.eSize; i++ {
		go ep.startEProc(i)
	}
}

// startEProc 轮询器池 处理器
// epoll 的监听和处理逻辑
func (ep *ePool) startEProc(id int) {
	epl, err := NewEpoller(id)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	// listen events
	go func() {
		for {
			select {
			case <-ep.done:
				return
			case conn := <-ep.eChan:
				// supply logs in below method
				addTcpNum()
				if addConnErr := epl.add(conn); addConnErr != nil {
					logger.CtxErrorf(ctx, "epoll_%d failed to add connection %v\n", id, err)
					_ = conn.Close()
					continue
				}
				logger.CtxInfof(ctx, "EpollerPool new connection[%v], epoll_%d, tcpSize:%d", (conn).RemoteAddr().String(), id, tcpNum)
			}
		}
	}()

	// wait for events
	for {
		select {
		case <-ep.done:
			return
		default:
			connections, waitErr := epl.Wait(200) // 200ms 一次轮询避免 忙轮询
			if waitErr != nil && waitErr != syscall.EINTR {
				logger.CtxErrorf(ctx, "failed to epoll_%d wait %v\n", id, waitErr)
				continue
			}

			for _, conn := range connections {
				if conn == nil {
					break
				}
				ep.f(conn, epl)
			}
		}
	}
}

func (ep *ePool) addTask(conn *net.TCPConn) {
	ep.eChan <- conn
}

// epoller 对象 轮询器
type epoller struct {
	connections sync.Map
	fd          int
	id          int // id of epoll
}

func NewEpoller(id int) (*epoller, error) {
	fd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &epoller{
		connections: sync.Map{},
		fd:          fd,
		id:          id,
	}, nil
}

func (epl *epoller) add(conn *net.TCPConn) error {
	fd := socketFD(conn)
	// the fd of epl is the epoll fd
	// the fd of conn is the connection fd
	err := unix.EpollCtl(epl.fd, syscall.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLHUP, Fd: int32(fd)})
	if err != nil {
		return err
	}
	epl.connections.Store(fd, conn)
	return nil
}

func (epl *epoller) remove(conn *net.TCPConn) error {
	subTcpNum()
	fd := socketFD(conn)
	err := unix.EpollCtl(epl.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		return err
	}
	epl.connections.Delete(fd)
	return nil
}

func (epl *epoller) Wait(millSec int) ([]*net.TCPConn, error) {
	events := make([]unix.EpollEvent, config.GetGatewayEpollWaitQueueSize())
	n, err := unix.EpollWait(epl.fd, events, millSec)
	if err != nil {
		return nil, err
	}
	var connections []*net.TCPConn
	for i := 0; i < n; i++ {
		if conn, ok := epl.connections.Load(int(events[i].Fd)); ok {
			connections = append(connections, conn.(*net.TCPConn))
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
