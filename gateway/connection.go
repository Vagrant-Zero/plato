package gateway

import (
	"net"
)

type connection struct {
	fd   int
	conn *net.TCPConn
}

func (c *connection) Close() error {
	return c.conn.Close()
}

func (c *connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}
