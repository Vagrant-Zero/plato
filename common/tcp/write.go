package tcp

import "net"

func SendData(conn *net.TCPConn, data []byte) error {
	totalLen := len(data)
	writeLen := 0
	for {
		length, err := conn.Write(data[writeLen:])
		if err != nil {
			return err
		}
		writeLen += length
		if writeLen >= totalLen {
			break
		}
	}
	return nil
}
