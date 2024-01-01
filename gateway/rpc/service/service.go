package service

import "context"

const (
	DelConnCmd = 1 // DelConn
	PushCmd    = 2 //push
)

type CmdContext struct {
	Ctx     *context.Context
	Cmd     int32
	ConnID  uint64
	PayLoad []byte
}

type Service struct {
	UnimplementedGatewayServer
	CmdChannel chan *CmdContext
}

func (s *Service) DelConn(ctx context.Context, gr *GatewayRequest) (*GatewayResponse, error) {
	c := context.TODO()
	s.CmdChannel <- &CmdContext{
		Ctx:    &c,
		Cmd:    DelConnCmd,
		ConnID: gr.GetConnID(),
	}
	return &GatewayResponse{
		Code: 0,
		Msg:  "success",
	}, nil
}

func (s *Service) Push(ctx context.Context, gr *GatewayRequest) (*GatewayResponse, error) {
	c := context.TODO()
	s.CmdChannel <- &CmdContext{
		Ctx:     &c,
		Cmd:     PushCmd,
		ConnID:  gr.GetConnID(),
		PayLoad: gr.GetData(),
	}
	return &GatewayResponse{
		Code: 0,
		Msg:  "success",
	}, nil
}
