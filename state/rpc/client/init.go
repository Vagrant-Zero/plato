package client

import (
	"fmt"
	"github.com/hardcore-os/plato/common/config"
	"github.com/hardcore-os/plato/common/prpc"
	"github.com/hardcore-os/plato/gateway/rpc/service"
)

var gatewayClient service.GatewayClient

func Init() {
	initGatewayClient()
}

func initGatewayClient() {
	pCli, err := prpc.NewPClient(config.GetGatewayServiceName())
	if err != nil {
		panic(err)
	}
	conn, err := pCli.DialByEndPoint(config.GetStateServerGatewayServerEndpoint())
	if err != nil {
		panic(fmt.Sprintf("state.gateway client DialByEndPoint fialed, err=%v", err))
	}
	gatewayClient = service.NewGatewayClient(conn)
}
