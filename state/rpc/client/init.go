package client

import (
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
	gatewayClient = service.NewGatewayClient(pCli.Conn())
}
