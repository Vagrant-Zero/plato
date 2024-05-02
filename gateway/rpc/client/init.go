package client

import (
	"fmt"
	"github.com/hardcore-os/plato/common/config"
	"github.com/hardcore-os/plato/common/prpc"
	"github.com/hardcore-os/plato/state/rpc/service"
	"sync"
)

var (
	stateClient service.StateClient
	once        sync.Once
)

func Init() {
	initStateClient()
}

func initStateClient() {
	once.Do(func() {
		pCli, err := prpc.NewPClient(config.GetStateServiceName())
		if err != nil {
			panic(err)
		}
		cli, err := pCli.DialByEndPoint(config.GetGatewayStateServerEndPoint())
		if err != nil {
			panic(fmt.Sprintf("gateway.state client DialByEndPoint fialed, err=%v", err))
		}
		stateClient = service.NewStateClient(cli)
	})
}
