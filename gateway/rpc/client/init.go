package client

import (
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
		stateClient = service.NewStateClient(pCli.Conn())
	})
}
