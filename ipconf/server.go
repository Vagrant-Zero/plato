package ipconf

import (
	"github.com/hardcore-os/plato/ipconf/domain"
	"github.com/hardcore-os/plato/ipconf/source"

	"github.com/cloudwego/hertz/pkg/app/server"
)

func RunMain() {
	// 启动数据源
	source.Init()
	// 初始化调度层
	domain.Init()
	// 启动web容器
	s := server.Default(server.WithHostPorts(":6789"))
	s.GET("/ip/list", GetIpInfoList)
	s.Spin()
}
