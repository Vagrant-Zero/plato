package ipconf

import (
	"context"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/hardcore-os/plato/ipconf/domain"
)

type Response struct {
	Message string      `json:"message"`
	Code    int         `json:"code"`
	Data    interface{} `json:"data"`
}

func GetIpInfoList(ctx context.Context, appCtx *app.RequestContext) {
	defer func() {
		if r := recover(); r != nil {
			appCtx.JSON(consts.StatusBadRequest, utils.H{"err": r})
		}
	}()
	// 构建客户端请求
	ipConfCtx := domain.BuildIpConfContext(&ctx, appCtx)
	// ip调度
	eds := domain.Dispatch(ipConfCtx)
	//获取分数top5返回
	ipConfCtx.AppCtx.JSON(consts.StatusOK, packRes(top5Endports(eds)))
}
