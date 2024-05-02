package config

import (
	"github.com/spf13/viper"
	"strconv"
	"strings"
)

func GetStateCmdChannelNum() int {
	return viper.GetInt("state.cmd_channel_num")
}

func GetStateServiceAddr() string {
	return viper.GetString("state.servide_addr")
}

func GetStateServiceName() string {
	return viper.GetString("state.service_name")
}

func GetStateServerPort() int {
	return viper.GetInt("state.server_port")
}

func GetStateRPCWeight() int {
	return viper.GetInt("state.weight")
}

var connStateSlotList []int

func GetStateServerLoginSlotRange() []int {
	if len(connStateSlotList) != 0 {
		return connStateSlotList
	}
	slotRnageStr := viper.GetString("state.conn_state_slot_range")
	slotRnage := strings.Split(slotRnageStr, ",")
	left, err := strconv.Atoi(slotRnage[0])
	if err != nil {
		panic(err)
	}
	right, err := strconv.Atoi(slotRnage[1])
	if err != nil {
		panic(err)
	}
	res := make([]int, right-left+1)
	for i := left; i <= right; i++ {
		res[i] = i
	}
	connStateSlotList = res
	return connStateSlotList
}

func GetStateServerGatewayServerEndpoint() string {
	return viper.GetString("state.gateway_server_endpoint")
}
