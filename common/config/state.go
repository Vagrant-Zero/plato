package config

import "github.com/spf13/viper"

func GetStateCmdChannelNum() int {
	return viper.GetInt("message.cmd_channel_num")
}

func GetStateServiceAddr() string {
	return viper.GetString("message.servide_addr")
}

func GetStateServiceName() string {
	return viper.GetString("message.service_name")
}

func GetStateServerPort() int {
	return viper.GetInt("message.server_port")
}

func GetStateRPCWeight() int {
	return viper.GetInt("message.weight")
}
