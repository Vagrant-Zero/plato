package config

import (
	"github.com/spf13/viper"
	"time"
)

func Init(path string) {
	viper.SetConfigFile(path)
	viper.SetConfigType("yaml")
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}
}

// GetEndpointsForDiscovery 获取服务发现地 址
func GetEndpointsForDiscovery() []string {
	return viper.GetStringSlice("discovery.endpoints")
}

// GetTimeoutForDiscovery 获取连接服务发现集群的超时时间 单位微秒
func GetTimeoutForDiscovery() time.Duration {
	return viper.GetDuration("discovery.timeout") * time.Second
}

func GetServicePathForIPConf() string {
	return viper.GetString("ip_conf.service_path")
}

func GetCacheRedisEndpointList() []string {
	return viper.GetStringSlice("cache.redis.endpoints")
}

// IsDebug 判断是不是debug环境
func IsDebug() bool {
	env := viper.GetString("global.env")
	return env == "debug"
}
