#!/bin/bash

# 停止并删除现有的容器
docker-compose down

# 清理网络状态
docker network prune -f

# 重新启动服务
docker-compose up