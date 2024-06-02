# 使用官方的 Go 镜像作为基础镜像
FROM golang:1.20 AS builder

# 设置工作目录
WORKDIR /app

# 拷贝 Go 项目代码到容器中
COPY . .

# 为 Linux 构建 plato 可执行文件
# 为了在mac上能够执行，指定了 GOOS=linux GOARCH=amd64
# 其他linux环境编译时不需要添加该命令，直接 go build -o plato . 即可
RUN GOOS=linux GOARCH=amd64 go build -o plato .


# 创建最终的生产环境镜像
FROM alpine:latest

# 设置工作目录
WORKDIR /app

# 拷贝构建好的可执行文件到生产镜像中
COPY --from=builder /app/plato .

# 赋予 plato-gateway 和 plato-state 可执行权限
RUN #chmod +x ./plato-gateway && chmod +x ./plato-state
RUN chmod +x ./plato

# 拷贝项目的配置文件到容器中
COPY ./plato.yaml .

# 声明 gateway 服务运行时监听的端口
EXPOSE 8900
EXPOSE 8901

# 声明 state 服务运行时监听的端口
EXPOSE 8902

# 启动应用程序
#CMD ["./plato gateway"]
