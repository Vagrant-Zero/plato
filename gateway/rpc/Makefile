all: gateway

gateway:
	@echo "begin"
	protoc -I service --go_out=service service/*.proto
	@echo "*.pb.go success"
	protoc -I service --go-grpc_out=service service/*.proto
	@echo "*_grpc.pb.go success"
	@echo "end"
