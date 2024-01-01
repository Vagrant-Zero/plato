all: idl

idl:
	@echo "begin"
	protoc -I message --go_out=message message/*.proto
	@echo "*.pb.go success"
	@echo "end"