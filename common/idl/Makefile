all: idl

idl:
	@echo "begin"
	protoc -I state --go_out=state state/*.proto
	@echo "*.pb.go success"
	@echo "end"