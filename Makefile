all:build

build:
	@echo "go build plato begin"
	go build -o plato .
	@echo "go build plato success"
