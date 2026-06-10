APP     = serial-enricher
VERSION = $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS = -ldflags "-s -w -X main.version=$(VERSION)"

## Build for the current machine
build:
	go build $(LDFLAGS) -o $(APP) .

## Build for Linux x86-64 (deploy to a Linux server)
build-linux:
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(APP)-linux-amd64 .

## Build for Windows x86-64
build-windows:
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(APP)-windows-amd64.exe .

## Download dependencies
deps:
	go mod tidy

## Run locally (config.json must be next to the binary after build)
run: build
	./$(APP)

## Run with a custom config path
run-cfg: build
	./$(APP) $(CFG)

.PHONY: build build-linux build-windows deps run run-cfg
