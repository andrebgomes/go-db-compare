.PHONY: all

all: test build

FORCE: ;
	
build:
	go mod tidy
	go build -o ./bin/compare main.go

help:
	./bin/compare -h

test:
	go test ./... -cover -count=1