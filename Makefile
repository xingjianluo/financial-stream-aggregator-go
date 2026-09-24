.PHONY: test race vet fmt build

build:
	go build ./...

test:
	go test -v ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l .
