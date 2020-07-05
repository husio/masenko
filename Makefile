VERSION=$(shell git rev-list -1 HEAD)

all: help

build-%:
	CGO_ENABLED=0 go build -o bin/${*} -ldflags "-X main.GitCommit=$(VERSION)" github.com/husio/masenko/cmd/${*}

inlineasset:
	CGO_ENABLED=0 go install github.com/husio/masenko/cmd/inlineasset

dockerize:
	docker build -t "masenko:${VERSION}" -t "masenko:latest" .

help:
	@echo
	@echo "Commands"
	@echo "========"
	@echo
	@sed -n '/^[a-zA-Z0-9_-]*:/s/:.*//p' < Makefile | grep -v -E 'default|help.*' | sort

.PHONY: dockerize help
