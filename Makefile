VERSION=$(shell git rev-list -1 HEAD)

all: help

build-%:
	CGO_ENABLED=0 go build -o bin/${*} -ldflags "-X main.GitCommit=$(VERSION)" github.com/husio/masenko/cmd/${*}

dockerize:
	docker build -t "masenko:${VERSION}" -t "masenko:latest" .

docs:
	. .venv/bin/activate; cd docs; $(MAKE) html

test-go:
	go test -race github.com/husio/masenko/...

test-python: build-masenko
	. .venv/bin/activate; cd clients/python; python -m unittest

run-dev:
	@# https://github.com/cespare/reflex
	reflex -G '.*' -G '*.yml' -G 'clients/*' -s -- sh -c 'go generate ./... && go run github.com/husio/masenko/cmd/masenko'

prometheus-server:
	docker run --rm -it --network host -v $(PWD)/etc/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus


help:
	@echo
	@echo "Commands"
	@echo "========"
	@echo
	@sed -n '/^[a-zA-Z0-9_-]*:/s/:.*//p' < Makefile | grep -v -E 'default|help.*' | sort

.PHONY: dockerize help docs
