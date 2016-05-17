SHELL = /bin/bash

.clean:
	rm -rf ./hraft/n*

.PHONY: test
test:
	go test -cover ./...
