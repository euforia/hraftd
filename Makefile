
.clean:
	rm -rf var/run/hraftd/*

.PHONY: test
test:
	go test -cover ./...
