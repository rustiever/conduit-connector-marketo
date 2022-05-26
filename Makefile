.PHONY: build test test-integration

build:
	go build -o marketo cmd/marketo/main.go

test:
	# -p 1 ensures that tests run one at a time.
	# Each test cases takes upto 1-2 minutes to finish, since marketo API is slow. Also it takes a while to prepare snapshots by Marketo. So 30 Mins as overall timeout period.
	# But most of the time tests will finish in 10-15 mins.
	go test $(GOTEST_FLAGS) -p 1 -timeout 30m -v -race ./...
