ci: lint test
.PHONY: ci

mocks:
	@echo "--- build all the mocks"
	@go run github.com/golang/mock/mockgen -destination=mocks/s3api.go -package=mocks github.com/wolfeidau/s3iofs S3API
.PHONY: mocks

lint: 
	@echo "--- lint all the things"
	@golangci-lint run
.PHONY: lint

test:
	@echo "--- test all the things"
	@go test -coverprofile=coverage.txt ./...
	@go tool cover -func=coverage.txt
.PHONY: test
