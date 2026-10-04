COVERAGE_DIR ?= .coverage.out

.PHONY: test test-with-flags

test:
	@mkdir -p "$(COVERAGE_DIR)"
	$(MAKE) test-with-flags TEST_FLAGS='-v -race -covermode atomic -coverprofile "$(COVERAGE_DIR)/combined.txt" -bench=. -benchmem -timeout 20m'

test-with-flags:
	@go test $(TEST_FLAGS) ./...
