BINDIR := ./bin

.PHONY: build registry worker migrator clean

build: registry worker migrator taskworker agent embedworker

registry:
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/registry ./cmd/registry

worker:
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/worker ./cmd/worker

migrator:
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/migrator ./cmd/migrator

embedworker:
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/embedworker ./cmd/embedworker


agent:
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/agent ./cmd/agent

taskworker:
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/taskworker ./cmd/taskworker

clean:
	rm -rf $(BINDIR)
