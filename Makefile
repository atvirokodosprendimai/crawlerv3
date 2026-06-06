BINDIR := ./bin

.PHONY: build registry worker migrator clean

build: registry worker migrator taskworker agent embedworker ocrworker

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

ocrworker:
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/ocrworker ./cmd/ocrworker

clean:
	rm -rf $(BINDIR)
