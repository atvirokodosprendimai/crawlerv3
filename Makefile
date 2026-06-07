BINDIR := ./bin

.PHONY: build registry worker migrator clean

build: registry worker migrator taskworker agent embedworker ocrworker litekoworker unicrawler

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

litekoworker:
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/litekoworker ./cmd/litekoworker

unicrawler:
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/unicrawler ./cmd/unicrawler

clean:
	rm -rf $(BINDIR)
