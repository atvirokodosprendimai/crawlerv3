BINDIR := ./bin

.PHONY: build registry worker migrator clean

build: registry worker migrator

registry:
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/registry ./cmd/registry

worker:
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/worker ./cmd/worker

migrator:
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/migrator ./cmd/migrator

clean:
	rm -rf $(BINDIR)
