# Makefile for Synerex Server

GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
RM=rm

LDFLAGS="-X github.com/synerex/synerex_sxutil.GitVer=`git describe --tag` -X github.com/synerex/synerex_sxutil.buildTime=`date +%Y-%m-%d_%T` -X github.com/synerex/synerex_sxutil.Sha1Ver=`git rev-parse HEAD`"



TARGET=synerex-server
# Main target

.PHONY: build 
build: $(TARGET)

synerex-server: synerex-server.go synerex-server_linux.go message-store.go
	$(GOBUILD)  -ldflags $(LDFLAGS)

.PHONY: clean
clean: 
	$(RM) $(TARGET)
