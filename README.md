# Synerex Beta Readme
Service Integration Platform for Smart Cities and Society 5.0

## Introduction
Synerex beta is a beta version of Synergic Exchange and its supporting systems.
This project is partially supported by JST MIRAI, NICT Grant, NEDO Grant, and MIC SCOPE.

This repository includes following git repositories by submodules.
- [synerex_api](https://github.com/synerex/synerex_api)
- [synerex_nodeapi](https://github.com/synerex/synerex_nodeapi)
- [synerex_nodeserv](https://github.com/synerex/synerex_nodeserv)
- [synerex_server](https://github.com/synerex/synerex_server)
- [synerex_sxutil](https://github.com/synerex/synerex_sxutil)
- [synerex_gateway](https://github.com/synerex/synerex_gateway)
- [synerex_proto](https://github.com/synerex/synerex_proto)
- [synerex_provider](https://github.com/synerex/synerex_provider)

## Requirements
go 1.13 or later (we use go.mod files for module dependencies)
nodejs(10.13.0) / npm(6.4.1) / yarn(1.12.1) for web client development.

## Installation
### Unix/Linux/Mac OS
Following step will git clone all sub-modules and update, and build basic softwares.

    git clone https://github.com/synerex/synerex_beta.git
    cd synerex_beta
    make
    
### Windows
In the most of directories(modules), build.bat is prepared.

    git clone https://github.com/synerex/synerex_beta.git
    cd synerex_beta
    git submodule update --init --recursive

    cd nodeserv
    build

    cd ../server
    build

### for Providers

    cd ../provider/fleet
    build

    cd ../provider/map
    build

    cd ../provider/harmovis
    go build

### for Gateways

    cd ../gateway/simple
    go build

### Run with docker
``` shell
# create network
docker network create synerex_net

# build image
docker-compose build

# run docker-compose
docker-compose up
```
