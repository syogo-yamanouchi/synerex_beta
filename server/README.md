# synerex_server
Synerex Server

# build with docker

```
docker build ./ -t synerex_server
docker run --tty  --name synerex_server --rm -v $PWD:/go/src/github.com/synerex_server synerex_server
```
