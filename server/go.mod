module synerex-server

go 1.13

require (
	github.com/golang/protobuf v1.4.3 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/mattn/go-colorable v0.1.4 // indirect
	github.com/mattn/go-isatty v0.0.11 // indirect
	github.com/mgutz/ansi v0.0.0-20170206155736-9520e82c474b // indirect
	github.com/onsi/ginkgo v1.11.0 // indirect
	github.com/onsi/gomega v1.8.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0
	github.com/shirou/gopsutil v3.20.11+incompatible // indirect
	github.com/sirupsen/logrus v1.4.2
	github.com/synerex/synerex_api v0.4.2
	github.com/synerex/synerex_nodeapi v0.5.4
	github.com/synerex/synerex_proto v0.1.9
	github.com/synerex/synerex_sxutil v0.6.2
	github.com/x-cray/logrus-prefixed-formatter v0.5.2
	golang.org/x/net v0.0.0-20201207224615-747e23833adb // indirect
	golang.org/x/sys v0.0.0-20201207223542-d4d67f95c62d // indirect
	golang.org/x/text v0.3.4 // indirect
	golang.org/x/xerrors v0.0.0-20191204190536-9bdfabe68543 // indirect
	google.golang.org/genproto v0.0.0-20201207150747-9ee31aac76e7 // indirect
	google.golang.org/grpc v1.34.0
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v2 v2.2.7 // indirect
)

//replace github.com/synerex/synerex_sxutil => ../sxutil
