package main

// package main

// Federation gateway
// open REST port for federation.

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	gosocketio "github.com/mtfelian/golang-socketio"
	api "github.com/synerex/synerex_api"
	"github.com/synerex/synerex_nodeapi"
	sxutil "github.com/synerex/synerex_sxutil"
	"golang.org/x/net/context"
)

// This gateway is a template code for various federation.
// You may copy this file to create your own federation gateway.

var (
	nodesrv = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	//	port    = flag.Int("rest_port", 10099, "federation port")
	name            = flag.String("name", "Federation-Gateway", "Name of Fedration Gateway")
	server          = flag.String("server", "", "Speficy Synerex Server name")
	port            = flag.Int("port", 10070, "Federataion Gateway API Port")
	version         = "0.01"
	assetsDir       http.FileSystem
	idlist          []uint64
	spMap           map[uint64]*sxutil.SupplyOpts
	mu              sync.Mutex
	sxServerAddress string
	ioserv          *gosocketio.Server
)

// assetsFileHandler for HTTP static Data
func assetsFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return
	}
	file := r.URL.Path
	//	log.Printf("Open File '%s'",file)
	if file == "/" {
		file = "/index.html"
	}
	f, err := assetsDir.Open(file)
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		return
	}
	http.ServeContent(w, r, file, fi.ModTime(), f)
}

func apiHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodPost {
		return
	}
	urlPath := r.URL.Path
	err := r.ParseForm()
	if err != nil {
		log.Printf("API request from %s: %s: %v", r.Host, r.Method, r.Form)

	} else {
		log.Printf("API request from %s: %s , URL %v with Error %v", r.Host, r.Method, urlPath, err)
	}

}

func run_server() *gosocketio.Server {

	currentRoot, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	d := filepath.Join(currentRoot, "static")

	assetsDir = http.Dir(d)
	log.Println("AssetDir:", assetsDir)

	assetsDir = http.Dir(d)
	server := gosocketio.NewServer()

	server.On(gosocketio.OnConnection, func(c *gosocketio.Channel) {
		log.Printf("Connected from %s as %s", c.IP(), c.Id())
		// do something.
		// we can add authentication.

	})

	server.On(gosocketio.OnDisconnection, func(c *gosocketio.Channel) {
		log.Printf("Disconnected from %s as %s", c.IP(), c.Id())
	})

	return server
}

func listenGatewayMsg(sg api.Synerex_SubscribeGatewayClient) {
	//	ctx := context.Background() //
	for {
		msg, err := sg.Recv()
		if err == nil {
			log.Printf("Receive Gateway message :%v", msg)

			mu.Lock()
			//			ioserv.BroadcastToAll("data", mm.GetJson())
			mu.Unlock()

		} else {
			log.Printf("Error on gateway receive! :%v", err)
		}
	}
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	sxo := &sxutil.SxServerOpt{
		ServerInfo: "",
		NodeType:   synerex_nodeapi.NodeType_GATEWAY,
		ClusterId:  0,
		AreaId:     "Default",
		GwInfo:     *server,
	}

	channelTypes := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	// obtain synerex server address from nodeserv
	srvs, err := sxutil.RegisterNode(*nodesrv, *name, channelTypes, sxo)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	if len(srvs) == 0 {
		log.Printf("Please specify synerex server name through -server.")

	} else {
		log.Printf("Connecting Servers [%s]\n", srvs)
		servers := strings.Split(srvs, ",")

		wg := sync.WaitGroup{} // for syncing other goroutines
		client0 := sxutil.GrpcConnectServer(servers[0])

		channels := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

		gi := &api.GatewayInfo{
			ClientId:    sxutil.GenerateIntID(),        // new client_ID
			GatewayType: api.GatewayType_BIDIRECTIONAL, /// default
			Channels:    channels,
		}
		ctx := context.Background() //
		sg0, err := client0.SubscribeGateway(ctx, gi)
		if err != nil {
			log.Printf("Synerex subscribe Error %v\n", err)
		} else {

			// start simple HTTP API server.
			ioserv = run_server()
			log.Printf("Starting Federation Gateway..\n")
			if ioserv == nil {
				fmt.Printf("Can't run websocket server.\n")
				os.Exit(1)
			}

			serveMux := http.NewServeMux()
			serveMux.Handle("/socket.io/", ioserv)
			serveMux.HandleFunc("/", assetsFileHandler)
			serveMux.HandleFunc("/api/", apiHandler)

			log.Printf("Starting Federation Gateway %s  on port %d", version, *port)
			err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", *port), serveMux)
			if err != nil {
				log.Fatal(err)
			}

			wg.Add(1)
			go listenGatewayMsg(sg0)
		}

		wg.Wait()
	}
	sxutil.CallDeferFunctions() // cleanup!

}
