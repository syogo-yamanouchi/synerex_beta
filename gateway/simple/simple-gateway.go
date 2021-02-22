package main

import (
	"flag"
	"log"
	"strings"
	"sync"

	api "github.com/synerex/synerex_api"
	"github.com/synerex/synerex_nodeapi"
	sxutil "github.com/synerex/synerex_sxutil"
	"golang.org/x/net/context"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	gateway         = flag.String("gateway", "0,1", "Speficy Synerex Server IDs(ordered)")
	readOnly        = flag.Bool("ro", false, "Read Only flag")
	name            = flag.String("name", "SimpleGW", "GW Name")
	servers         = flag.String("servers", "", "Speficy Synerex Server names")
	idlist          []uint64
	spMap           map[uint64]*sxutil.SupplyOpts
	mu              sync.Mutex
	sxServerAddress string
)

func forwardGatewayMsg(sg api.Synerex_SubscribeGatewayClient, client api.SynerexClient) {
	ctx := context.Background() //
	for {
		msg, err := sg.Recv()
		if err == nil {
			client.ForwardToGateway(ctx, msg)
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
		ServerInfo: *gateway,
		NodeType:   synerex_nodeapi.NodeType_GATEWAY,
		ClusterId:  0,
		AreaId:     "Default",
		GwInfo:     *servers,
	}

	channelTypes := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	// obtain synerex server address from nodeserv
	srvs, err := sxutil.RegisterNode(*nodesrv, *name, channelTypes, sxo)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Servers [%s]\n", srvs)
	servers := strings.Split(srvs, ",")

	wg := sync.WaitGroup{} // for syncing other goroutines
	client0 := sxutil.GrpcConnectServer(servers[0])

	channels := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8}

	gi := &api.GatewayInfo{
		ClientId:    sxutil.GenerateIntID(),        // new client_ID
		GatewayType: api.GatewayType_BIDIRECTIONAL, /// default
		Channels:    channels,
	}
	ctx := context.Background() //
	sg0, err := client0.SubscribeGateway(ctx, gi)
	if err != nil {
		log.Printf("Synerex subscribe Error %v\n", err)
	}

	if len(servers) > 1 {
		// we should check duplicated servers.
		if servers[0] == servers[1] {
			log.Printf("Duplicated server!")
		} else {
			client1 := sxutil.GrpcConnectServer(servers[1])
			sg1, err1 := client1.SubscribeGateway(ctx, gi)
			if err1 != nil {
				log.Printf("Synerex subscribe Error to %s %v\n", servers[1], err1)
			}
			wg.Add(2)
			go forwardGatewayMsg(sg1, client0)
			go forwardGatewayMsg(sg0, client1)
		}
	} else {
		for {
			msg, _ := sg0.Recv()
			log.Printf("Recv:%v\n", msg)
		}
	}

	wg.Wait()
	sxutil.CallDeferFunctions() // cleanup!

}
