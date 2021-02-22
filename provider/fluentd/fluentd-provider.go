package main

//go:generate protoc -I ../../proto/fluentd/ --go_out=paths=source_relative:../../proto/fluentd/ fluentd.proto

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	fluentd "github.com/synerex/proto_fluentd"
	pb "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	idlist          []uint64
	spMap           map[uint64]*sxutil.SupplyOpts
	mu              sync.Mutex
	sxServerAddress string
)

type Channel struct {
	Channel string `json:"channel"`
}

type MyFleet struct {
	VehicleId  int                    `json:"vehicle_id"`
	Status     int                    `json:"status"`
	Coord      map[string]interface{} `json:"coord"`
	Angle      float32                `json:"angle"`
	Speed      int                    `json:"speed"`
	MyServices map[string]interface{} `json"services"`
	Demands    []int                  `json:"demands"`
}

type MyVehicle struct {
	vehicles []*MyFleet `json:"vehicles"`
}

type MyJson map[string]interface{}

func init() {
	idlist = make([]uint64, 0)
	spMap = make(map[uint64]*sxutil.SupplyOpts)
}

func jsonDecode(jsonByte []byte) map[string]interface{} {
	var dt map[string]interface{}
	err := json.Unmarshal(jsonByte, &dt)
	if err == nil {
		return dt
	}
	fmt.Println("jsonDecodeErr:", err)
	return nil
}

func base64UnCompress(str string) []byte {
	data, _ := base64.StdEncoding.DecodeString(str)
	dt1, err := zlib.NewReader(bytes.NewReader(data))
	if err == nil {
		buf, err := ioutil.ReadAll(dt1)
		if err == nil {
			return buf
		} else {
			fmt.Println("base64UncompErr:", err)
		}
	} else {
		fmt.Println("base64UncompErr:", err)
	}
	return []byte(" ")
}

var totalTerminals int32
var terminals map[string]*Terminal
var amps map[string]*AMPM

type Terminal struct {
	lastTS     float64
	lastAMP    string
	AMPS       []string // slice of seen AMPs
	timestamps []string // slice of seen AMPs
	powers     []string // slice of seen AMPs
	//	oids       []string // slice of seen AMPs
	counts  []string // slice of seen AMPs
	termstr string   // infoString
	count   int32    // howmany count
}

type AMPM struct { // for signal info
	AMPname string
	lastTS  int64
	count   int32
}

func convertWiFi(dt map[string]interface{}) {
	host := dt["h"].(string)
	wifi := dt["d"].(string)

	wbases := strings.Split(wifi, "\n")
	for _, s := range wbases {
		vals := strings.Split(s, ",")
		//		log.Printf("Split into %d", len(vals))
		if len(vals) < 5 {
			continue
		}
		f, _ := strconv.ParseFloat(vals[0], 64)
		term := terminals[vals[1]] // get mac_id
		if term == nil {           // add new terminal
			terminals[vals[1]] = &Terminal{
				lastTS:     f,
				lastAMP:    host,
				AMPS:       make([]string, 0),
				timestamps: make([]string, 0),
				powers:     make([]string, 0),
				//				oids:       make([]string, 0),
				counts:  make([]string, 0),
				termstr: s,
				count:   0,
			}
			term = terminals[vals[1]]
		}

		term.lastAMP = host[9:]
		term.lastTS = f
		term.AMPS = append(term.AMPS, host[9:])
		term.timestamps = append(term.timestamps, vals[0])
		term.powers = append(term.powers, vals[2])
		//		term.oids = append(term.oids, vals[3])
		term.counts = append(term.counts, vals[4])
		term.count = term.count + 1
		totalTerminals++
		log.Printf("%v", *term)
	}

	log.Printf("WiFi from %s wifi [%d] terminal count %d/%d\n", host, len(wbases)-1, len(terminals), totalTerminals)
}

func checkAMPM() string {
	tm := time.Now().Unix()
	st := make([]string, 0)

	for n, v := range amps {
		nm := n[10:] // slice from "AMPM18-HZ0XX"
		if tm-v.lastTS < 10 {
			st = append(st, nm)
		}
	}
	sort.Slice(st, func(i, j int) bool {
		ii, _ := strconv.Atoi(st[i])
		jj, _ := strconv.Atoi(st[j])
		return ii < jj
	})

	return fmt.Sprintf("%d/%d %v", len(st), len(amps), st)

}

// callback for each Supply
func supplyCallback(clt *sxutil.SXServiceClient, sp *pb.Supply) {
	// check if demand is match with my supply.
	//	log.Println("Got Fluentd Supply callback")

	record := &fluentd.FluentdRecord{}
	err := proto.Unmarshal(sp.Cdata.Entity, record)

	if err == nil {
		//		log.Println("Got record:", record.Tag, record.Time)
		recordStr := *(*string)(unsafe.Pointer(&(record.Record)))
		replaced := strings.Replace(recordStr, "=>", ":", 1)
		dt0 := jsonDecode([]byte(replaced))
		if dt0 != nil {
			buf := base64UnCompress(dt0["m"].(string))
			if len(buf) > 1 {
				dt := jsonDecode(buf)
				if dt != nil {
					if record.Tag == "ampsense.pack.test.signal" {
						//				log.Printf("ID:%v, %v, %v", dt["a"], dt["ts"], dt["g"])
						ampName := dt["a"].(string)
						amp := amps[ampName]
						if amp == nil {
							amps[ampName] = &AMPM{
								AMPname: ampName,
								count:   0,
							}
							amp = amps[ampName]
						}
						amp.lastTS = time.Now().Unix()
						amp.count++
						sxutil.SetNodeStatus(int32(len(terminals)), checkAMPM())
					} else if record.Tag == "ampsense.pack.packet.test" {
						//						log.Printf("packet:%v\n", dt)
						convertWiFi(dt)
					} else { // unknown data.
						log.Printf("UNmarshal Result: %s, %v\n", record.Tag, dt)
					}
				}
			}
		}
	}

}

func subscribeSupply(client *sxutil.SXServiceClient) {
	// goroutine!
	ctx := context.Background() //
	client.SubscribeSupply(ctx, supplyCallback)
	// comes here if channel closed
	log.Printf("Server closed... on fluentd provider")
}

func main() {
	terminals = make(map[string]*Terminal)
	amps = make(map[string]*AMPM)
	totalTerminals = 0

	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{pbase.FLUENTD_SERVICE}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(*nodesrv, "FluentAMP-Checker", channelTypes, nil)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	wg := sync.WaitGroup{} // for syncing other goroutines
	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJson := fmt.Sprintf("{Client:Fluentd}")
	sclient := sxutil.NewSXServiceClient(client, pbase.FLUENTD_SERVICE, argJson)

	wg.Add(1)
	subscribeSupply(sclient)

	wg.Wait()
	sxutil.CallDeferFunctions() // cleanup!

}
