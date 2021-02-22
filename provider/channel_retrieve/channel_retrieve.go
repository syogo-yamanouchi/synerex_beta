package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"

	pb "github.com/synerex/synerex_api"
	sxutil "github.com/synerex/synerex_sxutil"
)

// datastore provider provides Datastore Service.

type DataStore interface {
	store(str string)
}

var (
	nodesrv   = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	channel   = flag.String("channel", "3", "Retrieving channel type(default 3, support comma separated)")
	local     = flag.String("local", "", "Specify Local Synerex Server")
	sendfile  = flag.String("sendfile", "", "Sending file name") // only one file
	startDate = flag.String("startDate", "02-07", "Specify Start Date")
	endDate   = flag.String("endDate", "12-31", "Specify End Date")
	startTime = flag.String("startTime", "00:00", "Specify Start Time")
	endTime   = flag.String("endTime", "24:00", "Specify End Time")
	dir       = flag.String("dir", "store", "Directory of data storage") // for all file
	all       = flag.Bool("all", true, "Send all file in data storage")  // for all file
	verbose   = flag.Bool("verbose", false, "Verbose information")
	jst       = flag.Bool("jst", false, "Run/display with JST Time")
	recTime   = flag.Bool("recTime", false, "Send with recorded time")
	speed     = flag.Float64("speed", 1.0, "Speed of sending packets(default real time =1.0), minus in msec")
	skip      = flag.Int("skip", 0, "Skip lines(default 0)")
)

func init() {

}

const dateFmt = "2006-01-02T15:04:05.999Z"

func atoUint(s string) uint32 {
	r, err := strconv.Atoi(s)
	if err != nil {
		log.Print("err", err)
	}
	return uint32(r)
}

func getHourMin(dt string) (hour int, min int) {
	st := strings.Split(dt, ":")
	hour, _ = strconv.Atoi(st[0])
	min, _ = strconv.Atoi(st[1])
	return hour, min
}

func getMonthDate(dt string) (month int, date int) {
	st := strings.Split(dt, "-")
	month, _ = strconv.Atoi(st[0])
	date, _ = strconv.Atoi(st[1])
	return month, date
}

func NotifySupplyWithTime(clt *sxutil.SXServiceClient, smo *sxutil.SupplyOpts, ts *timestamp.Timestamp) (uint64, error) {
	id := sxutil.GenerateIntID()
	dm := pb.Supply{
		Id:          id,
		SenderId:    uint64(clt.ClientID),
		ChannelType: clt.ChannelType,
		SupplyName:  smo.Name,
		Ts:          ts,
		ArgJson:     smo.JSON,
		Cdata:       smo.Cdata,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//	resp , err := clt.Client.NotifySupply(ctx, &dm)

	_, err := clt.SXClient.Client.NotifySupply(ctx, &dm)
	if err != nil {
		log.Printf("Error for sending:NotifySupply to Synerex Server as %v ", err)
		return 0, err
	}
	//	log.Println("RegiterSupply:", smo, resp)
	smo.ID = id // assign ID
	return id, nil
}

// sending People Counter File.
func sendingStoredFile(clients map[uint32]*sxutil.SXServiceClient) {
	// file
	fp, err := os.Open(*sendfile)
	if err != nil {
		panic(err)
	}
	defer fp.Close()

	scanner := bufio.NewScanner(fp) // csv reader
	var buf []byte = make([]byte, 1024)
	scanner.Buffer(buf, 1024*1024*64) // 64Mbytes buffer

	last := time.Now()
	started := false // start flag
	stHour, stMin := getHourMin(*startTime)
	edHour, edMin := getHourMin(*endTime)
	skipCount := 0

	if *verbose {
		log.Printf("Verbose output for file %s", *sendfile)
		log.Printf("StartTime %02d:%02d  -- %02d:%02d", stHour, stMin, edHour, edMin)
	}
	jstZone := time.FixedZone("Asia/Tokyo", 9*60*60)

	for scanner.Scan() { // read one line.
		if *skip != 0 { // if there is skip  , do it first
			skipCount++
			if skipCount < *skip {
				continue
			}
			log.Printf("Skip %d:", *skip)
			skipCount = 0
		}

		dt := scanner.Text()
		//		if *verbose {
		//			log.Printf("Scan:%s", dt)
		//		}

		token := strings.Split(dt, ",")
		//		log.Printf("dt:%d, token %d", len(dt), len(token))

		//                                    , 0  ,1    ,2          ,3           ,4              ,5            ,6           , 7        ,8
		//Sprintf("%s,%d,%d,%d,%d,%s,%s,%d,%s", ts, sm.Id, sm.SenderId, sm.TargetId, sm.ChannelType, sm.SupplyName, sm.ArgJson, sm.MbusId, bsd)

		tm, err := time.Parse(dateFmt, token[0]) // RFC3339Nano
		if err != nil {
			log.Printf("Time parsing error with %s, %s : %v", token[0], dt, err)
		}

		if *jst { // we need to convert UTC to JST.
			tm = tm.In(jstZone)
		}

		//		tp, _ := ptypes.TimestampProto(tm)
		sDec, err2 := base64.StdEncoding.DecodeString(token[8])
		if err2 != nil {
			log.Printf("Decoding error with %s : %v", token[8], err)
		}

		if !started {
			if (tm.Hour() > stHour || (tm.Hour() == stHour && tm.Minute() >= stMin)) &&
				(tm.Hour() < edHour || (tm.Hour() == edHour && tm.Minute() <= edMin)) {
				started = true
				log.Printf("Start output! %v", tm)
			} else {
				continue // skip all data
			}
		} else {
			if tm.Hour() > edHour || (tm.Hour() == edHour && tm.Minute() > edMin) {
				started = false
				log.Printf("Stop  output! %v", tm)
				continue
			}
		}

		if !started {
			continue // skip following
		}

		{ // sending each packets
			cont := pb.Content{Entity: sDec}
			smo := sxutil.SupplyOpts{
				Name:  token[5],
				JSON:  token[6],
				Cdata: &cont,
			}

			tsProto, _ := ptypes.TimestampProto(tm)

			// if channel in channels
			chnum, err := strconv.Atoi(token[4])
			client, ok := clients[uint32(chnum)]
			if ok && err == nil { // if there is channel
				_, nerr := NotifySupplyWithTime(client, &smo, tsProto)
				if nerr != nil {
					log.Printf("Send Fail!%v", nerr)
				} else {
					//				log.Printf("Sent OK! %#v\n", smo)
					log.Printf("ts:%s,chan:%s,%s,%s,%s,len:%d", tm.Format(time.RFC3339), token[4], token[5], token[6], token[7], len(token[8]))

				}
			}
			if *speed < 0 { // sleep for each packet
				time.Sleep(time.Duration(-*speed) * time.Millisecond)
			}

		}

		dur := tm.Sub(last)

		if dur.Nanoseconds() > 0 {
			if *speed > 0 {
				time.Sleep(time.Duration(float64(dur.Nanoseconds()) / *speed))
			}
			last = tm
		}
		if dur.Nanoseconds() < 0 {
			last = tm
		}
	}

	serr := scanner.Err()
	if serr != nil {
		log.Printf("Scanner error %v", serr)
	}

}

func sendAllStoredFile(clients map[uint32]*sxutil.SXServiceClient) {
	// check all files in dir.
	stMonth, stDate := getMonthDate(*startDate)
	edMonth, edDate := getMonthDate(*endDate)

	if *dir == "" {
		log.Printf("Please specify directory")
		data := "data"
		dir = &data
	}
	files, err := ioutil.ReadDir(*dir)

	if err != nil {
		log.Printf("Can't open diretory %v", err)
		os.Exit(1)
	}
	// should be sorted.
	var ss = make(sort.StringSlice, 0, len(files))

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".csv") { // check is CSV file
			//
			fn := file.Name()
			var year, month, date int
			ct, err := fmt.Sscanf(fn, "%4d-%02d-%02d.csv", &year, &month, &date)
			if (month > stMonth || (month == stMonth && date >= stDate)) &&
				(month < edMonth || (month == edMonth && date <= edDate)) {
				ss = append(ss, file.Name())
			} else {
				log.Printf("file: %d %v %s: %04d-%02d-%02d", ct, err, fn, year, month, date)
			}
		}
	}

	ss.Sort()

	for _, fname := range ss {
		dfile := path.Join(*dir, fname)
		// check start date.

		log.Printf("Sending %s", dfile)
		sendfile = &dfile
		sendingStoredFile(clients)
	}

}

//dataServer(pc_client)

func main() {
	log.Printf("ChannelRetrieve(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	// check channel types.
	//
	channelTypes := []uint32{}
	chans := strings.Split(*channel, ",")
	for _, ch := range chans {
		v, err := strconv.Atoi(ch)
		if err == nil {
			channelTypes = append(channelTypes, uint32(v))
		} else {
			log.Fatal("Can't convert channels ", *channel)
		}
	}

	srv, rerr := sxutil.RegisterNode(*nodesrv, fmt.Sprintf("ChannelRetrieve[%s]", *channel), channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		srv = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", srv)

	//	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(srv)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	} else {
		log.Print("Connecting SynerexServer")
	}

	// we need to add clients for each channel:
	pcClients := map[uint32]*sxutil.SXServiceClient{}

	for _, chnum := range channelTypes {
		argJson := fmt.Sprintf("{ChannelRetrieve[%d]}", chnum)
		pcClients[chnum] = sxutil.NewSXServiceClient(client, chnum, argJson)
	}

	if *sendfile != "" {
		//		for { // infinite loop..
		sendingStoredFile(pcClients)
		//		}
	} else if *all { // send all file
		sendAllStoredFile(pcClients)
	} else if *dir != "" {
	}

	//	wg.Wait()

}
