package main

//go:generate protoc -I ../api --go_out=paths=source_relative,plugins=grpc:../api ../api/synerex.proto

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	api "github.com/synerex/synerex_api"
	nodeapi "github.com/synerex/synerex_nodeapi"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"

	"github.com/rcrowley/go-metrics"

	"google.golang.org/grpc"
)

const MessageChannelBufferSize = 100

var (
	port      = flag.Int("port", getServerPort(), "The Synerex Server Listening Port")
	servaddr  = flag.String("servaddr", getServerHostName(), "Server Address for Other Providers")
	nodeport  = flag.Int("nodeport", getNodeservPort(), "The Node ID Server Listening Port")
	nodeaddr  = flag.String("nodeaddr", getNodeservHostName(), "Node ID Server Address")
	name      = flag.String("name", getServerName(), "Server Name for Other Providers")
	isMetrics = flag.Bool("metrics", getIsMetrics(), "Expose Server Metrics")
	//	log       = logrus.New() // for default logging
	server_id uint64
	sinfo     *synerexServerInfo
)

//type sxutil.IDType uint64

type synerexServerInfo struct {
	demandChans             [pbase.ChannelTypeMax][]chan *api.Demand // create slices for each ChannelType(each slice contains channels)
	supplyChans             [pbase.ChannelTypeMax][]chan *api.Supply
	mbusChans               map[uint64][]chan *api.MbusMsg                           // Private Message bus for each provider
	mbusMap                 map[sxutil.IDType]map[uint64]chan *api.MbusMsg           // map from sxutil.IDType to Mbus channel
	demandMap               [pbase.ChannelTypeMax]map[sxutil.IDType]chan *api.Demand // map from sxutil.IDType to Demand channel
	supplyMap               [pbase.ChannelTypeMax]map[sxutil.IDType]chan *api.Supply // map from sxutil.IDType to Supply channel
	waitConfirms            [pbase.ChannelTypeMax]map[sxutil.IDType]chan *api.Target // confirm maps
	gatewayMap              map[sxutil.IDType]chan *api.GatewayMsg                   // for gateway. (//TODO: should use channels)
	dmu, smu, mmu, wmu, gmu sync.RWMutex
	messageStore            *MessageStore // message store
}

// for metrics
var (
	totalMessages   = metrics.NewCounter()
	receiveMessages = metrics.NewCounter()
	sendMessages    = metrics.NewCounter()
	mbusMessages    = metrics.NewCounter()
)

func getServerHostName() string {
	env := os.Getenv("SX_SERVER_HOST")
	if env != "" {
		return env
	} else {
		return "127.0.0.1"
	}
}

func getNodeservHostName() string {
	env := os.Getenv("SX_NODESERV_HOST")
	if env != "" {
		return env
	} else {
		return "127.0.0.1"
	}
}

func getServerPort() int {
	env := os.Getenv("SX_SERVER_PORT")
	if env != "" {
		env, _ := strconv.Atoi(env)
		return env
	} else {
		return 10000
	}
}

func getNodeservPort() int {
	env := os.Getenv("SX_NODESERV_PORT")
	if env != "" {
		env, _ := strconv.Atoi(env)
		return env
	} else {
		return 9990
	}
}

func getServerName() string {
	env := os.Getenv("SX_SERVER_NAME")
	if env != "" {
		return env
	} else {
		return "SynerexServer"
	}
}

func getIsMetrics() bool {
	env := os.Getenv("SX_SERVER_METRICS")
	if env == "false" {
		return false
	} else {
		return true
	}
}

func init() {
	//	sxutil.InitNodeNum(0)

	// for Logrus initialization
	//	log.Formatter = new(logprefix.TextFormatter)
	//	log.Level = logrus.DebugLevel // TODO: Should we change this by flag?

	//	log.Printf("Initialized!")

	if *isMetrics {
		log.Printf("Register Metrics")
		// for metrics initialization
		metrics.Register("messages.total", totalMessages)
		metrics.Register("messages.receive", receiveMessages)
		metrics.Register("messages.send", sendMessages)
		metrics.Register("messages.mbus", mbusMessages)

		// log -> syslog
		InitMetricsLog()
	}

}

func sendDemand(s *synerexServerInfo, dm *api.Demand, isGateway bool) (okFlag bool, okMsg string) {
	okFlag = true
	okMsg = ""
	totalMessages.Inc(1)
	receiveMessages.Inc(1)
	s.dmu.RLock()
	chs := s.demandChans[dm.GetChannelType()]
	for i := range chs {
		ch := chs[i]
		if len(ch) < MessageChannelBufferSize { // performance trouble?
			totalMessages.Inc(1)
			sendMessages.Inc(1)
			ch <- dm
		} else {
			okFlag = false
			okMsg = fmt.Sprintf("SendDemand MessageDrop %v", dm)
			log.Printf(okMsg)
		}
	}
	s.dmu.RUnlock()
	if len(s.gatewayMap) > 0 && !isGateway {
		gm := &api.GatewayMsg{
			SrcSynerexId: server_id,
			MsgType:      api.MsgType_DEMAND,
			MsgOneof:     &api.GatewayMsg_Demand{Demand: dm},
		}
		s.gmu.RLock()
		for _, gch := range s.gatewayMap { // TODO: may performance check!
			totalMessages.Inc(1)
			sendMessages.Inc(1)
			gch <- gm
		}
		s.gmu.RUnlock()
	}

	return okFlag, okMsg
}

// Implementation of each Protocol API
func (s *synerexServerInfo) NotifyDemand(c context.Context, dm *api.Demand) (r *api.Response, e error) {
	// send demand for desired channels
	okFlag, okMsg := sendDemand(s, dm, false)
	r = &api.Response{Ok: okFlag, Err: okMsg}
	return r, nil
}

func sendSupply(s *synerexServerInfo, sp *api.Supply, isGateway bool) (okFlag bool, okMsg string) {
	okFlag = true
	okMsg = ""
	s.smu.RLock()
	totalMessages.Inc(1)
	receiveMessages.Inc(1)
	chs := s.supplyChans[sp.GetChannelType()]
	for i := range chs {
		ch := chs[i]
		if len(ch) < MessageChannelBufferSize { // run under not blocking state.
			totalMessages.Inc(1)
			sendMessages.Inc(1)
			ch <- sp
		} else {
			okMsg = fmt.Sprintf("SendSupply MessageDrop %v", sp)
			okFlag = false
			log.Printf(okMsg)
		}
	}
	s.smu.RUnlock()
	if len(s.gatewayMap) > 0 && !isGateway {
		gm := &api.GatewayMsg{
			SrcSynerexId: server_id,
			MsgType:      api.MsgType_SUPPLY,
			MsgOneof:     &api.GatewayMsg_Supply{Supply: sp},
		}
		s.gmu.RLock()
		for _, gch := range s.gatewayMap { // TODO: may performance check!
			totalMessages.Inc(1)
			sendMessages.Inc(1)
			gch <- gm
		}
		s.gmu.RUnlock()
	}
	return okFlag, okMsg

}

func (s *synerexServerInfo) NotifySupply(c context.Context, sp *api.Supply) (r *api.Response, e error) {
	//	fmt.Printf("Notify Supply!!!")
	ctype := sp.GetChannelType()
	if ctype == 0 || ctype >= pbase.ChannelTypeMax {
		log.Printf("ChannelType Error! %d", ctype)
		r = &api.Response{Ok: false, Err: "ChannelType Error"}
		return r, errors.New("ChannelType Error")
	}
	okFlag, okMsg := sendSupply(s, sp, false)
	r = &api.Response{Ok: okFlag, Err: okMsg}
	return r, nil
}

func (s *synerexServerInfo) ProposeDemand(c context.Context, dm *api.Demand) (r *api.Response, e error) {
	ctype := dm.GetChannelType()
	if ctype == 0 || ctype >= pbase.ChannelTypeMax {
		log.Printf("ChannelType Error! %d", ctype)
		r = &api.Response{Ok: false, Err: "ChannelType Error"}
		return r, errors.New("ChannelType Error")
	}

	okFlag, okMsg := sendDemand(s, dm, false)
	r = &api.Response{Ok: okFlag, Err: okMsg}
	return r, nil
}
func (s *synerexServerInfo) ProposeSupply(c context.Context, sp *api.Supply) (r *api.Response, e error) {
	ctype := sp.GetChannelType()
	if ctype == 0 || ctype >= pbase.ChannelTypeMax {
		log.Printf("ChannelType Error! %d", ctype)
		r = &api.Response{Ok: false, Err: "ChannelType Error"}
		return r, errors.New("ChannelType Error")
	}
	okFlag, okMsg := sendSupply(s, sp, false)
	r = &api.Response{Ok: okFlag, Err: okMsg}
	return r, nil
}

func (s *synerexServerInfo) SelectSupply(c context.Context, tg *api.Target) (r *api.ConfirmResponse, e error) {
	targetSender := s.messageStore.getSrcId(tg.GetTargetId()) // find source from Id
	ctype := tg.GetChannelType()
	if ctype == 0 || ctype >= pbase.ChannelTypeMax {
		log.Printf("ChannelType Error! %d", ctype)
		r = &api.ConfirmResponse{Ok: false, Err: "ChannelType Error"}
		return r, errors.New("ChannelType Error")
	}
	s.dmu.RLock()
	// find subscribe demand with sender
	ch, ok := s.demandMap[ctype][sxutil.IDType(targetSender)]
	s.dmu.RUnlock()
	if !ok {
		//TODO: there might be packet through gateway...
		if len(s.gatewayMap) == 0 {
			r = &api.ConfirmResponse{Ok: false, Err: "Can't find demand target from SelectSupply"}
			log.Printf("Can't find SelectSupply target ID %d, src %d", tg.GetTargetId(), targetSender)
			e = errors.New("Cant find channel in SelectSupply")
			return
		} else {
			// TODO: implement select for gateway!
			return
		}
	}
	id := sxutil.GenerateIntID()
	dm := &api.Demand{
		Id:          id, // generate ID from synerex server
		SenderId:    tg.SenderId,
		TargetId:    tg.TargetId,
		ChannelType: tg.ChannelType,
		MbusId:      id, // mbus id is a message id for select.
	}
	//
	//	args := idToNode(tg.SenderId) + "->" + idToNode(tg.TargetId)
	//	go monitorapi.SendMessage("ServSelSupply", int(tg.Type), dm.Id, tg.SenderId, tg.TargetId, tg.TargetId, args)

	tch := make(chan *api.Target)
	s.wmu.Lock()
	s.waitConfirms[tg.ChannelType][sxutil.IDType(id)] = tch
	s.wmu.Unlock()

	ch <- dm // send select message

	// wait for confim...
	select {

	case tb := <-tch: // got confirm!
		s.wmu.Lock() // remove waitChannel
		delete(s.waitConfirms[tg.ChannelType], sxutil.IDType(id))
		s.wmu.Unlock()
		//		args := idToNode(tg.SenderId) + "->" + idToNode(tg.TargetId)
		//		go monitorapi.SendMessage("gotConfirm", int(tg.Type), dm.Id, tb.SenderId, tb.TargetId, tb.TargetId, args)

		if tb.TargetId == id {
			if tb.MbusId == id {
				r = &api.ConfirmResponse{Ok: true, Err: "", MbusId: id}
				return r, nil
			} else {
				r = &api.ConfirmResponse{Ok: true, Err: "no mbus id"}
				return r, nil
			}
		}

	case <-time.After(30 * time.Second): // timeout! // todo: reconsider expiration time.
		//		args := idToNode(tg.SenderId) + "->" + idToNode(tg.TargetId)
		//		go monitorapi.SendMessage("notConfirm", int(tg.Type), dm.Id, tg.SenderId, tg.TargetId, tg.TargetId, args)
		r = &api.ConfirmResponse{Ok: false, Err: "waitConfirm Timeout!"}

	}

	return r, errors.New("Should not happen")

}

func (s *synerexServerInfo) SelectDemand(c context.Context, tg *api.Target) (r *api.ConfirmResponse, e error) {
	// select!
	// TODO: not yet implemented...

	r = &api.ConfirmResponse{Ok: true, Err: ""}
	return r, nil
}

func (s *synerexServerInfo) Confirm(c context.Context, tg *api.Target) (r *api.Response, e error) {
	// check waitConfirms
	s.wmu.RLock()
	ch, ok := s.waitConfirms[tg.ChannelType][sxutil.IDType(tg.TargetId)]
	s.wmu.RUnlock()
	//	go monitorapi.SendMessage("ServConfirm", int(tg.ChannelType), tg.Id, tg.SenderId, 0, tg.TargetId, "ConfirmTo")
	if !ok {
		ss := fmt.Sprintf("Can't find targetID %d in channel %d",tg.TargetId ,tg.ChannelType)
		log.Print(ss)
		r = &api.Response{Ok: false, Err: ss}
		return r, errors.New(ss)
	}
	ch <- tg // send OK
	r = &api.Response{Ok: true, Err: ""}
	return r, nil
}

// go routine which wait demand channel and sending demands to each providers.
func demandServerFunc(ch chan *api.Demand, stream api.Synerex_SubscribeDemandServer, id sxutil.IDType, chnum uint32) error {
	for dm := range ch { // block until receiving info
		err := stream.Send(dm)
		if err != nil {
			log.Printf("Error in DemandServer Error %v", err)
			return err
		}
	}
	log.Printf("SubscribeDemand for Client node %v Channel %d is closed.", id, chnum)
	return nil
}

// remove channel from slice

func removeDemandChannelFromSlice(sl []chan *api.Demand, c chan *api.Demand) []chan *api.Demand {
	for i, ch := range sl {
		if ch == c {
			return append(sl[:i], sl[i+1:]...)
		}
	}
	log.Printf("Cant find channel %v in removeChannel", c)
	return sl
}

func removeSupplyChannelFromSlice(sl []chan *api.Supply, c chan *api.Supply) []chan *api.Supply {
	for i, ch := range sl {
		if ch == c {
			return append(sl[:i], sl[i+1:]...)
		}
	}
	log.Printf("Cant find channel %v in removeChannel", c)
	return sl
}

// SubscribeDemand is called form client to subscribe channel
func (s *synerexServerInfo) SubscribeDemand(ch *api.Channel, stream api.Synerex_SubscribeDemandServer) error {
	// TODO: we can check the duplication of node id here! (especially 1024 snowflake node ID)
	idt := sxutil.IDType(ch.GetClientId())
	s.dmu.Lock()
	_, ok := s.demandMap[ch.ChannelType][idt]
	if ok { // check the availability of duplicated client ID
		s.dmu.Unlock()
		return fmt.Errorf("duplicated SubscribeDemand ClientID %d", idt)
	}

	log.Printf("Subscribe Demand Type:%d, From: %x %s", ch.ChannelType, ch.ClientId, ch.ArgJson)
	// It is better to logging here.
	//	monitorapi.SendMes(&monitorapi.Mes{Message:"Subscribe Demand", Args: fmt.Sprintf("Type:%d,From: %x  %s",ch.Type,ch.ClientId, ch.ArgJson )})
	//	monitorapi.SendMessage("SubscribeDemand", int(ch.Type), 0, ch.ClientId, 0, 0, ch.ArgJson)

	subCh := make(chan *api.Demand, MessageChannelBufferSize)
	// We should think about thread safe coding.
	tp := ch.GetChannelType()
	s.demandChans[tp] = append(s.demandChans[tp], subCh)
	s.demandMap[tp][idt] = subCh // mapping from clientID to channel
	s.dmu.Unlock()
	demandServerFunc(subCh, stream, idt, tp) // infinite go routine?
	// if this returns, stream might be closed.
	// we should remove channel

	s.dmu.Lock()
	_, ok = s.demandMap[tp][idt]
	if ok {
		delete(s.demandMap[tp], idt) // remove map from idt
		s.demandChans[tp] = removeDemandChannelFromSlice(s.demandChans[tp], subCh)
		log.Printf("Remove Demand Stream Channel %v", ch)
	}
	s.dmu.Unlock()
	return nil
}

// This function is created for each subscribed provider
// This is not efficient if the number of providers increases.
func supplyServerFunc(ch chan *api.Supply, stream api.Synerex_SubscribeSupplyServer, idt sxutil.IDType, chnum uint32) error {
	for sp := range ch { // block until receiving info
		err := stream.Send(sp)
		if err != nil {
			log.Printf("Error in SupplyServer Error %v", err)
			log.Printf("SubscribeSupply for Client node %v Channel %d is closed.", idt, chnum)
			return err
		}
	}
	log.Printf("SubscribeSupply for Client node %v Channel %d is closed.", idt, chnum)
	return nil
}

func (s *synerexServerInfo) SubscribeSupply(ch *api.Channel, stream api.Synerex_SubscribeSupplyServer) error {
	idt := sxutil.IDType(ch.GetClientId())
	tp := ch.GetChannelType()
	s.smu.Lock()
	_, ok := s.supplyMap[tp][idt]
	if ok { // check the availability of duplicated client ID
		s.smu.Unlock()
		return errors.New(fmt.Sprintf("duplicated SubscribeSupply for ClientID %v", idt))
	}

	subCh := make(chan *api.Supply, MessageChannelBufferSize)

	log.Printf("Subscribe Supply Channel:%d, Node:%d Args: %s", ch.ChannelType, ch.ClientId, ch.ArgJson)
	//	monitorapi.SendMes(&monitorapi.Mes{Message:"Subscribe Supply", Args: fmt.Sprintf("Type:%d, From: %x %s",ch.Type,ch.ClientId,ch.ArgJson )})
	//	monitorapi.SendMessage("SubscribeSupply", int(ch.Type), 0, ch.ClientId, 0, 0, ch.ArgJson)

	s.supplyChans[tp] = append(s.supplyChans[tp], subCh)
	s.supplyMap[tp][idt] = subCh // mapping from clientID to channel
	s.smu.Unlock()
	err := supplyServerFunc(subCh, stream, idt, tp)
	// this supply stream may closed. so take care.

	s.smu.Lock()
	_, ok = s.supplyMap[tp][idt] // still exist? (may removed by others)
	if ok {
		delete(s.supplyMap[tp], idt) // remove map from idt
		s.supplyChans[tp] = removeSupplyChannelFromSlice(s.supplyChans[tp], subCh)
		log.Printf("Remove Supply Stream Channel %v", ch)
	}
	s.smu.Unlock()

	return err
}

// for closing demand channel
func (s *synerexServerInfo) CloseDemandChannel(ctx context.Context, ch *api.Channel) (resp *api.Response, err error) {
	idt := sxutil.IDType(ch.GetClientId())
	tp := ch.GetChannelType()
	err = nil
	s.smu.Lock()
	subCh, ok := s.demandMap[tp][idt]
	if ok {
		delete(s.demandMap[tp], idt) // remove map from idt
		s.demandChans[tp] = removeDemandChannelFromSlice(s.demandChans[tp], subCh)
		log.Printf("Remove Demand Channel %v", ch)
		close(subCh) // close subchannel!
		resp = &api.Response{
			Ok: true,
		}
	} else {
		log.Printf("Cannot find Demand Channel %v", ch)
		resp = &api.Response{
			Ok:  false,
			Err: fmt.Sprintf("Cannot find Demand Channel %v", ch),
		}
	}
	s.smu.Unlock()
	return resp, nil
}

//
func (s *synerexServerInfo) CloseSupplyChannel(ctx context.Context, ch *api.Channel) (resp *api.Response, err error) {
	idt := sxutil.IDType(ch.GetClientId())
	tp := ch.GetChannelType()
	s.smu.Lock()
	subCh, ok := s.supplyMap[tp][idt]
	if ok {
		delete(s.supplyMap[tp], idt) // remove map from idt
		s.supplyChans[tp] = removeSupplyChannelFromSlice(s.supplyChans[tp], subCh)
		log.Printf("Remove Supply Channel %v", ch)
		close(subCh) // close subchannel!
		resp = &api.Response{
			Ok: true,
		}
	} else {
		log.Printf("Cannot find Supply Channel %v", ch)
		resp = &api.Response{
			Ok:  false,
			Err: fmt.Sprintf("Cannot find Supply Channel %v", ch),
		}
	}
	s.smu.Unlock()
	return resp, nil
}

func showAllSubscribers() {
	supp := make([]string, 0)
	for tp, chans := range sinfo.supplyMap {
		if len(chans) > 0 {
			supp = append(supp, fmt.Sprintf("SupplyType:%d", tp))
			for node, _ := range chans {
				supp = append(supp, fmt.Sprintf("ID:%d", node))
			}
		}
	}
	for tp, chans := range sinfo.demandMap {
		if len(chans) > 0 {
			supp = append(supp, fmt.Sprintf("DemandType:%d", tp))
			for node, _ := range chans {
				supp = append(supp, fmt.Sprintf("ID:%d", node))
			}
		}
	}

	log.Printf("ShowAll: %v", supp)
}

func closeAllChannels(node_id int32) {
	idt := sxutil.IDType(node_id)
	sinfo.smu.Lock()
	// starting from supplyMap
	for tp, chans := range sinfo.supplyMap {
		subCh, ok := chans[idt]
		if ok {
			delete(chans, idt) // remove map from idt
			// log.Printf("Length of supplyChans %d", len(sinfo.supplyChans[tp]))
			sinfo.supplyChans[tp] = removeSupplyChannelFromSlice(sinfo.supplyChans[tp], subCh)
			log.Printf("Remove Supply Channel node_id %v, chan %v", idt, tp)
			close(subCh) // close subchannel!
		}
	}
	for tp, chans := range sinfo.demandMap {
		subCh, ok := chans[idt]
		if ok {
			delete(chans, idt) // remove map from idt
			// log.Printf("Length of demandChans %d", len(sinfo.demandChans[tp]))
			sinfo.demandChans[tp] = removeDemandChannelFromSlice(sinfo.demandChans[tp], subCh)
			log.Printf("Remove Demand Channel node_id %v, chan %v", idt, tp)
			close(subCh) // close subchannel!
		}
	}
	sinfo.smu.Unlock()
}

// Closing all channels related to provider ID.
func (s *synerexServerInfo) CloseAllChannels(ctx context.Context, pid *api.ProviderID) (resp *api.Response, err error) {
	closeAllChannels(int32(pid.GetClientId()))
	resp = &api.Response{
		Ok: true,
	}
	return resp, nil
}

// This function is created for each subscribed provider
// This is not efficient if the number of providers increases.
func mbusServerFunc(ch chan *api.MbusMsg, stream api.Synerex_SubscribeMbusServer, id sxutil.IDType) error {
	for {
		select {
		case msg := <-ch:
			if msg.GetMsgId() == 0 { // close message
				return nil // grace close
			}

			if sxutil.IDType(msg.GetSenderId()) != id { // do not send msg from myself
				tgt := sxutil.IDType(msg.GetTargetId())
				if tgt == 0 || tgt == id { // =0 broadcast , = tgt unicast
					err := stream.Send(msg)
					if err != nil {
						//				log.Printf("Error mBus Error %v", err)
						return err
					}
					totalMessages.Inc(1) // update total counter
					mbusMessages.Inc(1)  // update mbus counter
				}
			}
		}
	}
}

func removeMbusChannelFromSlice(sl []chan *api.MbusMsg, c chan *api.MbusMsg) []chan *api.MbusMsg {
	for i, ch := range sl {
		if ch == c {
			return append(sl[:i], sl[i+1:]...)
		}
	}
	log.Printf("Cant find channel %v in removeMbusChannel", c)
	return sl
}
func (s *synerexServerInfo) SubscribeMbus(mb *api.Mbus, stream api.Synerex_SubscribeMbusServer) error {

	mbusCh := make(chan *api.MbusMsg, MessageChannelBufferSize) // make channel for each mbus
	id := sxutil.IDType(mb.GetClientId())
	mbid := mb.MbusId
	s.mmu.Lock()
	chans, cok := s.mbusChans[mbid] 
	if cok == false {
		log.Printf("new MbusChan for MbusID %d", mbid)
	}else{
		log.Printf("next MbusChan for MbusID %d, len(%d)", mbid, len(chans))
	}
	s.mbusChans[mbid] = append(chans, mbusCh)
	mm, ok := s.mbusMap[id]
	if ok {
		//		mm[mbid] = mbusCh
	} else {
		mm = make(map[uint64]chan *api.MbusMsg)
		mm[mbid] = mbusCh
		s.mbusMap[id] = mm
	}
	s.mmu.Unlock()

	err := mbusServerFunc(mbusCh, stream, id) // loop until close for each subscriber.

	s.mmu.Lock()
	s.mbusChans[mbid] = removeMbusChannelFromSlice(s.mbusChans[mbid], mbusCh)
	delete(s.mbusMap, id)
	//	log.Printf("Remove Mbus Stream Channel %v", ch)
	s.mmu.Unlock()

	return err
}

// update name from synerex_api v0.4.1
func (s *synerexServerInfo) SendMbusMsg(c context.Context, msg *api.MbusMsg) (r *api.Response, err error) {
	// FIXME: wait until all subscriber is comming
	count := 0 // loop counter.
	for {
		chans, ok := s.mbusChans[msg.GetMbusId()]
		if ok && len(chans) >= 2 {
			log.Printf("##### All subscriber comming!! [MbusID: %d]\n", msg.GetMbusId())
			break
		}
		count++
		if count > 10 {
			log.Printf("##### Mbus Subscription timeout [MbusId: %d]\n", msg.GetMbusId())
			break
		}
		log.Printf("##### Another Subscriber wating... [MbusId: %d, len(chans): %d]\n", msg.GetMbusId(), len(chans))
		time.Sleep(1 * time.Second)
	}
	okFlag := true
	okMsg := ""
	s.mmu.RLock()
	chs := s.mbusChans[msg.GetMbusId()] // get channel slice from mbus_id
	for i := range chs {
		ch := chs[i]
		if len(ch) < MessageChannelBufferSize { // run under not blocking state.
			ch <- msg
		} else {
			okMsg = fmt.Sprintf("MBus MessageDrop %v", msg)
			okFlag = false
			log.Printf(okMsg) // TODO: thisi is a critical log (message drop)
		}
	}
	s.mmu.RUnlock()
	r = &api.Response{Ok: okFlag, Err: okMsg}
	return r, nil
}

func (s *synerexServerInfo) CloseMbus(c context.Context, mb *api.Mbus) (r *api.Response, err error) {
	okFlag := true
	okMsg := ""
	s.mmu.RLock()
	chs := s.mbusChans[mb.GetMbusId()] // get channel slice from mbus_id
	cmsg := &api.MbusMsg{              // this is close message
		MsgId: 0,
	}
	for i := range chs {
		ch := chs[i]
		if len(ch) < MessageChannelBufferSize { // run under not blocking state.
			ch <- cmsg
		} else {
			okMsg = fmt.Sprintf("MBusClose MessageDrop %v", cmsg)
			okFlag = false
			log.Printf(okMsg)
		}
	}
	s.mmu.RUnlock()
	r = &api.Response{Ok: okFlag, Err: okMsg}
	return r, nil
}

// from synerex_api v0.4.0
func (s *synerexServerInfo) CreateMbus(c context.Context, mbo *api.MbusOpt) (mb *api.Mbus, err error) {
	// just generate new unique ID
	// TODO: private mbus is not implemented yet!
	if mbo.MbusType == api.MbusOpt_PRIVATE {
		log.Printf("Private MBUS is not yet implemented!")
	}
	mb = &api.Mbus{}
	mb.ClientId = 0                    // client must set their own ID.
	mb.MbusId = sxutil.GenerateIntID() // generate unique ID for new Mbus.
	return mb, nil
}

// from synerex_api v0.4.0
func (s *synerexServerInfo) GetMbusState(c context.Context, mb *api.Mbus) (mbs *api.MbusState, err error) {
	// return the status of Mbus.
	// TODO: this method is not fully implemented yet!
	mbs = &api.MbusState{
		MbusId:      mb.MbusId,
		Status:      api.MbusState_INVALID,
		Subscribers: []uint64{},
	}
	return mbs, nil
}

func gatewayServerFunc(ch chan *api.GatewayMsg, ssgs api.Synerex_SubscribeGatewayServer) error {
	for {
		select {
		case sp := <-ch:
			err := ssgs.Send(sp)
			if err != nil {
				return err
			}
		}
	}
}

// for Gateway subscribe
func (s *synerexServerInfo) SubscribeGateway(gi *api.GatewayInfo, ssgs api.Synerex_SubscribeGatewayServer) error {
	log.Printf("Subscribe Gateway %v\n", gi)
	idt := sxutil.IDType(gi.GetClientId())
	//	tp := gi.GetChannels() // not using channels:
	s.gmu.RLock()
	_, ok := s.gatewayMap[idt]
	s.gmu.RUnlock()
	if ok { // check the availability of duplicated gateway client ID
		return errors.New(fmt.Sprintf("duplicated SubscribeGateway for ClientID %v", idt))
	}

	subCh := make(chan *api.GatewayMsg, MessageChannelBufferSize)

	s.gmu.Lock()
	s.gatewayMap[idt] = subCh // mapping from clientID to channel
	s.gmu.Unlock()
	err := gatewayServerFunc(subCh, ssgs)
	// this supply stream may closed. so take care.
	s.gmu.Lock()
	delete(s.gatewayMap, idt) // remove map from idt
	log.Printf("Remove Gateway Client %v", idt)
	s.gmu.Unlock()
	return err
}

// for Gateway Forward
func (s *synerexServerInfo) ForwardToGateway(ctx context.Context, gm *api.GatewayMsg) (*api.Response, error) {
	// need to extract each message and then send them..
	// send demand for desired channels
	okFlag := true
	okMsg := ""
	msgType := gm.GetMsgType()
	switch msgType {
	case api.MsgType_DEMAND:
		dm := gm.GetDemand()
		okFlag, okMsg = sendDemand(s, dm, true)
	case api.MsgType_SUPPLY:
		sp := gm.GetSupply()
		okFlag, okMsg = sendSupply(s, sp, true)
		/*
			case api.MsgType_TARGET:
				tg := gm.GetTarget()
				okFlag, okMsg = sendTarget(s, tg)
			case api.MsgType_MBUS:
				mb := gm.GetMbus()
				okFlag, okMsg = sendMbus(s,mb)
			case api.MsgType_MBUSMSG:
				mbm := gm.GetMbusMsg()
				okFlag, okMsg = sendMbusMsg(s,mbm)

		*/
	}
	r := &api.Response{Ok: okFlag, Err: okMsg}
	return r, nil
}

func newServerInfo() *synerexServerInfo {
	var ms synerexServerInfo
	s := &ms
	for i := 0; i < pbase.ChannelTypeMax; i++ {
		s.demandMap[i] = make(map[sxutil.IDType]chan *api.Demand)
		s.supplyMap[i] = make(map[sxutil.IDType]chan *api.Supply)
		s.waitConfirms[i] = make(map[sxutil.IDType]chan *api.Target)
	}
	s.mbusChans = make(map[uint64][]chan *api.MbusMsg)
	s.mbusMap = make(map[sxutil.IDType]map[uint64]chan *api.MbusMsg)
	s.messageStore = CreateLocalMessageStore()
	s.gatewayMap = make(map[sxutil.IDType]chan *api.GatewayMsg)

	return s
}

// synerex ID system
var (
	NodeBits uint8 = 10
	StepBits uint8 = 12

	nodeMax   int64 = -1 ^ (-1 << NodeBits)
	nodeMask  int64 = nodeMax << StepBits
	nodeShift uint8 = StepBits
	nodeMap         = make(map[int]string)
)

func idToNode(id uint64) string {
	nodeNum := int(int64(id) & nodeMask >> nodeShift) // snowflake node ID:
	//	var ok bool
	var str string
	//	if str, ok = nodeMap[nodeNum]; !ok {
	//		str = sxutil.GetNodeName(nodeNum)
	//	}
	rs := strings.Replace(str, "Provider", "", -1)
	rs2 := strings.Replace(rs, "Server", "", -1)
	return rs2 + ":" + strconv.Itoa(nodeNum)
}

func unaryServerInterceptor(logger *log.Logger, s *synerexServerInfo) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		var err error
		var args string
		var msgType int
		var srcId, tgtId, mid uint64
		method := path.Base(info.FullMethod)
		switch method {
		// Demand
		case "NotifyDemand", "ProposeDemand":
			dm := req.(*api.Demand)
			msgType = int(dm.ChannelType)
			srcId = dm.SenderId
			tgtId = dm.TargetId
			mid = dm.Id
			//			args = "Type:" + strconv.Itoa(int(dm.Type)) + ":" + strconv.FormatUint(dm.Id, 16) + ":" + idToNode(dm.SenderId) + "->" + strconv.FormatUint(dm.TargetId, 16)
			args = idToNode(dm.SenderId) + "->" + idToNode(dm.TargetId)
			// Supply
		case "NotifySupply", "ProposeSupply":
			sp := req.(*api.Supply)
			msgType = int(sp.ChannelType)
			srcId = sp.SenderId
			tgtId = sp.TargetId
			mid = sp.Id
			//			args = "Type:" + strconv.Itoa(int(sp.Type)) + ":" + strconv.FormatUint(sp.Id, 16) + ":" + idToNode(sp.SenderId) + "->" + strconv.FormatUint(sp.TargetId, 16)
			args = idToNode(sp.SenderId) + "->" + idToNode(sp.TargetId)
			// Target
		case "SelectSupply", "Confirm", "SelectDemand":
			tg := req.(*api.Target)
			msgType = int(tg.ChannelType)
			mid = tg.Id
			srcId = tg.SenderId
			tgtId = tg.TargetId
			args = idToNode(tg.SenderId) + "->" + idToNode(tg.TargetId)
			//			args = "Type:" + strconv.Itoa(int(tg.Type)) + ":" + strconv.FormatUint(tg.Id, 16) + ":" + idToNode(tg.Id) + "->" + strconv.FormatUint(tg.TargetId, 16)
		case "SendMsg":
			msg := req.(*api.MbusMsg)
			msgType = int(msg.MsgType)
			mid = msg.MsgId
			srcId = msg.SenderId
			tgtId = msg.TargetId
			args = idToNode(msg.SenderId) + "->" + idToNode(msg.TargetId)

		}

		//		monitorapi.SendMes(&monitorapi.Mes{Message:method+":"+args, Args:""})

		dstId := s.messageStore.getSrcId(tgtId) //
		//		meth := strings.Replace(method, "Propose", "P", 1)
		//		met2 := strings.Replace(meth, "Notify", "N", 1)
		//		met3 := strings.Replace(met2, "Supply", "S", 1)
		//		met4 := strings.Replace(met3, "Demand", "D", 1)
		// it seems here to stuck.
		//		go monitorapi.SendMessage(met4, msgType, mid, srcId, dstId, tgtId, args)

		// save for messageStore
		s.messageStore.AddMessage(method, msgType, mid, srcId, dstId, args)

		// Obtain log using defer
		defer func(begin time.Time) {
			// Obtain method name from info
			method := path.Base(info.FullMethod)
			took := time.Since(begin)
			if err != nil {
				logger.Printf("method %s, took %#v, err %v", method, took, err)
			}
			/*
				fields := logrus.Fields{
					"method": method,
					"took":   took,
				}
				if err != nil {
					fields["error"] = err
					logger.WithFields(fields).Error("Failed")
				} else {
					//				logger.WithFields(fields).Info("Succeeded")
				}
			*/
		}(time.Now())

		// handler = RPC method
		reply, hErr := handler(ctx, req)
		if hErr != nil {
			err = hErr
		}

		sxutil.MsgCountUp()

		return reply, err
	}
}

// Stream Interceptor
func streamServerInterceptor(logger *log.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		var err error
		//		var args string
		log.Printf("streamserver intercept...")
		method := path.Base(info.FullMethod)
		switch method {
		case "SubscribeDemand":
		case "SubscribeSupply":
		}
		//		monitorapi.SendMes(&monitorapi.Mes{Message:method, Args:args})

		defer func(begin time.Time) {
			// Obtain method name from info
			method := path.Base(info.FullMethod)
			took := time.Since(begin)
			if err != nil {
				logger.Printf("method %s, took %#v, err %v", method, took, err)
			}
			//	logger.Printf("method %s, took %#v",method, took)
			/*			fields := logrus.Fields{
							"method": method,
							"took":   took,
						}
						if err != nil {
							fields["error"] = err
							logger.WithFields(fields).Error("Failed")
						} else {
							logger.WithFields(fields).Info("Succeeded")
						}
			*/

		}(time.Now())

		// handler = RPC method
		if hErr := handler(srv, stream); err != nil {
			err = hErr
		}
		log.Printf("streamserver intercept..end .")
		return err
	}
}

func prepareGrpcServer(ssi *synerexServerInfo, opts ...grpc.ServerOption) *grpc.Server {
	gcServer := grpc.NewServer(opts...)
	api.RegisterSynerexServer(gcServer, ssi)
	return gcServer
}

func keepAliveFunc(cmd nodeapi.KeepAliveCommand, str string) {
	//	log.Printf("KeepAlive func %v %v ", cmd, str)
	if cmd == nodeapi.KeepAliveCommand_PROVIDER_DISCONNECT { // we need to purge
		log.Printf("Clear Channel command from NodeServ %s", str)

		var killNodes []int32
		err := json.Unmarshal([]byte(str), &killNodes)
		if err == nil {
			showAllSubscribers()
			for i := range killNodes {
				log.Printf("Closing node %d", killNodes[i])
				closeAllChannels(killNodes[i])
			}
		} else {
			log.Printf("Unmarshal Err %#v", err)
		}

	}

}

func main() {
	flag.Parse()
	log.Printf("SynerexServer(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	srvaddr := fmt.Sprintf("%s:%d", *servaddr, *port)
	//	fmt.Printf("ServerInfo %s\n", srvaddr)
	sxo := &sxutil.SxServerOpt{
		ServerInfo: srvaddr,
		NodeType:   nodeapi.NodeType_SERVER,
		ClusterId:  0,
		AreaId:     "Default",
	}

	channels := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11} // current basic types+alpha

	//	_, rerr := sxutil.RegisterNodeWithCmd(*nodesrv, *name, channels, sxo, keepAliveFunc)
	//	//	monitorapi.InitMonitor(*monitor)
	//	if rerr != nil {
	//		log.Fatalln("Can't register synerex server")
	//	}
	for {
		_, rerr := sxutil.RegisterNodeWithCmd(fmt.Sprintf("%s:%d", *nodeaddr, *nodeport), *name, channels, sxo, keepAliveFunc)
		if rerr != nil {
			log.Println("Can't register synerex server, reconnect now...")
			time.Sleep(1 * time.Second)
		} else {
			log.Println("Register synerex server")
			break
		}
	}
	server_id = sxutil.GenerateIntID() // now obtain unique ID using node_id

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption

	s := newServerInfo()
	sinfo = s
	opts = append(opts, grpc.UnaryInterceptor(unaryServerInterceptor(log.New(os.Stdout, "[Unary]", log.LstdFlags|log.LUTC), s)))

	// for more precise monitoring , we do not use StreamIntercepter.
	//	opts = append(opts, grpc.StreamInterceptor(streamServerInterceptor(logger)))

	grpcServer := prepareGrpcServer(s, opts...)
	log.Printf("Start Synerex Server, connection waiting at port :%d ...", *port)
	serr := grpcServer.Serve(lis)
	log.Printf("Should not arrive here.. server closed. %v", serr)

}
