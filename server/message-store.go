package main

import (
	"log"
	"sync"
)

// MessageStore saves all massage in memory
//   this should change into other distributed service.
/*
type MessageStore interface{
	AddMessage(msgType string, chType int, mid uint64, src uint64, dst uint64, arg string)
	getSrcId(mid uint64) uint64
}*/

type message struct {
	msgType string
	chType  int
	mid     uint64
	src     uint64
	dst     uint64
	arg     string
}

// real struct for MessageStore
type MessageStore struct {
	store     map[uint64]message
	limit     []uint64 // for storing message history
	limit_pt  int      // for message index
	limit_max int      // for max number of stored message
	count     uint64   // for counting message number (for debug)
	mutex     sync.RWMutex
}

// CreateMessageStore creates base dataset
func CreateLocalMessageStore() *MessageStore {
	mst := &MessageStore{}
	mst.init()
	return mst
}

func (mst *MessageStore) init() {
	//	fmt.Println("Initialize LocalStore")
	mst.store = make(map[uint64]message)
	mst.mutex = sync.RWMutex{}
	mst.limit_max = 1000 // todo: Currently, we just store last 1000 messages.
	mst.limit_pt = 0
	mst.count = 0
	mst.limit = make([]uint64, mst.limit_max)
	log.Printf("Initialize LocalStore with limit %d", mst.limit_max)
}

//todo: This is not efficient store. So we need to fix it.
func (mst *MessageStore) AddMessage(msgType string, chType int, mid uint64, src uint64, dst uint64, arg string) {

	mes := message{msgType, chType, mid, src, dst, arg}
	//	fmt.Printf("AddMessage %v\n",mes)
	//	fmt.Printf("ls.store %v %d \n",ls.store, mid)
	mst.mutex.Lock()
	if mst.limit[mst.limit_pt] != 0 { // ring buffer, delete last one.
		delete(mst.store, mst.limit[mst.limit_pt])
		//		fmt.Printf("mstore: %4d/%7d ",  mst.limit_pt, mst.count)
	}
	mst.store[mid] = mes
	mst.count++
	mst.limit[mst.limit_pt] = mid
	mst.limit_pt = (mst.limit_pt + 1) % mst.limit_max
	mst.mutex.Unlock()
	//	fmt.Println("OK.")
}

func (mst *MessageStore) getSrcId(mid uint64) uint64 {
	mst.mutex.RLock()
	mes, ok := mst.store[mid]
	mst.mutex.RUnlock()
	if !ok {
		//		fmt.Println("Cant find message id Error!")
		return 0
	}
	return mes.src
}
