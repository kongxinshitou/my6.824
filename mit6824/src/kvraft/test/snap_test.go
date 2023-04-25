package kvraft

import (
	"fmt"
	"testing"
	"time"
)

func (kv *KVServer) mockCh() {
	for k, v := range kv.IsReady {
		go func(k string, v chan struct{}) {
			<-v
			fmt.Println(k, "close gracefully")
		}(k, v)
	}
}

func TestDO(t *testing.T) {
	kv := NewDefaultKVServer()
	kv.History[1] = 2
	kv.Map["1"] = "10"
	kv.RequestRes["1 1"] = "9 10"
	kv.RequestRes["1 2"] = "10  10 10 23"
	kv.History[2] = 3
	kv.Map["2"] = "12"
	kv.RequestRes["2 1"] = "10"
	kv.RequestRes["2 2"] = "11"
	kv.RequestRes["2 3"] = "12"
	snapshot := kv.getSnapshot()
	kv2 := NewDefaultKVServer()

	fmt.Println(kv2.Map)
	fmt.Println(kv2.RequestRes)
	fmt.Println(kv2.History)
	kv2.IsReady["1 1"] = make(chan struct{})
	kv2.IsReady["1 2"] = make(chan struct{})
	kv2.IsReady["2 1"] = make(chan struct{})

	kv2.processSnapshot(snapshot)
	go kv2.mockCh()
	time.Sleep(2 * time.Second)
}

func TestDefault(t *testing.T) {
	var a bool
	fmt.Println(a)
}
