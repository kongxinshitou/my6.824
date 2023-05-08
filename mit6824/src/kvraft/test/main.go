package kvraft

import (
	"6824/kvraft"
	"6824/labgob"
	"bytes"
	"go.uber.org/zap/buffer"
	"strconv"
	"strings"
)

type KVServer struct {
	Map            map[string]string
	RequestRes     map[string]string
	History        map[int]int
	IsReady        map[string]chan struct{}
	lastApplyIndex int
}

func (kv *KVServer) setMapState(state string) {
	kv.Map = map[string]string{}
	kvs := strings.Split(state, ",")
	for _, s := range kvs {
		strs := strings.Split(s, ":")
		k, v := strs[0], strs[1]
		kv.Map[k] = v
	}
}

func (kv *KVServer) getMapState() string {
	res := []string{}
	for k, v := range kv.Map {
		res = append(res, k+":"+v)
	}
	return strings.Join(res, ",")
}

func (kv *KVServer) setHisState(state string) {
	hiss := strings.Split(state, ",")
	kv.History = map[int]int{}
	kv.RequestRes = map[string]string{}
	for _, his := range hiss {
		strs := strings.Split(his, ":")
		client, _ := strconv.Atoi(strs[0])
		lastOpID, _ := strconv.Atoi(strs[1])
		res := strs[2]
		uuid := strs[0] + " " + strs[1]
		kv.History[client] = lastOpID
		kv.RequestRes[uuid] = res
	}
}

func (kv *KVServer) closeRegisterCh() {
	for uuid, ch := range kv.IsReady {
		client, opId := kvraft.SplitUUID(uuid)
		if opId <= kv.History[client] {
			kvraft.CloseCh(ch)
		}
	}
}

func (kv *KVServer) getHisState() string {
	state := []string{}
	for client, lastOPID := range kv.History {
		uuid := kvraft.CombineUUID(client, lastOPID)
		res := kv.RequestRes[uuid]
		state = append(state, strconv.Itoa(client)+":"+strconv.Itoa(lastOPID)+":"+res)
	}
	return strings.Join(state, ",")
}

func (kv *KVServer) getSnapshot() []byte {
	mapState := kv.getMapState()
	hisState := kv.getHisState()
	return []byte(mapState + "-" + hisState)
}

func (kv *KVServer) processSnapshot(snapshot []byte) {
	states := strings.Split(string(snapshot), "-")
	mapState, hisState := states[0], states[1]
	kv.setMapState(mapState)
	kv.setHisState(hisState)
	kv.closeRegisterCh()
}

func NewDefaultKVServer() *KVServer {
	kv := KVServer{}
	kv.Map = map[string]string{}
	kv.RequestRes = map[string]string{}
	kv.History = map[int]int{}
	kv.IsReady = map[string]chan struct{}{}
	return &kv
}

type Op struct {
	A int
	M map[string]string
}

func EncodeOpt(op Op) []byte {
	w := new(buffer.Buffer)
	en := labgob.NewEncoder(w)
	en.Encode(op)
	return w.Bytes()
}

func DecodeOpt(data []byte) *Op {
	op := &Op{}
	w := bytes.NewBuffer(data)
	decode := labgob.NewDecoder(w)
	decode.Decode(op)
	return op
}
