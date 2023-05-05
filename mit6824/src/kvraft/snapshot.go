package kvraft

import (
	"strconv"
	"strings"
)

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
	kv.requestRes = map[int]string{}
	for _, his := range hiss {
		strs := strings.Split(his, ":")
		client, _ := strconv.Atoi(strs[0])
		lastOpID, _ := strconv.Atoi(strs[1])
		res := strs[2]
		kv.History[client] = lastOpID
		kv.requestRes[client] = res
	}
}

func (kv *KVServer) closeRegisterCh() {
	for uuid, ch := range kv.isReady {
		client, opId := SplitUUID(uuid)
		if opId <= kv.History[client] {
			CloseCh(ch)
		}
	}
}

func (kv *KVServer) getHisState() string {
	state := []string{}
	for client, lastOPID := range kv.History {
		res := kv.requestRes[client]
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
	if len(snapshot) == 0 {
		return
	}
	states := strings.Split(string(snapshot), "-")
	mapState, hisState := states[0], states[1]
	kv.setMapState(mapState)
	kv.setHisState(hisState)
	kv.closeRegisterCh()
}
