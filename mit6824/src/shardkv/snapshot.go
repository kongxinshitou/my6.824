package shardkv

import (
	"6824/labgob"
	"6824/shardctrler"
	"bytes"
	"go.uber.org/zap/buffer"
	"strings"
)

func (kv *ShardKV) EncodeShards(req ClientReq) []byte {
	w := new(buffer.Buffer)
	en := labgob.NewEncoder(w)
	en.Encode(req)
	return w.Bytes()
}

func DecodeClientReq(data []byte) *ClientReq {
	op := &ClientReq{}
	w := bytes.NewBuffer(data)
	decode := labgob.NewDecoder(w)
	decode.Decode(op)
	return op
}

func (kv *ShardKV) getSnapshot() []byte {
	lastConfig := string(EncodeAny(kv.LastConfig))
	currentConfig := string(EncodeAny(kv.CurrentConfig))
	stateMache := string(EncodeAny(kv.StateMachine))
	history := string(EncodeAny(kv.History))
	requestRes := string(EncodeAny(kv.RequestRes))

	return []byte(lastConfig + "--" + currentConfig + "--" + stateMache + "--" + history + "--" + requestRes)
}

func DecodeConfig(data []byte) shardctrler.Config {
	c := shardctrler.Config{}
	w := bytes.NewBuffer(data)
	decode := labgob.NewDecoder(w)
	decode.Decode(&c)
	return c
}

func DecodeStateMachine(data []byte) map[int]*Shard {
	stateMachine := map[int]*Shard{}
	w := bytes.NewBuffer(data)
	decode := labgob.NewDecoder(w)
	decode.Decode(&stateMachine)
	return stateMachine
}

func DecodeHistory(data []byte) map[string]int {
	history := map[string]int{}
	w := bytes.NewBuffer(data)
	decode := labgob.NewDecoder(w)
	decode.Decode(&history)
	return history
}

func DecodeRequestRes(data []byte) map[string]string {
	requestRes := map[string]string{}
	w := bytes.NewBuffer(data)
	decode := labgob.NewDecoder(w)
	decode.Decode(&requestRes)
	return requestRes
}

func (kv *ShardKV) processSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	states := strings.Split(string(snapshot), "--")
	lastConfig, currentConfig, stateMache, history, requestRes := states[0], states[1], states[2], states[3], states[4]
	kv.LastConfig = DecodeConfig([]byte(lastConfig))
	kv.CurrentConfig = DecodeConfig([]byte(currentConfig))
	kv.StateMachine = DecodeStateMachine([]byte(stateMache))
	kv.History = DecodeHistory([]byte(history))
	kv.RequestRes = DecodeRequestRes([]byte(requestRes))
	//fmt.Printf("Resume from dead")
	if len(kv.StateMachine) == 0 {
		for i := 0; i < 10; i++ {
			kv.StateMachine[i] = NewShard()
		}
	}
	return
}
