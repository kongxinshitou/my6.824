package kvraft

import (
	"6824/labgob"
	"6824/labrpc"
	"6824/raft"
	"bytes"
	"go.uber.org/zap"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

type MyLogger struct {
	logger *zap.Logger
}

var (
	logger MyLogger
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	MethodType string
	MethodArg  []string
	//OpUUID string
	OpUUID string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// real KV storage
	Map map[string]string
	// the result of request
	requestRes map[string]string

	maxClientReq map[int]int

	// notify
	isReady map[string]chan struct{}
	// could be optimized
	lastApplyIndex int
	lastExecutedID int
	// 3B
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key, uuid := args.Key, args.UUID
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = "not leader"
		return
	}
	var ch chan struct{}
	var ok bool
	var op Op
	var command []byte
	kv.mu.Lock()
	if res, ok := kv.requestRes[uuid]; ok {
		reply.Value = res
		defer kv.mu.Unlock()
		return
	}
	if ch, ok = kv.isReady[uuid]; ok {
		kv.mu.Unlock()
		goto WaitForRes
	}
	op.MethodType = "Get"
	op.MethodArg = []string{key}
	op.OpUUID = uuid
	command = EncodeOpt(op)
	kv.isReady[uuid] = make(chan struct{})
	kv.mu.Unlock()
	if _, _, ok := kv.rf.Start(command); !ok {
		reply.Err = "not leader"
		return
	}

	logger.Infof("KVServer %v try to start agreement on %v", kv.me, op)

WaitForRes:
	<-ch
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.requestRes[uuid]
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	key, uuid, value, opt := args.Key, args.UUID, args.Value, args.Op
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = "not leader"
		return
	}
	var ch chan struct{}
	var ok bool
	var op Op
	var command []byte
	kv.mu.Lock()
	if _, ok := kv.requestRes[uuid]; ok {
		defer kv.mu.Unlock()
		return
	}
	if ch, ok = kv.isReady[uuid]; ok {
		kv.mu.Unlock()
		goto WaitForRes
	}
	op.MethodType = opt
	op.MethodArg = []string{key, value}
	op.OpUUID = uuid
	command = EncodeOpt(op)
	ch = make(chan struct{})
	kv.isReady[uuid] = ch
	kv.mu.Unlock()
	if _, _, ok := kv.rf.Start(command); !ok {
		reply.Err = "not leader"
		return
	}
	logger.Infof("KVServer %v try to start agreement on %v", kv.me, op)

WaitForRes:
	<-ch
	return
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.Map = map[string]string{}
	kv.requestRes = map[string]string{}
	kv.isReady = map[string]chan struct{}{}
	// You may need initialization code here.
	go kv.applier()
	return kv
}

func (kv *KVServer) processReq(op *Op) {

	uuid := op.OpUUID
	var res string
	switch op.MethodType {
	case "Get":
		key := op.MethodArg[0]
		if s, ok := kv.Map[key]; !ok {
			res = ""
		} else {
			res = s
		}
	case "Put":
		key, value := op.MethodArg[0], op.MethodArg[1]
		kv.Map[key] = value
		res = value
	case "Append":
		key, value := op.MethodArg[0], op.MethodArg[1]
		if s, ok := kv.Map[key]; ok {
			kv.Map[key] = s + value
			res = s + value
		} else {
			kv.Map[key] = value
			res = value
		}
	}
	kv.requestRes[uuid] = res
	if kv.isReady[uuid] != nil {
		close(kv.isReady[uuid])
	}
	logger.Infof("KVServer %v execute op %v succeed res is %v", kv.me, op, res)
}

func (kv *KVServer) snapshot(index int) {

	l := kv.rf.GetStateSize()
	if l <= kv.maxraftstate {
		return
	}
	snapshot, err := kv.rf.GetSnapshot(index)
	if err != nil {
		return
	}
	kv.rf.Snapshot(index, snapshot)
	logger.Warnf("server %v snapshot succeed", kv.me)
}

func (kv *KVServer) applier() {
	for req := range kv.applyCh {
		var reqType string
		if req.SnapshotValid {
			reqType = "snapshot"
		} else if req.CommandValid {
			reqType = "command"
		}

		switch reqType {
		case "command":
			kv.mu.Lock()
			op := DecodeOpt(req.Command.([]byte))
			//strs := strings.Split(op.OpUUID, " ")
			//opClient, _ := strconv.Atoi(strs[0])
			//opUUID, _ := strconv.Atoi(strs[1])
			//logger.Infof("KVServer %v Get Op %v", kv.me, op)
			//if opUUID < kv.maxClientReq[opClient] {
			//	kv.mu.Unlock()
			//	continue
			//}
			if _, ok := kv.requestRes[op.OpUUID]; ok {
				kv.mu.Unlock()
				logger.Infof("Op %v has been executed by KVServer %v", op, kv.me)
				continue
			}
			kv.processReq(op)
			if kv.maxraftstate != -1 {
				go kv.snapshot(req.CommandIndex)
			}
			kv.lastApplyIndex = max(kv.lastApplyIndex, req.CommandIndex)
			kv.mu.Unlock()
		case "snapshot":
			kv.mu.Lock()
			var messages []raft.ApplyMsg
			r := bytes.NewBuffer(req.Snapshot)
			de := labgob.NewDecoder(r)
			de.Decode(&messages)
			lastIndex := messages[len(messages)-1].CommandIndex
			if lastIndex <= kv.lastApplyIndex {
				kv.mu.Unlock()
				break
			}
			for i := len(messages) - (lastIndex - kv.lastApplyIndex); i < len(messages); i++ {
				op := DecodeOpt(messages[i].Command.([]byte))
				if _, ok := kv.requestRes[op.OpUUID]; ok {
					logger.Infof("Op %v has been executed by KVServer %v", op, kv.me)
					continue
				}
				kv.processReq(op)
			}
			kv.lastApplyIndex = lastIndex
			kv.mu.Unlock()
		}

	}
}
