package kvraft

import (
	"6824/labgob"
	"6824/labrpc"
	"6824/raft"
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
	History    map[int]int
	// notify
	isReady map[string]chan struct{}
	// could be optimized
	lastApplyIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key, uuid := args.Key, args.UUID
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = "not leader"
		return
	}
	var ch chan struct{}
	var op Op
	var command []byte
	var lastOpID int
	clientId, clientOPID := SplitUUID(uuid)
	kv.mu.Lock()
	lastOpID = kv.History[clientId]
	if clientOPID < lastOpID {
		defer kv.mu.Unlock()
		return
	}
	if clientOPID == lastOpID {
		reply.Value = kv.requestRes[uuid]
		defer kv.mu.Unlock()
		return
	}
	op.MethodType = "Get"
	op.MethodArg = []string{key}
	op.OpUUID = uuid
	command = EncodeOpt(op)
	if _, ok := kv.isReady[uuid]; !ok {
		ch = make(chan struct{})
		kv.isReady[uuid] = ch
	} else {
		ch = kv.isReady[uuid]
	}
	kv.mu.Unlock()
	if _, _, ok := kv.rf.Start(command); !ok {
		reply.Err = "not leader"
		return
	}

	logger.Infof("KVServer %v try to start agreement on %v", kv.me, op)

	<-ch
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Don't worry about the situation that can't get a reply, there always exist latest history
	if value, ok := kv.requestRes[uuid]; ok {
		reply.Value = value
	} else {
		reply.Err = "no record, turn to others"
	}
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
	var op Op
	var command []byte
	var lastOpID int
	clientId, clientOPID := SplitUUID(uuid)
	kv.mu.Lock()
	lastOpID = kv.History[clientId]
	// meaningless req
	if clientOPID <= lastOpID {
		defer kv.mu.Unlock()
		return
	}
	op.MethodType = opt
	op.MethodArg = []string{key, value}
	op.OpUUID = uuid
	command = EncodeOpt(op)
	if _, ok := kv.isReady[uuid]; !ok {
		ch = make(chan struct{})
		kv.isReady[uuid] = ch
	} else {
		ch = kv.isReady[uuid]
	}
	kv.mu.Unlock()
	if _, _, ok := kv.rf.Start(command); !ok {
		reply.Err = "not leader"
		return
	}
	logger.Infof("KVServer %v try to start agreement on %v", kv.me, op)

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
	kv.History = map[int]int{}
	kv.processSnapshot(kv.rf.GetSnapshot())
	// You may need initialization code here.
	go kv.applier()
	return kv
}

// The request from a client is arrived in order
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
			opClient, opUUID := SplitUUID(op.OpUUID)
			hisID := kv.History[opClient]
			if opUUID <= hisID {
				kv.mu.Unlock()
				continue
			}
			logger.Infof("KVServer %v Get Op %v", kv.me, op)
			kv.processReq(op)
			kv.lastApplyIndex = max(kv.lastApplyIndex, req.CommandIndex)
			logger.Infof("server %v, lastApplyIndex is %v", kv.me, kv.lastApplyIndex)
			kv.History[opClient] = opUUID
			if kv.maxraftstate != -1 && kv.rf.GetStateSize() > int64(kv.maxraftstate) {
				snapshot := kv.getSnapshot()
				logger.Infof("get the server %v state, the map is %v, the histRes is %v", kv.me, kv.Map, kv.History)
				kv.rf.Snapshot(kv.lastApplyIndex, snapshot)
			}
			kv.mu.Unlock()
		case "snapshot":
			logger.Debugf("server %v get snapshot message", kv.me)
			kv.mu.Lock()
			snapshot := req.Snapshot
			if req.SnapshotIndex <= kv.lastApplyIndex {
				kv.mu.Unlock()
				continue
			}
			logger.Debugf("before snapshot the server %v state, the map is %v, the histRes is %v", kv.me, kv.Map, kv.History)
			kv.processSnapshot(snapshot)
			logger.Debugf("after snapshot the server %v state, the map is %v, the histRes is %v", kv.me, kv.Map, kv.History)
			kv.lastApplyIndex = req.SnapshotIndex
			kv.mu.Unlock()
		}

	}
}

func (kv *KVServer) CleanCh() {
	newIsReady := map[string]chan struct{}{}
	for k, v := range kv.isReady {
		if isClosed(v) {
			continue
		}
		newIsReady[k] = v
	}
	kv.isReady = newIsReady
	return
}
