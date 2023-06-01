package shardctrler

import "6824/raft"
import "6824/labrpc"
import "sync"
import "6824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs    []Config // indexed by config num
	requestRes map[string]interface{}
	History    map[string]int
	// notify
	isReady        map[string]chan struct{}
	lastApplyIndex int
}

type Op struct {
	// Your data here.
	MethodType string
	MethodArg  any
	//OpUUID string
	OpUUID string
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	uuid := args.UUID
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = "not leader"
		return
	}
	var ch chan struct{}
	var op Op
	var command []byte
	var lastOpID int
	clientId, clientOPID := SplitUUID(uuid)
	sc.mu.Lock()
	lastOpID = sc.History[clientId]
	if clientOPID < lastOpID {
		defer sc.mu.Unlock()
		return
	}
	if clientOPID == lastOpID {
		defer sc.mu.Unlock()
		return
	}
	op.MethodType = "Join"
	op.MethodArg = args
	op.OpUUID = uuid
	command = EncodeOpt(op)
	if _, ok := sc.isReady[uuid]; !ok {
		ch = make(chan struct{})
		sc.isReady[uuid] = ch
	} else {
		ch = sc.isReady[uuid]
	}
	sc.mu.Unlock()
	if _, _, ok := sc.rf.Start(command); !ok {
		reply.WrongLeader = true
		reply.Err = "not leader"
		return
	}

	<-ch
	// Don't worry about the situation that can't get a reply, there always exist latest history
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	uuid := args.UUID
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = "not leader"
		return
	}
	var ch chan struct{}
	var op Op
	var command []byte
	var lastOpID int
	clientId, clientOPID := SplitUUID(uuid)
	sc.mu.Lock()
	if len(sc.configs) == 1 {
		defer sc.mu.Unlock()
		reply.Err = "No config can't leave"
		return
	}
	lastOpID = sc.History[clientId]
	if clientOPID < lastOpID {
		defer sc.mu.Unlock()
		return
	}
	if clientOPID == lastOpID {
		defer sc.mu.Unlock()
		return
	}
	op.MethodType = "Leave"
	op.MethodArg = args
	op.OpUUID = uuid
	command = EncodeOpt(op)
	if _, ok := sc.isReady[uuid]; !ok {
		ch = make(chan struct{})
		sc.isReady[uuid] = ch
	} else {
		ch = sc.isReady[uuid]
	}
	sc.mu.Unlock()
	if _, _, ok := sc.rf.Start(command); !ok {
		reply.WrongLeader = true
		reply.Err = "not leader"
		return
	}

	<-ch
	// Don't worry about the situation that can't get a reply, there always exist latest history
	return

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	uuid := args.UUID
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = "not leader"
		return
	}
	var ch chan struct{}
	var op Op
	var command []byte
	var lastOpID int
	clientId, clientOPID := SplitUUID(uuid)
	sc.mu.Lock()
	lastOpID = sc.History[clientId]
	if clientOPID < lastOpID {
		defer sc.mu.Unlock()
		return
	}
	if clientOPID == lastOpID {
		defer sc.mu.Unlock()
		return
	}
	op.MethodType = "Move"
	op.MethodArg = args
	op.OpUUID = uuid
	command = EncodeOpt(op)
	if _, ok := sc.isReady[uuid]; !ok {
		ch = make(chan struct{})
		sc.isReady[uuid] = ch
	} else {
		ch = sc.isReady[uuid]
	}
	sc.mu.Unlock()
	if _, _, ok := sc.rf.Start(command); !ok {
		reply.WrongLeader = true
		reply.Err = "not leader"
		return
	}

	<-ch
	// Don't worry about the situation that can't get a reply, there always exist latest history
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	uuid := args.UUID

	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = "not leader"
		return
	}
	var ch chan struct{}
	var op Op
	var command []byte
	var lastOpID int
	clientId, clientOPID := SplitUUID(uuid)
	sc.mu.Lock()
	lastOpID = sc.History[clientId]
	if args.Num < -1 {
		reply.Err = "Wrong req"
		defer sc.mu.Unlock()
		return
	}
	if clientOPID < lastOpID {
		defer sc.mu.Unlock()
		return
	}
	if clientOPID == lastOpID {
		defer sc.mu.Unlock()
		return
	}
	op.MethodType = "Query"
	op.MethodArg = args
	op.OpUUID = uuid
	command = EncodeOpt(op)
	if _, ok := sc.isReady[uuid]; !ok {
		ch = make(chan struct{})
		sc.isReady[uuid] = ch
	} else {
		ch = sc.isReady[uuid]
	}
	sc.mu.Unlock()
	if _, _, ok := sc.rf.Start(command); !ok {
		reply.WrongLeader = true
		reply.Err = "not leader"
		return
	}

	<-ch
	// Don't worry about the situation that can't get a reply, there always exist latest history
	sc.mu.Lock()
	defer sc.mu.Unlock()
	reply.Config = sc.requestRes[clientId].(Config)
	return

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	// Your code here.
	sc.History = map[string]int{}
	sc.requestRes = map[string]interface{}{}
	sc.isReady = map[string]chan struct{}{}
	logger.Infof("Start server")
	go sc.applier()
	return sc
}

func (sc *ShardCtrler) applier() {
	for req := range sc.applyCh {
		sc.mu.Lock()
		op := DecodeOpt(req.Command.([]byte))

		opClient, opUUID := SplitUUID(op.OpUUID)
		hisID := sc.History[opClient]
		if opUUID <= hisID {
			sc.mu.Unlock()
			continue
		}
		sc.processReq(op)
		sc.lastApplyIndex = max(sc.lastApplyIndex, req.CommandIndex)
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) processReq(op *Op) {
	uuid := op.OpUUID

	client, opID := SplitUUID(uuid)
	switch op.MethodType {
	case "Join":
		args := op.MethodArg.(JoinArgs)
		newConfig := NewDefaultConfig()
		newConfig.Num = sc.configs[len(sc.configs)-1].Num + 1
		newConfig.AppendGroups(args.Servers)
		newConfig.AppendGroups(sc.configs[len(sc.configs)-1].Groups)
		newConfig.Rehash()
		sc.configs = append(sc.configs, newConfig)
	case "Leave":
		args := op.MethodArg.(LeaveArgs)
		newConfig := NewDefaultConfig()
		newConfig.Num = sc.configs[len(sc.configs)-1].Num + 1
		newConfig.AppendGroups(sc.configs[len(sc.configs)-1].Groups)
		for _, gID := range args.GIDs {
			delete(newConfig.Groups, gID)
		}
		newConfig.Rehash()
		sc.configs = append(sc.configs, newConfig)
	case "Move":
		args := op.MethodArg.(MoveArgs)
		newConfig := NewDefaultConfig()
		newConfig.Num = sc.configs[len(sc.configs)-1].Num + 1
		newConfig.AppendGroups(sc.configs[len(sc.configs)-1].Groups)
		newConfig.Shards = sc.configs[len(sc.configs)-1].Shards
		newConfig.Shards[args.Shard] = args.GID
		sc.configs = append(sc.configs, newConfig)
	case "Query":
		args := op.MethodArg.(QueryArgs)
		maxConfigNum := sc.configs[len(sc.configs)-1].Num
		if args.Num >= maxConfigNum || args.Num == -1 {
			sc.requestRes[client] = sc.configs[len(sc.configs)-1]
		} else if 0 <= args.Num && args.Num < maxConfigNum {
			sc.requestRes[client] = sc.configs[args.Num]
		} else {
		}
	}
	sc.History[client] = opID
	if sc.isReady[uuid] != nil {
		close(sc.isReady[uuid])
	}
}
