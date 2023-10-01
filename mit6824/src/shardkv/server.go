package shardkv

import (
	"6824/labrpc"
	"6824/shardctrler"
	"fmt"
	"strconv"
	"time"
)
import "6824/raft"
import "sync"

const (
	REQUESTTIMEOUT      = 50 * time.Millisecond
	GCTIMEOUT           = 200 * time.Millisecond
	PULLSHARDTIMEOUT    = 200 * time.Millisecond
	UPDATECONFIGTIMEOUT = 200 * time.Millisecond
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int64 // snapshot if log grows this big

	mck *shardctrler.Clerk

	LastConfig    shardctrler.Config
	CurrentConfig shardctrler.Config

	StateMachine map[int]*Shard

	RequestRes     map[string]string
	History        map[string]int
	LastApplyIndex int
	IsReady        map[string]chan struct{}

	// Your definitions here.
}

func (kv *ShardKV) GetNotifyChannel(LogIndex int) chan struct{} {
	var ch chan struct{}
	notifyIdx := strconv.Itoa(LogIndex)
	ch = make(chan struct{})
	kv.IsReady[notifyIdx] = ch

	return ch
}

func (kv *ShardKV) SetShardStateByConfig() {
	c := kv.CurrentConfig
	if c.Num == 1 {
		return
	}
	currentShardNum := map[int]struct{}{}
	for i := 0; i < len(c.Shards); i++ {
		if c.Shards[i] == kv.gid {
			currentShardNum[i] = struct{}{}
		}
	}
	for shardNum, shard := range kv.StateMachine {
		if _, ok := currentShardNum[shardNum]; !ok {
			if len(shard.Data) == 0 {
				shard.State = Serving
			} else {
				shard.State = BePulling
			}
		}
	}
	for shardNum, _ := range currentShardNum {
		if kv.LastConfig.Shards[shardNum] != kv.gid {
			if kv.StateMachine[shardNum] == nil {
				kv.StateMachine[shardNum] = NewShard()
			}
			kv.StateMachine[shardNum].State = Pulling
		}
	}
}

// Can the kv server serve for this shardID
func (kv *ShardKV) CanServe(ShardID int) bool {
	if shard, ok := kv.StateMachine[ShardID]; ok {
		if kv.CurrentConfig.Shards[ShardID] == kv.gid && (shard.State == Serving || shard.State == GCing) {
			return true
		}
	}
	return false
}

func (kv *ShardKV) GetGid2Shards(state int) map[int][]int {
	m := map[int][]int{}
	for shardID, shard := range kv.StateMachine {
		if shard.State == state {
			gid := kv.LastConfig.Shards[shardID]
			m[gid] = append(m[gid], shardID)
		}
	}

	return m
}

func (kv *ShardKV) Execute(Args interface{}, Reply interface{}) {
	switch Args.(type) {
	case *PutAppendArgs:
		args := Args.(*PutAppendArgs)
		reply := Reply.(*PutAppendReply)
		uuid := args.UUID
		if _, isLeader := kv.rf.GetState(); !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		var op ClientReq
		var command []byte
		var lastOpID int
		clientId, clientOPID := SplitUUID(uuid)

		kv.mu.Lock()
		lastOpID = kv.History[clientId]
		// meaningless req
		if clientOPID <= lastOpID {
			defer kv.mu.Unlock()
			reply.Err = OK
			return
		}
		op.OpType = "PutAppendArgs"
		op.OpArg = *args
		command = EncodeClientReq(op)
		kv.mu.Unlock()
		idx, _, ok := kv.rf.Start(command)
		if !ok {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		ch := kv.GetNotifyChannel(idx)
		kv.mu.Unlock()
		select {
		case <-ch:
			kv.mu.Lock()
			defer kv.mu.Unlock()
			taskID := kv.History[clientId]
			if taskID < clientOPID {
				reply.Err = ErrWrongGroup
			} else {
				reply.Err = OK
			}
			return
		case <-time.After(REQUESTTIMEOUT):
			reply.Err = ErrTimeout
			return
		}
		return
	case *GetArgs:
		args := Args.(*GetArgs)
		reply := Reply.(*GetReply)
		uuid := args.UUID
		if _, isLeader := kv.rf.GetState(); !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		var op ClientReq
		var command []byte
		var lastOpID int
		clientId, clientOPID := SplitUUID(uuid)
		kv.mu.Lock()
		lastOpID = kv.History[clientId]
		// old request, 直接返回没关系的，客户端已经前进了
		if clientOPID < lastOpID {
			defer kv.mu.Unlock()
			reply.Err = OK
			return
		}
		// 重复的请求
		if clientOPID == lastOpID {
			reply.Value = kv.RequestRes[clientId]
			defer kv.mu.Unlock()
			reply.Err = OK
			return
		}
		op.OpType = "Get"
		op.OpArg = *args
		command = EncodeClientReq(op)
		kv.mu.Unlock()
		idx, _, ok := kv.rf.Start(command)
		if !ok {
			reply.Err = ErrWrongLeader
			return
		}

		kv.mu.Lock()
		// register notify channel
		ch := kv.GetNotifyChannel(idx)
		kv.mu.Unlock()

		logger.Infof("KVServer %v try to start agreement on %v", kv.me, op)
		select {
		case <-ch:
			// 接收到了通知，但是没有处理
			kv.mu.Lock()
			defer kv.mu.Unlock()
			taskID := kv.History[clientId]
			if taskID < clientOPID {
				reply.Err = ErrWrongGroup
			} else {
				reply.Err = OK
				reply.Value = kv.RequestRes[clientId]
			}
			return
		case <-time.After(REQUESTTIMEOUT):
			reply.Err = ErrTimeout
			return
		}
		// Don't worry about the situation that can't get a reply, there always exist latest history

	case *AddShardArg:
		args := Args.(*AddShardArg)
		reply := Reply.(*AddShardReply)
		kv.mu.Lock()
		if args.ConfigNum != kv.CurrentConfig.Num {
			reply.Err = ErrOutOfDate
			defer kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		op := ClientReq{}
		op.OpType = "AddShard"
		op.OpArg = *args
		command := EncodeClientReq(op)
		fmt.Println("AddShard", args.ShardIDs)
		if _, _, ok := kv.rf.Start(command); !ok {
			reply.Err = ErrWrongLeader
		}
		return
	case *DeleteShardArg:
		args := Args.(*DeleteShardArg)
		reply := Reply.(*DeleteShardReply)
		kv.mu.Lock()
		if args.ConfigNum != kv.CurrentConfig.Num {
			reply.Err = ErrOutOfDate
			defer kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		op := ClientReq{}
		op.OpType = "DeleteShard"
		op.OpArg = *args
		command := EncodeClientReq(op)
		idx, _, ok := kv.rf.Start(command)
		if !ok {
			reply.Err = ErrWrongLeader
		}
		kv.mu.Lock()
		ch := kv.GetNotifyChannel(idx)
		kv.mu.Unlock()
		select {
		case <-ch:
			reply.Success = true
			fmt.Println("DELETE SUCCESS")
		case <-time.After(REQUESTTIMEOUT):
			reply.Err = ErrTimeout
		}
		return
	case *UpdateConfigArg:
		args := Args.(*UpdateConfigArg)
		reply := Reply.(*UpdateConfigReply)
		op := ClientReq{}
		op.OpType = "UpdateConfig"
		op.OpArg = *args
		command := EncodeClientReq(op)
		fmt.Println(kv.gid, "UpdateConfig", kv.CurrentConfig.Num, "TO", args.Config.Num)
		if _, _, err := kv.rf.Start(command); !err {
			reply.Err = ErrWrongLeader
		}

		return
	}

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.Execute(args, reply)
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.Execute(args, reply)
	return
}

func (kv *ShardKV) Monitor(timeout time.Duration, f func()) {
	for !kv.rf.Killed() {
		if kv.rf.IsLeader() {
			f()
		}
		time.Sleep(timeout)
	}
}

func (kv *ShardKV) FetchLatestConfig() {
	kv.mu.Lock()
	canFetchLatestConfig := true
	for _, shard := range kv.StateMachine {
		if shard.State != Serving {
			canFetchLatestConfig = false
			break
		}
	}
	nextConfigNum := kv.CurrentConfig.Num + 1
	kv.mu.Unlock()
	if canFetchLatestConfig {
		conf := kv.mck.Query(nextConfigNum)
		if conf.Num != nextConfigNum {
			return
		}
		arg := &UpdateConfigArg{conf}
		reply := &UpdateConfigReply{}
		kv.Execute(arg, reply)
	}

	return
}

func (kv *ShardKV) GetShard(args *GetShardArg, reply *GetShardReply) {
	if !kv.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.CurrentConfig.Num < args.ConfigNum {
		reply.Err = ErrOutOfDate
		return
	}
	shards := map[int]*Shard{}
	for _, shardID := range args.ShardIDs {
		if kv.StateMachine[shardID] == nil {
			continue
		}
		shards[shardID] = kv.StateMachine[shardID].deepCopy()
	}
	reply.ShardIDs = args.ShardIDs
	reply.ConfigNum = args.ConfigNum
	reply.Shards = shards
	reply.RequestRes = DeepCopyMapStringString(kv.RequestRes)
	reply.History = DeepCopyStringInt(kv.History)
	reply.Success = true
	fmt.Println()
}

func (kv *ShardKV) PullShards() {
	wg := sync.WaitGroup{}
	kv.mu.Lock()
	for gid, shardIDs := range kv.GetGid2Shards(Pulling) {
		servers := kv.LastConfig.Groups[gid]
		args := &GetShardArg{
			ShardIDs:  shardIDs,
			ConfigNum: kv.CurrentConfig.Num,
		}
		reply := &GetShardReply{}
		wg.Add(1)
		go func(servers []string, args *GetShardArg, reply *GetShardReply) {
			defer wg.Done()
			fmt.Println(kv.gid, "is Pulling data from ", kv.LastConfig.Shards[args.ShardIDs[0]], args, reply)
			for _, serverName := range servers {
				srv := kv.make_end(serverName)
				srv.Call("ShardKV.GetShard", args, reply)
				if reply.Success {
					break
				}
			}
			if reply.Success {
				addShardArg := GetAddShardByGetShardReply(reply)
				addShardReply := &AddShardReply{}

				fmt.Println(kv.gid, "Get Shards", args.ShardIDs, "Success")
				kv.Execute(addShardArg, addShardReply)
				return
			}

		}(servers, args, reply)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) DeleteShard(args *DeleteShardArg, reply *DeleteShardReply) {
	if !kv.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.CurrentConfig.Num > args.ConfigNum {
		reply.Success = true
		defer kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	kv.Execute(args, reply)
	return
}

func (kv *ShardKV) GCShard() {
	wg := sync.WaitGroup{}
	kv.mu.Lock()
	for gid, shardIDs := range kv.GetGid2Shards(GCing) {
		servers := kv.LastConfig.Groups[gid]
		args := &DeleteShardArg{
			ShardIDs:  shardIDs,
			ConfigNum: kv.CurrentConfig.Num,
		}
		reply := &DeleteShardReply{}
		wg.Add(1)
		go func(servers []string, args *DeleteShardArg, reply *DeleteShardReply) {
			defer wg.Done()
			for _, serverName := range servers {
				srv := kv.make_end(serverName)
				srv.Call("ShardKV.DeleteShard", args, reply)
				if reply.Success {
					break
				}
			}
			if reply.Success {
				kv.Execute(args, reply)
				if reply.Success {
					fmt.Println("I successfully deleted")
				}
				return
			}

		}(servers, args, reply)
	}
	kv.mu.Unlock()
	wg.Wait()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()

}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = int64(maxraftstate)
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.RequestRes = map[string]string{}
	kv.History = map[string]int{}
	kv.IsReady = map[string]chan struct{}{}
	kv.StateMachine = map[int]*Shard{}
	for i := 0; i < 10; i++ {
		kv.StateMachine[i] = NewShard()
	}
	kv.processSnapshot(kv.rf.GetSnapshot())
	go kv.Monitor(UPDATECONFIGTIMEOUT, kv.FetchLatestConfig)
	go kv.Monitor(PULLSHARDTIMEOUT, kv.PullShards)
	go kv.Monitor(GCTIMEOUT, kv.GCShard)
	go kv.applier()

	return kv
}

func (kv *ShardKV) getConfigChangeUUID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

func (kv *ShardKV) applier() {
	for req := range kv.applyCh {
		var reqType string
		if req.SnapshotValid {
			reqType = "snapshot"
		} else if req.CommandValid {
			reqType = "command"
		}
		notifyIdx := strconv.Itoa(req.CommandIndex)
		switch reqType {
		case "command":
			op := DecodeClientReq(req.Command.([]byte))
			switch op.OpType {
			case "PutAppendArgs":
				//fmt.Println(kv.gid, "in config ", kv.CurrentConfig.Num, "is Going to", "PutAppendArgs")
				kv.processAppendPut(op, notifyIdx)
			case "Get":
				//fmt.Println(kv.gid, "in config ", kv.CurrentConfig.Num, "is Going to", "Get")
				kv.processGet(op, notifyIdx)
			case "AddShard":
				//fmt.Println(kv.gid, "in config ", kv.CurrentConfig.Num, "is Going to", "AddShard")
				kv.processAddShard(op)
			case "DeleteShard":
				//fmt.Println(kv.gid, "in config ", kv.CurrentConfig.Num, "is Going to", "DeleteShard")
				kv.processDeleteShard(op, notifyIdx)
			case "UpdateConfig":
				//fmt.Println(kv.gid, "in config ", kv.CurrentConfig.Num, "is Going to", "UpdateConfig")
				kv.processConfigUpdate(op)
			}
			kv.LastApplyIndex = max(kv.LastApplyIndex, req.CommandIndex)
			logger.Infof("server %v, lastApplyIndex is %v", kv.me, kv.LastApplyIndex)
			if kv.maxraftstate != -1 && kv.rf.GetStateSize() > int64(kv.maxraftstate) {
				snapshot := kv.getSnapshot()
				kv.rf.Snapshot(kv.LastApplyIndex, snapshot)
			}
		case "snapshot":
			kv.processSnapshot(req.Snapshot)

		}

	}
}
