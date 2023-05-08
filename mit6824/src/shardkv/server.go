package shardkv

import (
	"6824/labrpc"
	"6824/shardctrler"
	"sync/atomic"
	"time"
)
import "6824/raft"
import "sync"
import "6824/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	OpArg  any
	OpUUID string
}

type ShardKV struct {
	mu               sync.Mutex
	me               int
	rf               *raft.Raft
	applyCh          chan raft.ApplyMsg
	make_end         func(string) *labrpc.ClientEnd
	gid              int
	ctrlers          []*labrpc.ClientEnd
	maxraftstate     int // snapshot if log grows this big
	shardsMap        map[int]*ShardMap
	expectConfig     int64
	currentConfigNum int64
	currentConfig    shardctrler.Config
	// Your definitions here.
}

func (kv *ShardKV) MigrateDate(args *MigrateArgs, reply *MigrateReply) {

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *ShardKV) CheckConfig() {
	args := &shardctrler.QueryArgs{}
	args.Num = -1
L1:
	for {
		time.Sleep(time.Millisecond * 100)
		for {
			for _, srv := range kv.ctrlers {
				reply := &shardctrler.QueryReply{}
				ok := srv.Call("ShardCtrler.Query", args, reply)
				if ok && reply.WrongLeader == false {
					if int64(reply.Config.Num) > atomic.LoadInt64(&kv.currentConfigNum) && reply.Config.Num != 0 {
						command := EncodeConfig(reply.Config)
						kv.rf.Start(command)
					}
					continue L1
				}
			}
		}
	}
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
	labgob.Register(Op{})
	labgob.Register(ShardMap{})
	labgob.Register(shardctrler.Config{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}

func (kv *ShardKV) applier() {
	for req := range kv.applyCh {
		var reqType string
		if req.SnapshotValid {
			reqType = "snapshot"
		} else if req.CommandValid {
			reqType = "command"
		}

		switch reqType {
		case "command":
			op := DecodeOpt(req.Command.([]byte))
			switch op.OpType {
			case "Get":
				kv.processKVCommand(op)
			case "Put":
				kv.processKVCommand(op)
			case "Append":
				kv.processKVCommand(op)
			case "DataMigration":
				kv.processDataMigration(op)
			case "Config":
				kv.processConfig(op)
			}

		case "snapshot":
			kv.processSnapshot(req.Snapshot)

		}

	}
}
