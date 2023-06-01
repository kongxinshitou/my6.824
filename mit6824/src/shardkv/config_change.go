package shardkv

import (
	"6824/labrpc"
	"6824/shardctrler"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

func (kv *ShardKV) sendMigrationData(args *PutMigrationDataArgs, reply *PutMigrationDataReply) {
	servers := make([]*labrpc.ClientEnd, 0)
	for _, serverName := range args.Servers {
		servers = append(servers, kv.make_end(serverName))
	}
	for {
		for _, server := range servers {
			reply.Err = ""
			reply.ErrorCode = 0
			ok := server.Call("ShardKV.MigrateData", args, reply)
			if !ok {
				continue
			}
			switch reply.ErrorCode {
			case 0:
				continue
			case 1:
				continue
			case 2:
				return
			case 3:
				return
			case 4:
				continue
			}
		}
	}

}

func (kv *ShardKV) CheckConfig() {
	args := &shardctrler.QueryArgs{}
	args.Num = -1

	for {
		time.Sleep(time.Millisecond * (REFRESHCONFIGTIMEOUT + time.Duration(rand.Intn(REFRESHCONFIGRANDOM))))
		if atomic.LoadInt64(&kv.IsConfigChanging) != 0 || !kv.rf.IsLeader() || len(kv.peers) == 0 {
			continue
		}

		for _, srv := range kv.ctrlers {
			reply := &shardctrler.QueryReply{}
			ok := srv.Call("ShardCtrler.Query", args, reply)
			if !ok || reply.WrongLeader == true {
				continue
			}
			beginTrxArgs := &BeginTransactionArgs{}
			beginTrxArgs.UUID = kv.getUUID()
			beginTrxReply := &BeginTransactionReply{}
			kv.sendTrxBegin(beginTrxArgs, beginTrxReply)
			if beginTrxReply.ErrorCode != 0 {
				break
			}
			kv.mu.Lock()
			NewConfigNum := reply.Config.Num
			NewConfig := reply.Config
			if len(kv.peers) == 0 {

			}

			confCommands := ConfigCommands{}
			deleteShards := map[int][]int{}
			for shard, gid := range reply.Config.Shards {
				var s *ShardMap
				var ok bool
				if s, ok = kv.shardsMap[shard]; !ok {
					continue
				}
				if kv.gid == gid && s.ShardNum < NewConfigNum {
					c := ConfigCommand{}
					c.Shard = shard
					c.Command = "update"
					c.Num = NewConfigNum
					confCommands.Commands = append(confCommands.Commands, c)
				}
				if kv.gid != gid {
					deleteShards[gid] = append(deleteShards[gid], shard)
				}
			}
			if len(confCommands.Commands) == 0 {
				kv.mu.Unlock()
				break
			}
			wg := sync.WaitGroup{}
			for gid := range deleteShards {
				args := &PutMigrationDataArgs{}
				reply := &PutMigrationDataReply{}
				args.GId = gid
				args.OpUUID = kv.getUUID()
				args.Servers = NewConfig.Groups[gid]
				for _, shard := range deleteShards[gid] {
					c := DataMigrateCommand{}
					c.Data = *kv.shardsMap[shard]
					c.ConfigNum = NewConfigNum
					args.Commands = append(args.Commands, c)
				}
				wg.Add(1)
				go func(args *PutMigrationDataArgs, reply *PutMigrationDataReply) {
					defer wg.Done()
					kv.sendMigrationData(args, reply)
					if reply.Err == "" {
						for _, command := range args.Commands {
							c := ConfigCommand{}
							c.Shard = command.Data.ShardNum
							c.Command = "delete"
							confCommands.Commands = append(confCommands.Commands, c)
						}
					}
				}(args, reply)
			}
			wg.Wait()
			configChangeArgs := &ConfigChangeArgs{}
			configChangeArgs.Commands = confCommands
			configChangeArgs.UUID = kv.getUUID()
			configChangeReply := &ConfigChangeReply{}

			kv.mu.Unlock()

			kv.sendConfigChange(configChangeArgs, configChangeReply)
		}
	}
}

func (kv *ShardKV) sendTrxBegin(args *BeginTransactionArgs, reply *BeginTransactionReply) {
	op := Op{}
	op.OpType = "TRXBegin"
	for {
		for _, server := range kv.peers {
			reply.Err = ""
			ok := server.Call("ShardKV.TrxBegin", args, reply)
			if ok && (reply.ErrorCode == 0 || reply.ErrorCode == 2) {
				return
			}
		}
	}

}

func (kv *ShardKV) TrxBegin(args *BeginTransactionArgs, reply *BeginTransactionReply) {
	if !kv.rf.IsLeader() {
		reply.ErrorCode = 1
		reply.Err = "not leader"
		return
	}
	if atomic.LoadInt64(&kv.IsConfigChanging) == 1 {

	}
	kv.mu.Lock()
	client, OpNum := SplitUUID(args.UUID)
	if earliest, ok := kv.History[client]; ok && earliest >= OpNum {
		kv.mu.Unlock()
		return
	}
	var ch chan struct{}
	var ok bool
	op := Op{}
	op.OpType = "trxBegin"
	op.OpArg = args
	op.OpUUID = args.UUID
	if ch, ok = kv.IsReady[op.OpUUID]; !ok {
		ch = make(chan struct{})
		kv.IsReady[op.OpUUID] = ch
	}
	command := EncodeOpt(op)
	kv.mu.Unlock()
	_, _, ok = kv.rf.Start(command)
	if !ok {
		reply.ErrorCode = 1
		reply.Err = "not leader"
	}
	select {
	case <-ch:
		return
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		reply.ErrorCode = 4
		reply.Err = "timeout"
		return
	}
}

func (kv *ShardKV) sendConfigChange(args *ConfigChangeArgs, reply *ConfigChangeReply) {
	for {
		for _, server := range kv.peers {
			reply.Err = ""
			ok := server.Call("ShardKV.ConfigChange", args, reply)
			if ok && reply.ErrorCode == 0 {
				return
			}
		}
	}
}

func (kv *ShardKV) ConfigChange(args *ConfigChangeArgs, reply *ConfigChangeReply) {
	if !kv.rf.IsLeader() {
		reply.ErrorCode = 1
		reply.Err = "not leader"
		return
	}
	if atomic.LoadInt64(&kv.IsConfigChanging) == 1 {
		reply.ErrorCode = 2
		reply.Err = "conflict"
		return
	}
	kv.mu.Lock()
	client, OpNum := SplitUUID(args.UUID)
	if earliest, ok := kv.History[client]; ok && earliest >= OpNum {
		kv.mu.Unlock()
		return
	}
	var ch chan struct{}
	var ok bool
	op := Op{}
	op.OpType = "ConfigChange"
	op.OpArg = args
	op.OpUUID = args.UUID
	if ch, ok = kv.IsReady[op.OpUUID]; !ok {
		ch = make(chan struct{})
		kv.IsReady[op.OpUUID] = ch
	}
	command := EncodeOpt(op)
	kv.mu.Unlock()
	_, _, ok = kv.rf.Start(command)
	if !ok {
		reply.ErrorCode = 1
		reply.Err = "not leader"
	}
	select {
	case <-ch:
		return
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		reply.ErrorCode = 4
		reply.Err = "timeout"
		return
	}
}

func (kv *ShardKV) MigrateData(args *PutMigrationDataArgs, reply *PutMigrationDataReply) {
	if !kv.rf.IsLeader() {
		reply.ErrorCode = 1
		reply.Err = "not leader"
		return
	}
	if atomic.LoadInt64(&kv.IsConfigChanging) == 1 {
		reply.ErrorCode = 2
		reply.Err = "conflict"
		return
	}
	kv.mu.Lock()
	client, OpNum := SplitUUID(args.OpUUID)
	if earliest, ok := kv.History[client]; ok && earliest >= OpNum {
		kv.mu.Unlock()
		return
	}
	var ch chan struct{}
	var ok bool
	op := Op{}
	op.OpType = "DataMigration"
	op.OpArg = args
	op.OpUUID = args.OpUUID
	if ch, ok = kv.IsReady[op.OpUUID]; !ok {
		ch = make(chan struct{})
		kv.IsReady[op.OpUUID] = ch
	}
	command := EncodeOpt(op)
	kv.mu.Unlock()
	_, _, ok = kv.rf.Start(command)
	if !ok {
		reply.ErrorCode = 1
		reply.Err = "not leader"
	}
	select {
	case <-ch:
		return
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		reply.ErrorCode = 4
		reply.Err = "timeout"
		return
	}
}
