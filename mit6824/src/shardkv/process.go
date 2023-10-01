package shardkv

// init shard and map

func (kv *ShardKV) canServe(shardID int) bool {
	return kv.CurrentConfig.Shards[shardID] == kv.gid && (kv.StateMachine[shardID].State == Serving || kv.StateMachine[shardID].State == GCing)
}

func (kv *ShardKV) cleanNotifyChannel(notifyIdx string) {
	CloseCh(kv.IsReady[notifyIdx])
	delete(kv.IsReady, notifyIdx)
}

func (kv *ShardKV) processAppendPut(op *ClientReq, notifyIdx string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.cleanNotifyChannel(notifyIdx)

	args := DecodePutAppendArgs(op)

	opClient, opUUID := SplitUUID(args.UUID)
	hisID := kv.History[opClient]
	//请求去重
	if opUUID <= hisID {
		return
	}
	if !kv.CanServe(args.ShardID) {
		//fmt.Println(args.ShardID, args.UUID, kv.gid, kv.CurrentConfig.Shards[args.ShardID])
		//fmt.Println(kv.CurrentConfig)
		return
	}

	var res string
	switch args.Op {
	case "Put":
		key, value := args.Key, args.Value
		kv.StateMachine[args.ShardID].Data[key] = value
		res = value
	case "Append":
		key, value := args.Key, args.Value
		if s, ok := kv.StateMachine[args.ShardID].Data[key]; ok {
			kv.StateMachine[args.ShardID].Data[key] = s + value
			res = s + value
		} else {
			kv.StateMachine[args.ShardID].Data[key] = value
			res = value
		}
	}
	kv.RequestRes[opClient] = ""
	kv.History[opClient] = opUUID
	logger.Infof("KVServer %v execute op %v succeed res is %v", kv.me, op, res)

}

func (kv *ShardKV) processGet(op *ClientReq, notifyIdx string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.cleanNotifyChannel(notifyIdx)
	args := DecodeGetArgs(op)

	opClient, opUUID := SplitUUID(args.UUID)
	hisID := kv.History[opClient]
	if opUUID <= hisID {
		return
	}
	if !kv.CanServe(args.ShardID) {
		return
	}

	var res string
	key := args.Key
	if s, ok := kv.StateMachine[args.ShardID].Data[key]; !ok {
		res = ""
	} else {
		res = s
	}
	kv.RequestRes[opClient] = res
	kv.History[opClient] = opUUID
	logger.Infof("KVServer %v execute op %v succeed res is %v", kv.me, op, res)
}

func (kv *ShardKV) processDeleteShard(op *ClientReq, notifyIdx string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.cleanNotifyChannel(notifyIdx)
	args := DecodeDeleteShardArg(op)
	if args.ConfigNum != kv.CurrentConfig.Num {
		return
	}
	for _, ShardID := range args.ShardIDs {
		if kv.StateMachine[ShardID].State == GCing {
			kv.StateMachine[ShardID].State = Serving
		} else if kv.StateMachine[ShardID].State == BePulling {
			kv.StateMachine[ShardID] = NewShard()
		} else {
			return
		}
	}
}

func (kv *ShardKV) processConfigUpdate(op *ClientReq) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	args := DecodeUpdateConfigArg(op)
	if args.Config.Num != kv.CurrentConfig.Num+1 {
		return
	}
	kv.LastConfig, kv.CurrentConfig = kv.CurrentConfig, args.Config
	kv.SetShardStateByConfig()
	//for ShardID, Shard := range kv.StateMachine {
	//	fmt.Println(kv.CurrentConfig.Num, ShardID, Shard)
	//}
	//fmt.Println(kv.gid, "update config from", kv.LastConfig.Num, "to", kv.CurrentConfig.Num, "succeed")
	return
}

func (kv *ShardKV) processAddShard(op *ClientReq) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	args := DecodeAddShardArg(op)
	//fmt.Println("Add shard args", args)
	if args.ConfigNum != kv.CurrentConfig.Num {
		return
	}
	for ShardID, shard := range args.Shards {
		if kv.StateMachine[ShardID].State == Pulling {
			kv.StateMachine[ShardID] = shard
			shard.State = GCing
		}
	}
	for clientID, cmdID := range args.History {
		id, ok := kv.History[clientID]
		if !ok || ok && id < cmdID {
			kv.History[clientID] = cmdID
			kv.RequestRes[clientID] = args.RequestRes[clientID]
		}
	}
	//fmt.Println(kv.gid, "in config ", kv.CurrentConfig.Num, "Add shard args", args, "Done")
	return
}
