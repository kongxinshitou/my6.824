package shardkv

import "6824/shardctrler"

func (kv *ShardKV) initShardMapByConfig(config shardctrler.Config) {
	kv.currentConfig = config
	kv.currentConfigNum = int64(config.Num)
	for i := 0; i < len(config.Shards); i++ {
		if config.Shards[i] == kv.gid {
			kv.shardsMap[i] = NewShardMap(i)
		}
	}

}

func (kv *ShardKV) processKVCommand(op *Op) {

}

func (kv *ShardKV) processSnapshot(snapshot []byte) {

}

func (kv *ShardKV) processDataMigration(op *Op) {

}

func (kv *ShardKV) processConfig(op *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	config := op.OpArg.(shardctrler.Config)

	if kv.currentConfigNum == 0 {
		kv.initShardMapByConfig(config)
		return
	}

	keys := make([]int, 0)
	for key := range kv.shardsMap {
		keys = append(keys, key)
	}
	for _, key := range keys {
		shardId := config.Shards[key]
		if shardId == kv.gid {
			continue
		}
		for _, server := range config.Groups[shardId] {
			args := &MigrateArgs{}

			reply := &MigrateReply{}
			end := kv.make_end(server)
			end.Call("ShardKV.Get", args, reply)
		}
	}
}
