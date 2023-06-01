package shardkv

import (
	"6824/labgob"
	"6824/shardctrler"
	"bytes"
	"go.uber.org/zap/buffer"
	"strconv"
	"strings"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type BeginTransactionArgs struct {
	UUID string
}

type BeginTransactionReply struct {
	ErrorCode int64
	Err       Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type ConfigChangeReply struct {
	ErrorCode int64
	Err       Err
}

type ConfigChangeArgs struct {
	Commands ConfigCommands
	UUID     string
}

type ConfigCommands struct {
	Commands []ConfigCommand
}

type PutMigrationDataArgs struct {
	Commands []DataMigrateCommand
	GId      int
	Servers  []string
	OpUUID   string
}

type PutMigrationDataReply struct {
	ErrorCode int64
	Err       Err
}

type DataMigrateCommand struct {
	Data      ShardMap
	ConfigNum int
}

type ConfigCommand struct {
	Shard   int
	Command string
	Num     int
}

type ShardMap struct {
	ShardNum int
	Map      map[string]string

	ConfigNum int
}

func NewShardMap(shardNum int) *ShardMap {
	s := &ShardMap{}
	s.ShardNum = shardNum
	s.Map = make(map[string]string)
	return s
}

func SplitUUID(uuid string) (string, int) {
	strs := strings.Split(uuid, " ")
	clientId := strs[0]
	clientOPID, _ := strconv.Atoi(strs[1])
	return clientId, clientOPID
}

func EncodeOpt(op Op) []byte {
	w := new(buffer.Buffer)
	en := labgob.NewEncoder(w)
	en.Encode(op)
	return w.Bytes()
}

func DecodeOpt(data []byte) *Op {
	op := &Op{}
	w := bytes.NewBuffer(data)
	decode := labgob.NewDecoder(w)
	decode.Decode(op)
	return op
}

func EncodeConfig(op shardctrler.Config) []byte {
	w := new(buffer.Buffer)
	en := labgob.NewEncoder(w)
	en.Encode(op)
	return w.Bytes()
}

func DecodeConfig(data []byte) *shardctrler.Config {
	op := &shardctrler.Config{}
	w := bytes.NewBuffer(data)
	decode := labgob.NewDecoder(w)
	decode.Decode(op)
	return op
}

func EncodeShardMap(op ShardMap) []byte {
	w := new(buffer.Buffer)
	en := labgob.NewEncoder(w)
	en.Encode(op)
	return w.Bytes()
}

func DecodeShardMap(data []byte) *ShardMap {
	op := &ShardMap{}
	w := bytes.NewBuffer(data)
	decode := labgob.NewDecoder(w)
	decode.Decode(op)
	return op
}

func init() {
	labgob.Register(ConfigCommands{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutMigrationDataArgs{})
	labgob.Register(PutMigrationDataReply{})
	labgob.Register(DataMigrateCommand{})
	labgob.Register(ConfigCommand{})
	labgob.Register(ShardMap{})
	labgob.Register(shardctrler.Config{})
}
