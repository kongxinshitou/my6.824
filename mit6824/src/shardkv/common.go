package shardkv

import (
	"6824/labgob"
	"6824/shardctrler"
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
	Serving = iota
	Pulling
	BePulling
	GCing
)

var (
	OK             = "OK"
	ErrOutOfDate   = "config out of date"
	ErrWrongLeader = "error wrong leader"
	ErrTimeout     = "error timeout"
	ErrWait        = "error wait"
	ErrWrongGroup  = "error wrong group"
)

type ClientReq struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	OpArg  any
}

func max(nums ...int) int {
	res := nums[0]
	for i := 0; i < len(nums); i++ {
		if nums[i] > res {
			res = nums[i]
		}
	}

	return res
}
func DeepCopyMapStringString(s map[string]string) map[string]string {
	if s == nil {
		return nil
	}

	res := map[string]string{}
	for k, v := range s {
		res[k] = v
	}

	return res
}

func DeepCopyStringInt(m map[string]int) map[string]int {
	if m == nil {
		return nil
	}

	res := map[string]int{}
	for k, v := range m {
		res[k] = v
	}

	return res

}

type Shard struct {
	Data  map[string]string
	State int
}

func (s *Shard) deepCopy() *Shard {
	res := &Shard{}
	res.Data = DeepCopyMapStringString(s.Data)
	res.State = s.State

	return res
}

func NewShard() *Shard {
	return &Shard{
		Data:  map[string]string{},
		State: Serving,
	}
}

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UUID    string
	ShardID int
}

type PutAppendReply struct {
	Err string
}

type GetReply struct {
	Err   string
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UUID    string
	ShardID int
}

type UpdateConfigArg struct {
	Config shardctrler.Config
}

type UpdateConfigReply struct {
	Err string
}

type GetShardArg struct {
	ShardIDs  []int
	ConfigNum int
}

type GetShardReply struct {
	Success    bool
	ConfigNum  int
	ShardIDs   []int
	Shards     map[int]*Shard
	RequestRes map[string]string
	History    map[string]int
	Err        string
}

func GetAddShardByGetShardReply(reply *GetShardReply) *AddShardArg {
	args := &AddShardArg{}
	args.ConfigNum = reply.ConfigNum
	args.Shards = reply.Shards
	args.History = reply.History
	args.RequestRes = reply.RequestRes
	args.ShardIDs = reply.ShardIDs

	return args
}

type AddShardArg struct {
	ConfigNum  int
	ShardIDs   []int
	Shards     map[int]*Shard
	RequestRes map[string]string
	History    map[string]int
}

type AddShardReply struct {
	Success bool
	Err     string
}

type DeleteShardArg struct {
	ConfigNum int
	ShardIDs  []int
}

type DeleteShardReply struct {
	Success bool
	Err     string
}

func SplitUUID(uuid string) (string, int) {
	strs := strings.Split(uuid, " ")
	clientId := strs[0]
	clientOPID, _ := strconv.Atoi(strs[1])
	return clientId, clientOPID
}

func EncodeAny(args interface{}) []byte {
	w := new(buffer.Buffer)
	en := labgob.NewEncoder(w)
	en.Encode(args)
	return w.Bytes()
}

func EncodeClientReq(req ClientReq) []byte {
	w := new(buffer.Buffer)
	en := labgob.NewEncoder(w)
	en.Encode(req)
	return w.Bytes()
}

func DecodeGetArgs(op *ClientReq) *GetArgs {
	arg := op.OpArg.(GetArgs)
	return &arg
}

func DecodePutAppendArgs(op *ClientReq) *PutAppendArgs {
	arg := op.OpArg.(PutAppendArgs)
	return &arg
}

func DecodeAddShardArg(op *ClientReq) *AddShardArg {
	arg := op.OpArg.(AddShardArg)
	return &arg
}

func DecodeUpdateConfigArg(op *ClientReq) *UpdateConfigArg {
	arg := op.OpArg.(UpdateConfigArg)
	return &arg
}

func DecodeDeleteShardArg(op *ClientReq) *DeleteShardArg {
	arg := op.OpArg.(DeleteShardArg)
	return &arg
}

func CloseCh(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
		return
	default:
		close(ch)
	}
}

func init() {
	labgob.Register(AddShardArg{})
	labgob.Register(AddShardReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(ClientReq{})
	labgob.Register(UpdateConfigArg{})
	labgob.Register(UpdateConfigReply{})
	labgob.Register(DeleteShardArg{})
	labgob.Register(DeleteShardReply{})
	labgob.Register(Shard{})
	labgob.Register(map[string]*Shard{})
	labgob.Register(map[string]string{})
	labgob.Register(map[string]int{})
	labgob.Register(shardctrler.Config{})
}
