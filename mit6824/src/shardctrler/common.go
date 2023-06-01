package shardctrler

import (
	"6824/labgob"
	"bytes"
	"fmt"
	"go.uber.org/zap/buffer"
	"sort"
	"strconv"
	"strings"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	UUID    string
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	UUID string
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	UUID  string
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num  int // desired config number
	UUID string
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func NewDefaultConfig() Config {
	return Config{
		Groups: map[int][]string{},
	}
}

func (c *Config) AppendGroups(groups map[int][]string) {
	for key, value := range groups {
		c.Groups[key] = value
	}
}

// This implementation is enough, but in general cases should use R/B Tree to implement consistent hash
func (c *Config) Rehash() {
	if len(c.Groups) == 0 {
		c.Shards = [NShards]int{}
		return
	}
	keys := make([]int, len(c.Groups))
	i := 0
	for key := range c.Groups {
		keys[i] = key
		i++
	}
	sort.Ints(keys)
	j := 0
	for i := 0; i < NShards; i++ {
		c.Shards[i] = keys[j%len(keys)]
		j++
	}
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

func max(values ...int) int {
	res := values[0]
	for i := 1; i < len(values); i++ {
		if values[i] > res {
			res = values[i]
		}
	}
	return res
}

func (logger MyLogger) Infof(format string, a ...any) {
	logger.logger.Info(fmt.Sprintf(format, a...))
}

func (logger MyLogger) Debugf(format string, a ...any) {
	logger.logger.Debug(fmt.Sprintf(format, a...))
}
func (logger MyLogger) Errorf(format string, a ...any) {
	logger.logger.Error(fmt.Sprintf(format, a...))
}

func (logger MyLogger) Errorln(a ...any) {
	logger.logger.Error(fmt.Sprintln(a...))
}

func (logger MyLogger) Warnf(format string, a ...any) {
	logger.logger.Warn(fmt.Sprintf(format, a...))
}

func SplitUUID(uuid string) (string, int) {
	strs := strings.Split(uuid, " ")
	clientId := strs[0]
	clientOPID, _ := strconv.Atoi(strs[1])
	return clientId, clientOPID
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

func isClosed(ch chan struct{}) bool {
	if ch == nil {
		return true
	}
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func init() {
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
}
