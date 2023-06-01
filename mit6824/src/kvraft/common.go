package kvraft

import (
	"6824/labgob"
	"bytes"
	"go.uber.org/zap/buffer"
	"strconv"
	"strings"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UUID string
}

type PutAppendReply struct {
	Err Err
}

type GetReply struct {
	Err   Err
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UUID string
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

func CombineUUID(client, op int) string {
	return strconv.Itoa(client) + " " + strconv.Itoa(op)
}

func SplitUUID(uuid string) (int, int) {
	strs := strings.Split(uuid, " ")
	clientId, _ := strconv.Atoi(strs[0])
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
