package kvraft

import (
	"6824/labgob"
	"bytes"
	"go.uber.org/zap/buffer"
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

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UUID string
}

type GetReply struct {
	Err   Err
	Value string
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
