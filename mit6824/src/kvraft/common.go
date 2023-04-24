package kvraft

import (
	"6824/labgob"
	"bytes"
	"fmt"
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
