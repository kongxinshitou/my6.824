package kvraft

import (
	"6824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

const (
	PUT_APPEND_TIME_OUT = 150
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	UUID := GetUUID()

	for {
		for _, server := range ck.servers {
			getArgs := &GetArgs{
				Key:  key,
				UUID: UUID,
			}
			getReply := &GetReply{}
			ch := make(chan bool)
			go func(getArgs *GetArgs, getReply *GetReply, ch chan bool) {
				ok := server.Call("KVServer.Get", getArgs, getReply)
				ch <- ok
			}(getArgs, getReply, ch)
			select {
			case <-time.After(time.Duration(PUT_APPEND_TIME_OUT * time.Millisecond)):
				go func(ch chan bool) {
					<-ch
				}(ch)
			case ok := <-ch:
				if !ok || getReply.Err != "" {
					continue
				}
				return getReply.Value
			}
		}
	}

	return ""
}

func GetUUID() string {
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	UUID := GetUUID()

	for {
		for _, server := range ck.servers {
			putAppendArgs := &PutAppendArgs{
				Key:   key,
				Value: value,
				Op:    op,
				UUID:  UUID,
			}
			putAppendReply := &PutAppendReply{}
			ch := make(chan bool)
			go func(putAppendArgs *PutAppendArgs, putAppendReply *PutAppendReply, ch chan bool) {
				ok := server.Call("KVServer.PutAppend", putAppendArgs, putAppendReply)
				ch <- ok
			}(putAppendArgs, putAppendReply, ch)
			select {
			case <-time.After(time.Duration(PUT_APPEND_TIME_OUT * time.Millisecond)):
				go func(ch chan bool) {
					<-ch
				}(ch)
			case ok := <-ch:
				if !ok || putAppendReply.Err != "" {
					continue
				}
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
