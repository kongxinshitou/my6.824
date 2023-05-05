package kvraft

import (
	"6824/labrpc"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

const (
	PUT_APPEND_TIME_OUT = 70
)

var (
	pid string

	clerkNum int64
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	LastServer int
	clerkID    int64
	requestNum int64
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
	ck.LastServer = -1
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
	if ck.clerkID == 0 {
		atomic.CompareAndSwapInt64(&ck.clerkID, 0, atomic.AddInt64(&clerkNum, 1))
	}
	UUID := ck.GetReqUUID()
	logger.Infof("Send Get opt UUID is %v, key is %v", UUID, key)
	if ck.LastServer != -1 {
		server := ck.servers[ck.LastServer]
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
		case <-time.After(PUT_APPEND_TIME_OUT * time.Millisecond):
			go func(ch chan bool) {
				<-ch
			}(ch)
		case ok := <-ch:
			if ok && getReply.Err == "" {
				logger.Infof("Op id is %v, Op is %v, key is %v, value is %v", UUID, "Get", key, getReply.Value)
				return getReply.Value
			}
		}
	}
	for {
		for i, server := range ck.servers {
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
			case <-time.After(PUT_APPEND_TIME_OUT * time.Millisecond):
				go func(ch chan bool) {
					<-ch
				}(ch)
			case ok := <-ch:
				if !ok || getReply.Err != "" {
					continue
				}
				ck.LastServer = i
				logger.Infof("Op id is %v, Op is %v, key is %v, value is %v", UUID, "Get", key, getReply.Value)
				return getReply.Value
			}
		}
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) GetReqUUID() string {
	return strconv.FormatInt(ck.clerkID, 10) + " " + strconv.FormatInt(atomic.AddInt64(&ck.requestNum, 1), 10)
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	if ck.clerkID == 0 {
		atomic.CompareAndSwapInt64(&ck.clerkID, 0, atomic.AddInt64(&clerkNum, 1))
	}
	UUID := ck.GetReqUUID()
	logger.Infof("Begin to send Put UUID is %v, key is %v, value is %v", UUID, key, value)
	if ck.LastServer != -1 {
		server := ck.servers[ck.LastServer]
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
		case <-time.After(PUT_APPEND_TIME_OUT * time.Millisecond):
			go func(ch chan bool) {
				<-ch
			}(ch)
		case ok := <-ch:
			if ok && putAppendReply.Err == "" {
				logger.Infof("Op id is %v, Op is %v, key is %v, value is %v", UUID, op, key, value)
				return
			}
		}
	}
	for {
		for i, server := range ck.servers {
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
			case <-time.After(PUT_APPEND_TIME_OUT * time.Millisecond):
				go func(ch chan bool) {
					<-ch
				}(ch)
			case ok := <-ch:
				if !ok || putAppendReply.Err != "" {
					continue
				}
				ck.LastServer = i
				logger.Infof("Op id is %v, Op is %v, key is %v, value is %v", UUID, op, key, value)
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

func init() {
	pid = strconv.Itoa(os.Getegid())
}
