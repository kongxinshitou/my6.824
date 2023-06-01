package shardctrler

//
// Shardctrler clerk.
//

import (
	"6824/labrpc"
	"strconv"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clerkID    int64
	requestNum int64
}

var (
	pid      string
	clerkNum int64
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	if ck.clerkID == 0 {
		atomic.CompareAndSwapInt64(&ck.clerkID, 0, atomic.AddInt64(&clerkNum, 1))
	}
	UUID := ck.GetReqUUID()
	args.UUID = UUID
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	if ck.clerkID == 0 {
		atomic.CompareAndSwapInt64(&ck.clerkID, 0, atomic.AddInt64(&clerkNum, 1))
	}
	UUID := ck.GetReqUUID()
	args.UUID = UUID
	args.Servers = servers
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {

				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	if ck.clerkID == 0 {
		atomic.CompareAndSwapInt64(&ck.clerkID, 0, atomic.AddInt64(&clerkNum, 1))
	}
	UUID := ck.GetReqUUID()
	args.UUID = UUID
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {

				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	if ck.clerkID == 0 {
		atomic.CompareAndSwapInt64(&ck.clerkID, 0, atomic.AddInt64(&clerkNum, 1))
	}
	UUID := ck.GetReqUUID()
	args.UUID = UUID
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) GetReqUUID() string {
	return "K" + strconv.FormatInt(ck.clerkID, 10) + " " + strconv.FormatInt(atomic.AddInt64(&ck.requestNum, 1), 10)
}
