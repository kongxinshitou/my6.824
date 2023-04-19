package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6824/labgob"
	"6824/labrpc"
)

var (
	logger *zap.Logger
)

// Utils

func (rf *Raft) CloseStopChannel(Term int) {
	if Term == 0 {
		return
	}
	if rf.StopChannels == nil {
		return
	}
	if _, ok := rf.StopChannels[Term]; !ok {
		return
	}
	select {
	case <-rf.StopChannels[Term]:
	default:
		close(rf.StopChannels[Term])
	}
	return
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// define the logentry

type LogEntry struct {
	Message ApplyMsg
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//signal channels
	RestartTicker chan struct{}         //用来充值election timeout的
	StopChannels  map[int]chan struct{} //用来终止当前term的doCandidate和doLeader的

	//the state of server
	ServerState int // 0 denote follower, 1 denote candidate, 2 denote leader
	// The persistent state(for simplicity, I don't implement the real persistent)
	CurrentTerm       int
	VotedFor          int
	Logs              []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
	// The volatile state on the servers
	CommitIndex int
	LastApplied int

	// The volatile state on leaders
	NextIndex  []int
	MatchIndex []int

	//appChannel
	ApplyChannel chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isLeader = rf.ServerState == 2
	return term, isLeader
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2B).
	index = rf.LastIncludedIndex + len(rf.Logs) + 1
	term = rf.CurrentTerm
	if rf.ServerState != 2 {
		return index, term, false
	}
	rf.Logs = append(rf.Logs, LogEntry{
		Message: ApplyMsg{
			Command:      command,
			CommandValid: true,
			CommandIndex: index,
		},
		Term: term,
	})
	rf.persist()
	rf.MatchIndex[rf.me] = len(rf.Logs) + rf.LastIncludedIndex

	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	L1:
		select {
		case <-rf.RestartTicker:
		case <-time.After(time.Duration(800+rand.Int63n(200)) * time.Millisecond):
			rf.mu.Lock()
			select {
			case <-rf.RestartTicker:
				rf.mu.Unlock()
				break L1
			default:

			}
			if rf.ServerState == 2 {
				logger.Info(fmt.Sprintf(" raft-server %d, state is %d time is used, but still leader, term is %d", rf.me, rf.ServerState, rf.CurrentTerm))
				rf.mu.Unlock()
				continue
			}
			rf.CloseStopChannel(rf.CurrentTerm)

			rf.ServerState = 1
			rf.CurrentTerm += 1
			rf.StopChannels[rf.CurrentTerm] = make(chan struct{})
			stopChannel := rf.StopChannels[rf.CurrentTerm]
			logger.Info(fmt.Sprintf("%d become the candidate of term %d", rf.me, rf.CurrentTerm))
			args := RequestVoteArgs{
				Term:        rf.CurrentTerm,
				CandidateId: rf.me,
			}
			if len(rf.Logs) == 0 {
				args.LastLogIndex = rf.LastIncludedIndex
				args.LastLogTerm = rf.LastIncludedTerm
			} else {
				args.LastLogIndex = rf.LastIncludedIndex + len(rf.Logs)
				args.LastLogTerm = rf.Logs[len(rf.Logs)-1].Term
			}

			rf.VotedFor = rf.me
			go rf.doCandidate(args, stopChannel)
			rf.mu.Unlock()

		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.Logs = []LogEntry{}
	rf.VotedFor = -1
	rf.RestartTicker = make(chan struct{})
	rf.StopChannels = make(map[int]chan struct{})
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	rf.ApplyChannel = applyCh
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex[i] = 1
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.CommitIndex = rf.LastIncludedIndex
	logger.Debug(fmt.Sprintf("**************大家好我是raft-server: NO %d, 我的当前任期为%d\n", rf.me, rf.CurrentTerm))
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func init() {
	rawJSON := []byte(`{
   "level": "panic",
   "encoding": "console",
   "outputPaths": ["stdout"],
   "errorOutputPaths": ["stderr"],
   "encoderConfig": {
     "messageKey": "message",
     "levelKey": "level",
     "levelEncoder": "lowercase"
   }
 }`)
	var cfg zap.Config
	var err error
	if err = json.Unmarshal(rawJSON, &cfg); err != nil {
		panic(err)
	}
	logger, err = cfg.Build()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()
}
