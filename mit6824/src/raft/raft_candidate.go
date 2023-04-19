package raft

import (
	"fmt"
	"sync"
	"time"
)

var (
	RequestVoteTimeOut = 50
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	logger.Info(fmt.Sprintf(" 在term%d的candidate%d向%d发起请求", args.Term, args.CandidateId, rf.me))
	defer rf.mu.Unlock()
	if rf.killed() {
		fmt.Println("killed")
		rf.CloseStopChannel(rf.CurrentTerm)
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	if rf.CurrentTerm == args.Term && rf.VotedFor == args.CandidateId {
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
		return
	}
	logger.Info(fmt.Sprintf("************%d收到来自于%d在term%d的投票请求\n", rf.me, args.CandidateId, rf.CurrentTerm))
	//1. if candidate's term is bigger than CurrentTerm, change to larger term
	if args.Term > rf.CurrentTerm {
		rf.CloseStopChannel(rf.CurrentTerm)
		rf.CurrentTerm = args.Term
		rf.ServerState = 0
		rf.VotedFor = -1
		rf.persist()
	}
	//2. if candidate's term is not bigger than CurrentTerm, set the reply.Term
	reply.Term = rf.CurrentTerm
	//3. reject if candidate's term is smaller , I'm not the follower , I have voted for others.
	if args.Term < rf.CurrentTerm || rf.ServerState != 0 || rf.VotedFor != -1 {
		logger.Info(fmt.Sprintf("**********%d不给给%d投票你赶紧爬\n", rf.me, args.CandidateId))
		reply.VoteGranted = false
		return
	}
	//4. check if candidate is up-to-date
	lastLogIndexC := len(rf.Logs) + rf.LastIncludedIndex
	var lastLogTermC int
	if len(rf.Logs) != 0 {
		lastLogTermC = rf.Logs[len(rf.Logs)-1].Term
	} else {
		lastLogTermC = rf.LastIncludedTerm
	}

	if lastLogTermC > args.LastLogTerm || lastLogTermC == args.LastLogTerm && lastLogIndexC > args.LastLogIndex {
		reply.VoteGranted = false
	} else {
		logger.Info(fmt.Sprintf("%d, state is %d, 给%d投票了\n", rf.me, rf.ServerState, args.CandidateId))
		rf.VotedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		go func() {
			rf.RestartTicker <- struct{}{}
		}()
	}
	return

}

func (rf *Raft) doCandidate(args RequestVoteArgs, stopChannel chan struct{}) {
	validLock := sync.RWMutex{}
	validCandidate := true
	votes := 1
	majority := len(rf.peers)/2 + 1
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				for !rf.killed() {
					select {
					case <-stopChannel:
						return
					default:
						//1.if the candidate state is invalid, return
						validLock.RLock()
						if validCandidate != true {
							validLock.RUnlock()
							return
						}
						validLock.RUnlock()
						reply := RequestVoteReply{}
						ch := make(chan bool)
						go func(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan bool) {
							ok := rf.sendRequestVote(server, args, reply)
							ch <- ok
						}(server, &args, &reply, ch)
						select {
						case <-time.After(50 * time.Millisecond):
							go func(ch chan bool) {
								<-ch
							}(ch)
							continue
						case ok := <-ch:
							if !ok {
								continue
							}
						}

						if reply.Term == -1 {
							logger.Info(fmt.Sprintf("***********server%d已经死亡，不需要继续发送请求\n", server))
							time.Sleep(time.Duration(RequestVoteTimeOut) * time.Millisecond)
							continue
						}
						select {
						case <-stopChannel:
							return
						default:
							//2.if the candidate state is valid, lock rf, validCandidate and votes
							validLock.Lock()
							rf.mu.Lock()
							select {
							case <-stopChannel:
								rf.mu.Unlock()
								validLock.Unlock()
								return
							default:

							}
							//2.1 check state if state is invalid, change invalid variable, return
							if rf.killed() || rf.ServerState != 1 || rf.CurrentTerm > args.Term {
								logger.Info(fmt.Sprintf("%d 的状态以及改变了，它现在的状态是%d,在%d向%d寻求投票的goroutine已经失效", rf.me, rf.ServerState, args.Term, server))
								validCandidate = false
								if !(rf.CurrentTerm == args.Term && rf.ServerState == 2) {
									rf.CloseStopChannel(args.Term)
								}
								rf.mu.Unlock()
								validLock.Unlock()

								return
							}
							//2.2 if reply.term is bigger, change rf.CurrentTerm and turn rf.ServerState to follower
							if reply.Term > rf.CurrentTerm {
								logger.Info(fmt.Sprintf("reply的任期是%d, 我的任期是%d我成为普通follower\n", reply.Term, rf.CurrentTerm))
								validCandidate = false
								rf.CurrentTerm = reply.Term
								rf.ServerState = 0
								rf.VotedFor = -1
								rf.persist()
								rf.CloseStopChannel(args.Term)
								rf.mu.Unlock()
								validLock.Unlock()

								return
							}
							//2.3 if win the vote, votes++ and check if win majority votes
							if reply.VoteGranted == true {
								votes++
								logger.Info(fmt.Sprintf("%d收到%d选票\n", rf.me, votes))
								//if candidate get majority votes,  be leader. And this doCandidate goroutine should be invalid. go doLeader goroutine
								if votes >= majority {
									logger.Warn(fmt.Sprintf("%d成为任期%d的领导\n", rf.me, rf.CurrentTerm))
									rf.ServerState = 2
									validCandidate = false
									go rf.doLeader(stopChannel, args.Term)
									rf.mu.Unlock()
									validLock.Unlock()

									return
								}
							}
							rf.mu.Unlock()
							validLock.Unlock()
							return

						}

					}

				}
			}(i)

		}
	}
}
