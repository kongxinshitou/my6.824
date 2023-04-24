package raft

import (
	"fmt"
	"sort"
	"time"
)

var (
	AppendEntriesTimeOut = 20
)

// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
	// TODO set conflictTerm = -1, if get no conflictTerm, do it in AppendEntries RPC
	ConflictTermIndex int
}

// LeaderWorker for one term
func (rf *Raft) doLeader(stopChannel chan struct{}, currentTerm int) {
	select {
	case <-stopChannel:
		return
	default:
		rf.mu.Lock()
		select {
		case <-stopChannel:
			rf.mu.Unlock()
			return
		default:

		}
		//fmt.Printf("server %d become the leader of term %d\n", rf.me, rf.CurrentTerm)
		for i := 0; i < len(rf.NextIndex); i++ {
			rf.NextIndex[i] = len(rf.Logs) + rf.LastIncludedIndex + 1
			if i == rf.me {
				rf.MatchIndex[i] = len(rf.Logs) + rf.LastIncludedIndex
			} else {
				rf.MatchIndex[i] = 0
			}
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			// TODO those logics can be packaged to a function, as it need to be called twice
			go rf.ServeAsLeader(i, currentTerm, stopChannel)
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) GetCurrentN() int {
	majority := len(rf.peers)/2 + 1
	copyMatches := make([]int, len(rf.MatchIndex))
	copy(copyMatches, rf.MatchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatches)))
	return copyMatches[majority-1]

}

func (rf *Raft) ServeAsLeader(server int, currentTerm int, stopChannel chan struct{}) {
	//1.logic to set wait time
	isHeartBeat := false
	isSleep := true
	for !rf.killed() {
		isHeartBeat = false
		if isSleep {
			time.Sleep(time.Duration(AppendEntriesTimeOut) * time.Millisecond)
		} else {
			isSleep = true
		}

		//2.Check is valid
		select {
		case <-stopChannel:
			return
		default:
			rf.mu.Lock()
			if rf.killed() || rf.ServerState != 2 || rf.CurrentTerm > currentTerm {
				rf.CloseStopChannel(currentTerm)
				rf.mu.Unlock()
				return
			}
			//3.set the args for appendEntries

			//TODO the logic of InstallSnapShot

			if rf.NextIndex[server] > rf.LastIncludedIndex {
				if len(rf.Logs)+rf.LastIncludedIndex < rf.NextIndex[server] {
					isHeartBeat = true
				}
				args := AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.CommitIndex,
					PrevLogIndex: rf.NextIndex[server] - 1,
				}
				if args.PrevLogIndex == 0 {
					args.PrevLogTerm = 0
				} else {
					//fmt.Println("********the current len(Logs) is ", len(rf.Logs))
					if args.PrevLogIndex > rf.LastIncludedIndex {
						args.PrevLogTerm = rf.Logs[args.PrevLogIndex-rf.LastIncludedIndex-1].Term
					} else {
						args.PrevLogTerm = rf.LastIncludedTerm
					}

				}

				if !isHeartBeat {
					entries := make([]LogEntry, 0)
					//entriesTerm := rf.Logs[args.PrevLogIndex-rf.LastIncludedIndex].Term
					for i := args.PrevLogIndex - rf.LastIncludedIndex; i < len(rf.Logs); i++ {
						//if rf.Logs[i].Term != entriesTerm {
						//	break
						//}
						entries = append(entries, rf.Logs[i])
					}
					args.Entries = entries
					//fmt.Printf("%d向%d发送从index%d到index为%d的entries了\n", rf.me, server, rf.NextIndex[server], rf.NextIndex[server]+len(entries)-1)
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ch := make(chan bool)
				go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, ch chan bool) {
					ok := rf.sendAppendEntries(server, args, reply)
					ch <- ok
				}(server, &args, &reply, ch)
				select {
				case <-time.After(70 * time.Millisecond):
					go func(ch chan bool) {
						<-ch
					}(ch)
					logger.Info(fmt.Sprintf("learder %d in the term %d is sending appendEntries to %d, timeout", rf.me, rf.CurrentTerm, server))
					isSleep = false
					continue
				case ok := <-ch:
					if !ok {
						logger.Info(fmt.Sprintf("learder %d in the term %d is sending appendEntries to %d, timeout", rf.me, rf.CurrentTerm, server))
						continue
					}
				}
				logger.Info(fmt.Sprintf("learder %d in the term %d is sending appendEntries to %d, succeed", rf.me, rf.CurrentTerm, server))
				if reply.Term == -1 {
					continue
				}
				//1.1 TODO change the state of nextIndex and matchIndex
				//1.2 TODO if get a true reply, commit the index. In a another goroutine(another channel in raft needed) or embed in this function(another lock needed)
				select {
				case <-stopChannel:
					return
				default:
					rf.mu.Lock()
					select {
					case <-stopChannel:
						rf.mu.Unlock()
						return
					default:
					}
					//4. check whether the validLeader has changed
					if rf.killed() || rf.ServerState != 2 || rf.CurrentTerm > currentTerm {
						rf.CloseStopChannel(currentTerm)
						rf.mu.Unlock()
						return
					}
					if reply.Term > currentTerm {
						rf.CurrentTerm = reply.Term
						rf.VotedFor = -1
						rf.ServerState = 0
						rf.persist()
						rf.CloseStopChannel(currentTerm)
						rf.mu.Unlock()
						return
					}
					if reply.ConflictTermIndex != 0 {
						rf.NextIndex[server] = reply.ConflictTermIndex
					}
					if !isHeartBeat && reply.Success == true {
						rf.MatchIndex[server] = rf.NextIndex[server] + len(args.Entries) - 1
						rf.NextIndex[server] = rf.MatchIndex[server] + 1
						N := rf.GetCurrentN()
						if N > rf.CommitIndex && rf.Logs[N-rf.LastIncludedIndex-1].Term == rf.CurrentTerm {
							for j := rf.CommitIndex + 1; j <= N; j++ {
								rf.ApplyChannel <- rf.Logs[j-rf.LastIncludedIndex-1].Message
							}
							logger.Error(fmt.Sprintf("leader %v commmit log index is from %d to %d", rf.me, rf.CommitIndex+1, N))
							rf.CommitIndex = N
						}
					}
					rf.mu.Unlock()

				}
			} else {
				args := InstallSnapshotArgs{
					Term:     rf.CurrentTerm,
					LeaderId: rf.me,
					Message: ApplyMsg{
						SnapshotValid: true,
						Snapshot:      rf.persister.snapshot,
						SnapshotIndex: rf.LastIncludedIndex,
						SnapshotTerm:  rf.LastIncludedTerm,
					},
				}
				rf.mu.Unlock()
				reply := InstallSnapshotReply{}
				ch := make(chan bool)
				logger.Error(fmt.Sprintf("server %v send instalsnapshoyt to %v, the snapshotIndex is %v",
					rf.me,
					server,
					args.Message.SnapshotIndex,
				))
				go func(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, ch chan bool) {
					ok := rf.sendInstallSnapshot(server, args, reply)
					ch <- ok
				}(server, &args, &reply, ch)
				select {
				case <-time.After(70 * time.Millisecond):
					go func(ch chan bool) {
						<-ch
					}(ch)
					isSleep = false
					continue
				case ok := <-ch:
					if !ok {
						continue
					}
				}
				logger.Error(fmt.Sprintf("server %v send instalsnapshoyt to %v, the snapshotIndex is %v succeed",
					rf.me,
					server,
					args.Message.SnapshotIndex,
				))
				select {
				case <-stopChannel:
					return
				default:
					rf.mu.Lock()
					select {
					case <-stopChannel:
						return
					default:
					}
					if reply.Term > currentTerm {
						rf.CurrentTerm = reply.Term
						rf.VotedFor = -1
						rf.ServerState = 0
						rf.persist()
						rf.CloseStopChannel(currentTerm)
						rf.mu.Unlock()
						return
					}

					rf.NextIndex[server] = args.Message.SnapshotIndex + 1
					rf.MatchIndex[server] = args.Message.SnapshotIndex
					rf.mu.Unlock()
				}

			}

		}

	}
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//1.if killed return false, and set reply.Term = -1 to indicate that the receiver has been killed

	rf.mu.Lock()
	logger.Info(fmt.Sprintf("%d in the term %d get the AppendEntries request for leader %d. leader's term term is %d", rf.me, rf.CurrentTerm, args.LeaderId, args.Term))
	defer rf.mu.Unlock()
	if rf.killed() {
		rf.CloseStopChannel(rf.CurrentTerm)
		reply.Term = -1
		reply.Success = false
		return
	}
	//2. Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	//3. Valid AppendEntries, 1. change the CurrentTerm and ServerState, 2. RestartTicker
	go func() {
		rf.RestartTicker <- struct{}{}
	}()

	rf.CloseStopChannel(rf.CurrentTerm)
	if args.Term > rf.CurrentTerm || rf.ServerState != 0 {
		rf.ServerState = 0
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.VotedFor = -1
		}
		rf.persist()
	}

	reply.Term = args.Term
	//4.TODO get the term of the conflicting entry and the first index it stores for that term
	//Reply false if log doesn't contain an entry at preLogIndex whose term matches preLogTerm.
	//There is a implement problem that the heartBeat condition is len(args.Entries) == 0, need to be repaired

	// get corresponding index, three case
	//1. < rf.LastIncludeIndex, it is possible, because the rpc maybe out of the order
	//2. rf.LastIncludeIndex <= args.preLogIndex <= len(rf.Log) + rf.LastIncludeIndex. it is possible, when it comes to an isolated leader.
	// mismatch term should be min{args.preLogTerm, the term of the corresponding index}
	//3. args.preLogIndex > len(rf.Log) + rf.LastIncludeIndex. It is possible too, then the mismatched term should the
	// mismatch term should be min{args.preLogTerm, the term of the last index}
	//4. reverse to find the last index of the term
	//**************************EveryThing is ok, It is time to insert Log Entries
	reply.Success = true
	if args.PrevLogIndex < rf.LastIncludedIndex {
		reply.ConflictTermIndex = rf.LastIncludedIndex + 1
		reply.Success = false
		return
	}

	if len(rf.Logs) == 0 && !(rf.LastIncludedIndex == args.PrevLogIndex && rf.LastIncludedTerm == args.PrevLogTerm) {
		reply.ConflictTermIndex = rf.LastIncludedIndex + 1
		reply.Success = false

		return
	}
	var mismatchTerm int
	reverseStart := len(rf.Logs) - 1
	if args.PrevLogIndex > len(rf.Logs)+rf.LastIncludedIndex {
		mismatchTerm = min(args.PrevLogTerm, rf.Logs[len(rf.Logs)-1].Term)

		reply.Success = false
	} else if args.PrevLogIndex > rf.LastIncludedIndex {
		correspondingTerm := rf.Logs[args.PrevLogIndex-rf.LastIncludedIndex-1].Term
		if correspondingTerm != args.PrevLogTerm {
			reply.Success = false
			mismatchTerm = min(args.PrevLogTerm, correspondingTerm)
		}
	}
	if reply.Success == false {
		for ; reverseStart >= 0 && rf.Logs[reverseStart].Term >= mismatchTerm && reverseStart+2+rf.LastIncludedIndex > rf.CommitIndex+1; reverseStart-- {
		}
		reply.ConflictTermIndex = reverseStart + 2 + rf.LastIncludedIndex

		logger.Error(fmt.Sprintf("follower %v, len(log) is %v, commit index is %v, leader is %v, len(entries) is %v, commit index is %v, previndex is %v, prevTerm is %v, conflictIndex is %v, conflictTerm is %v",
			rf.me,
			len(rf.Logs),
			rf.CommitIndex,
			args.LeaderId,
			len(args.Entries),
			args.LeaderCommit,
			args.PrevLogIndex,
			args.PrevLogTerm,
			reply.ConflictTermIndex,
			mismatchTerm,
		))
		return
	}

	//5.TODO if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it

	if len(args.Entries) > 0 {
		logger.Error(fmt.Sprintf("follower %v, len(log) is %v, lastIncludeIndex is %v, commit index is %v, leader is %v, len(entries) is %v, commit index is %v, previndex is %v, prevTerm is %v",
			rf.me,
			len(rf.Logs),
			rf.LastIncludedIndex,
			rf.CommitIndex,
			args.LeaderId,
			len(args.Entries),
			args.LeaderCommit,
			args.PrevLogIndex,
			args.PrevLogTerm,
		))
		startIndex := args.PrevLogIndex - rf.LastIncludedIndex

		for i := 0; i < len(args.Entries); i++ {
			if startIndex+i < len(rf.Logs) && rf.Logs[startIndex+i].Term == args.Entries[i].Term {
				continue
			}
			rf.Logs = rf.Logs[:startIndex+i]
			rf.Logs = append(rf.Logs, args.Entries[i:]...)
			break
		}
		args.PrevLogIndex = args.PrevLogIndex + len(args.Entries)
		rf.persist()
	}
	//if len(args.Entries) > 0 {
	//	startIndex := args.PrevLogIndex - rf.LastIncludedIndex
	//	rf.Logs = rf.Logs[:startIndex]
	//	rf.Logs = append(rf.Logs, args.Entries...)
	//	rf.persist()
	//	args.PrevLogIndex = args.PrevLogIndex + len(args.Entries)
	//}

	//5.1 TODO implement the idempotence
	//6.TODO if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	//fmt.Printf("%d*********currentLog%v\n", rf.me, rf.Logs)

	if args.LeaderCommit > rf.CommitIndex {

		commitIndex := min(args.LeaderCommit, args.PrevLogIndex)
		logger.Warn(fmt.Sprintf("server %v, commitIndex is %v, len(logs) is %v, lastIncludeIndex is %v,leader %v,  commitIndex is %v, preLogIndex is %v", rf.me, rf.CommitIndex, len(rf.Logs), rf.LastIncludedIndex, args.LeaderId, args.LeaderCommit, args.PrevLogIndex))
		if commitIndex > rf.CommitIndex {
			for i := rf.CommitIndex + 1; i <= commitIndex; i++ {
				rf.ApplyChannel <- rf.Logs[i-rf.LastIncludedIndex-1].Message

			}
			logger.Error(fmt.Sprintf("folloer %v commmit log index is from %d to %d", rf.me, rf.CommitIndex+1, commitIndex))
			rf.CommitIndex = commitIndex
		}
		rf.persist()
	}

	return
}
