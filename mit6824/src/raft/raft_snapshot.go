package raft

import "fmt"

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

	// Your code here (2D).
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if index <= rf.LastIncludedIndex || index > rf.LastIncludedIndex+len(rf.Logs) {
			return
		}
		logger.Error(fmt.Sprintf("server %d is going to snapshot, "+
			"my current LastIncludedIndex is %d, my len(Log) is %d"+
			"expect snapshotIndex is %d",
			rf.me,
			rf.LastIncludedIndex,
			len(rf.Logs),
			index,
		))
		rf.persister.snapshot = clone(snapshot)
		LastIncludedTerm := rf.Logs[index-rf.LastIncludedIndex-1].Term
		rf.Logs = rf.Logs[index-rf.LastIncludedIndex:]
		rf.LastIncludedTerm = LastIncludedTerm
		rf.LastIncludedIndex = index
		rf.persist()
	}()

}

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int
	Message  ApplyMsg
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		rf.CloseStopChannel(rf.CurrentTerm)
		reply.Term = -1
		fmt.Printf("老子已经寄了别发了\n")
		return
	}

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	go func() {
		rf.RestartTicker <- struct{}{}
	}()

	rf.ServerState = 0
	rf.CloseStopChannel(rf.CurrentTerm)
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
	}
	if args.Message.SnapshotIndex > rf.LastIncludedIndex {
		logger.Error(fmt.Sprintf("server %v install snapshot, lastIndex is %v, len(Logs) is %v,  snapshotIndex is %v",
			rf.me,
			rf.LastIncludedIndex,
			len(rf.Logs),
			args.Message.SnapshotIndex,
		))
		rf.persister.snapshot = args.Message.Snapshot
		if rf.LastIncludedIndex+len(rf.Logs) > args.Message.SnapshotIndex {
			rf.Logs = rf.Logs[args.Message.SnapshotIndex-rf.LastIncludedIndex:]
		} else {
			rf.Logs = make([]LogEntry, 0)
		}
		rf.LastIncludedIndex = args.Message.SnapshotIndex
		rf.LastIncludedTerm = args.Message.SnapshotTerm
		rf.CommitIndex = max(rf.LastIncludedIndex, rf.CommitIndex)
		rf.ApplyChannel <- args.Message
		logger.Error(fmt.Sprintf("server %v has  installed snapshot, lastIndex is %v, len(Logs) is %v,  snapshotIndex is %v",
			rf.me,
			rf.LastIncludedIndex,
			len(rf.Logs),
			args.Message.SnapshotIndex,
		))
	}
	rf.persist()
	reply.Term = rf.CurrentTerm

	return

}
