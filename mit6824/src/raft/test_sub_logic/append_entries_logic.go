package raft_sub_logic

import (
	"6824/labgob"
	"bytes"
)

type Log struct {
	Term int
}

type Raft struct {
	Logs              []Log
	LastIncludedIndex int
	LastIncludedTerm  int
}

type Para struct {
	PrevLogIndex int
	PrevLogTerm  int
}

type Response struct {
	Success           bool
	ConflictTermIndex int
}

func min(values ...int) int {
	minValue := 0x7fffffff
	for _, value := range values {
		if value < minValue {
			minValue = value
		}
	}
	return minValue
}

func FindMismatch(args *Para, rf *Raft, reply *Response) {
	reply.Success = true
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
		for ; reverseStart >= 0 && rf.Logs[reverseStart].Term >= mismatchTerm; reverseStart-- {
		}
		reply.ConflictTermIndex = reverseStart + 2 + rf.LastIncludedIndex
		return
	}

}

func Mashal(rf Raft) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.Logs)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	data := w.Bytes()
	return data
}

func UnMashal(data []byte) Raft {
	rf := Raft{}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Logs []Log
	var LastIncludedIndex int
	var LastIncludedTerm int
	if d.Decode(&Logs) != nil || d.Decode(&LastIncludedIndex) != nil || d.Decode(&LastIncludedTerm) != nil {
		panic("Decode persistence state failed")
	} else {
		rf.Logs = Logs
		rf.LastIncludedIndex = LastIncludedIndex
		rf.LastIncludedTerm = LastIncludedTerm
	}
	return rf
}
