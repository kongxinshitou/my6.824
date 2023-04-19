package raft_sub_logic_test

import (
	raft_sub_logic "6824/raft/test_sub_logic"
	"fmt"
	"testing"
)

func TestAppendEntiresFindMisMatch(t *testing.T) {
	rf := raft_sub_logic.Raft{
		LastIncludedIndex: 4,
		LastIncludedTerm:  3,
		Logs:              []raft_sub_logic.Log{{4}, {4}, {5}, {5}},
	}
	args := raft_sub_logic.Para{7, 6}
	reply := raft_sub_logic.Response{}
	raft_sub_logic.FindMismatch(&args, &rf, &reply)
	t.Log(reply)
	args.PrevLogTerm = 5
	reply = raft_sub_logic.Response{}
	raft_sub_logic.FindMismatch(&args, &rf, &reply)
	t.Log(reply)
	args.PrevLogTerm = 9
	reply = raft_sub_logic.Response{}
	raft_sub_logic.FindMismatch(&args, &rf, &reply)
	t.Log(reply)
	args.PrevLogTerm = 4
	reply = raft_sub_logic.Response{}
	raft_sub_logic.FindMismatch(&args, &rf, &reply)
	t.Log(reply)
	rf = raft_sub_logic.Raft{
		LastIncludedIndex: 4,
		LastIncludedTerm:  3,
	}
	args.PrevLogIndex = 4
	args.PrevLogTerm = 3
	reply = raft_sub_logic.Response{}
	raft_sub_logic.FindMismatch(&args, &rf, &reply)
	t.Log(reply)
}

func TestMashal(t *testing.T) {
	rf := raft_sub_logic.Raft{
		LastIncludedIndex: 4,
		LastIncludedTerm:  3,
		Logs:              []raft_sub_logic.Log{{4}, {4}, {5}, {5}},
	}
	data := raft_sub_logic.Mashal(rf)
	rf2 := raft_sub_logic.UnMashal(data)
	fmt.Println(rf2)
}
