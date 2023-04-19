package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(args ...int) int {
	num := -1
	for i, val := range args {
		if i == 0 {
			num = val
			continue
		}
		if num > val {
			num = val
		}
	}
	return num
}
func max(args ...int) int {
	num := -1
	for i, val := range args {
		if i == 0 {
			num = val
			continue
		}
		if val > num {
			num = val
		}
	}
	return num
}
