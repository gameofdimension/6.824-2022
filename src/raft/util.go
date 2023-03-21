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

func isMoreUpToDate(aTerm int, aIndex int, bTerm int, bIndex int) bool {
	if aTerm > bTerm {
		return true
	}
	if aTerm < bTerm {
		return false
	}
	return aIndex >= bIndex
}
