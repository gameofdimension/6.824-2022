package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) applyLog() {
	round := 0
	for rf.killed() == false {
		round += 1
		rf.mu.Lock()
		rc, newVal, msg := rf.nextToApply(round)
		rf.lastApplied = newVal
		rf.mu.Unlock()
		if rc {
			rf.applyCh <- *msg
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) nextToApply(round int) (bool, int, *ApplyMsg) {
	roundId := fmt.Sprintf("APPLY%016d", round)
	prefix := fmt.Sprintf("%s %d of [%d,%d] progress [%d vs %d]",
		roundId, rf.me, rf.currentTerm, rf.role, rf.lastApplied, rf.commitIndex)
	if rf.lastApplied > rf.commitIndex {
		panic(fmt.Sprintf("%s lastApplied bigger than commitIndex", prefix))
	}
	if rf.lastApplied == rf.commitIndex {
		// DPrintf("%s no log to apply", prefix)
		return false, rf.lastApplied, nil
	}
	nextApplyIndex := rf.lastApplied + 1
	if nextApplyIndex <= rf.vlog.GetLastIncludedIndex() {
		DPrintf("%s next log %d snapshoted", prefix, nextApplyIndex)
		nextApplyIndex = rf.vlog.GetLastIncludedIndex()
		return false, nextApplyIndex, nil
	}
	nextItem := rf.vlog.GetItem(nextApplyIndex)
	DPrintf("%s will apply %d of %v", prefix, nextApplyIndex, *nextItem)
	msg := ApplyMsg{
		CommandValid: true,
		Command:      nextItem.Command,
		CommandIndex: nextApplyIndex,
	}
	return true, nextApplyIndex, &msg
}
