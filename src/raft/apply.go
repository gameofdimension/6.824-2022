package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) applyLog() {
	for rf.killed() == false {
		rf.mu.Lock()
		rc, newVal, msg := rf.nextToApply()
		rf.lastApplied = newVal
		rf.mu.Unlock()
		if rc {
			rf.applyCh <- *msg
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) nextToApply() (bool, int, *ApplyMsg) {
	if rf.lastApplied > rf.commitIndex {
		panic(fmt.Sprintf("lastApplied bigger than commitIndex %d vs %d", rf.lastApplied, rf.commitIndex))
	}
	if rf.lastApplied == rf.commitIndex {
		return false, rf.lastApplied, nil
	}
	nextApplyIndex := rf.lastApplied + 1
	if nextApplyIndex <= rf.vlog.GetLastIncludedIndex() {
		nextApplyIndex = rf.vlog.GetLastIncludedIndex()
		return false, nextApplyIndex, nil
	}
	nextItem := rf.vlog.GetItem(nextApplyIndex)
	DPrintf("%d apply %d vs %d, %v", rf.me, rf.lastApplied, rf.commitIndex, nextItem)
	msg := ApplyMsg{
		CommandValid: true,
		Command:      nextItem.Command,
		CommandIndex: nextApplyIndex,
	}
	return true, nextApplyIndex, &msg
}
