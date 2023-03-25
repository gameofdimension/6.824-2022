package raft

import "time"

func (rf *Raft) applyLog() {
	for rf.killed() == false {
		rf.mu.Lock()
		idle := true
		if rf.commitIndex > rf.lastApplied {
			DPrintf("%d apply %d vs %d, %v", rf.me, rf.lastApplied, rf.commitIndex, rf.vlog.GetItem(rf.lastApplied+1))
			rf.lastApplied += 1
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.vlog.GetItem(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			idle = false
		}
		rf.mu.Unlock()
		if idle {
			time.Sleep(10 * time.Millisecond)
		}
	}
}
