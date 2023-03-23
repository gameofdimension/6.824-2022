package raft

import "time"

func (rf *Raft) applyLog() {
	for {
		rf.mu.Lock()
		idle := true
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
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

func (rf *Raft) spawnWorker() {
	go rf.applyLog()
	rf.spawnLeaderWorker()
}

func (rf *Raft) replicateWorker(server int) {

}

func (rf *Raft) commitIndexWorker() {
	for rf.killed() == false {
		rc := rf.tryUpdateCommitIndex()
		if rc < 0 {
			time.Sleep(10 * time.Millisecond)
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (rf *Raft) spawnLeaderWorker() {
	go rf.commitIndexWorker()
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.replicateWorker(idx)
	}
}
