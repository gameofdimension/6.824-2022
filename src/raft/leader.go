package raft

import "time"

func (rf *Raft) sendHeartBeat() {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			entries := make([]LogEntry, 0)
			logIdx := len(rf.log) - 1
			logTerm := rf.log[logIdx].Term
			args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me,
				PrevLogIndex: logIdx, PrevLogTerm: logTerm, Entries: entries,
				LeaderCommit: rf.commitIndex}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, &args, &reply)
		}(idx)
	}
}

func (rf *Raft) syncLog(server int, leaderId int, currentTerm int,
	leaderCommit int, entries *[]LogEntry, preLogIndex int, preLogTerm int) {
	args := AppendEntriesArgs{Term: currentTerm, LeaderId: leaderId, PrevLogIndex: preLogIndex,
		PrevLogTerm: preLogTerm, Entries: *entries, LeaderCommit: leaderCommit}
	reply := AppendEntriesReply{}
	rc := rf.sendAppendEntries(server, &args, &reply)
	if !rc {
		DPrintf("sendAppendEntries rpc fail %t", rc)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		
	}
	if !reply.Success {
		DPrintf("sendAppendEntries process fail %t", reply.Success)
	}
}

func (rf *Raft) replicate(startTerm int) {
	for {
		rf.mu.Lock()
		role := rf.role
		self := rf.me
		currentTerm := rf.currentTerm
		leaderCommit := rf.commitIndex
		// https://github.com/go101/go101/wiki/How-to-perfectly-clone-a-slice%3F
		nextIndex := append(rf.nextIndex[:0:0], rf.nextIndex...)
		// matchIndex := append(rf.matchIndex[:0:0], rf.matchIndex...)
		log := append(rf.log[:0:0], rf.log...)
		rf.mu.Unlock()

		if role != RoleLeader {
			DPrintf("replicate %d not leader any more", self)
			return
		}

		if currentTerm != startTerm {
			DPrintf("replicate term not match %d vs %d", currentTerm, startTerm)
			return
		}

		idleCount := 0
		for idx := range rf.peers {
			if idx == self {
				continue
			}
			if len(log)-1 < nextIndex[idx] {
				continue
			}
			preLogIndex := nextIndex[idx] - 1
			preLogTerm := log[preLogIndex].Term
			entries := log[nextIndex[idx]:]
			if len(entries) == 0 {
				idleCount += 1
				continue
			}
			go rf.syncLog(idx, self, currentTerm, leaderCommit, &entries, preLogIndex, preLogTerm)
		}
		if idleCount+1 == len(rf.peers) {
			time.Sleep(10 * time.Millisecond)
		}
		rf.tryUpdateCommitIndex()
	}
}

func (rf *Raft) tryUpdateCommitIndex() {

}
