package raft

import "time"

func (rf *Raft) sendHeartBeat() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != RoleLeader {
		return false
	}

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			entries := make([]LogEntry, 0)
			logIdx := len(rf.log) - 1
			logTerm := rf.log[logIdx].Term
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: logIdx,
				PrevLogTerm:  logTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, &args, &reply)
		}(idx)
	}
	return true
}

func (rf *Raft) syncLog(server int) int {
	rf.mu.Lock()
	role := rf.role
	self := rf.me
	currentTerm := rf.currentTerm
	leaderCommit := rf.commitIndex
	// https://github.com/go101/go101/wiki/How-to-perfectly-clone-a-slice%3F
	nextIndex := append(rf.nextIndex[:0:0], rf.nextIndex...)
	// matchIndex := append(rf.matchIndex[:0:0], rf.matchIndex...)

	preLogIndex := nextIndex[server] - 1
	preLogTerm := rf.log[preLogIndex].Term
	entries := rf.log[nextIndex[server]:]
	rf.mu.Unlock()

	if role != RoleLeader {
		return -1
	}

	if len(entries) <= 0 {
		return -2
	}

	args := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     self,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  preLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	reply := AppendEntriesReply{}
	rc := rf.sendAppendEntries(server, &args, &reply)
	if !rc {
		DPrintf("sendAppendEntries rpc fail %t", rc)
		return 1
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		DPrintf("sendAppendEntries becomeFollower, %d vs %d", reply.Term, rf.currentTerm)
		rf.becomeFollower(reply.Term)
		return 2
	}
	if !reply.Success {
		DPrintf("sendAppendEntries process fail %t", reply.Success)
		rf.nextIndex[server] = rf.computeNextIndex(reply.XTerm, reply.XIndex, reply.XLen)
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
		return 3
	}
	rf.matchIndex[server] = preLogIndex + len(entries)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	return 0
}

func lastIndexOfTerm(log []LogEntry, term int) int {
	res := -1
	for i := len(log) - 1; i >= 0; i -= 1 {
		if log[i].Term == term {
			return i
		}
		if log[i].Term < term {
			break
		}
	}
	return res
}

func (rf *Raft) computeNextIndex(xTerm int, xIndex int, Xlen int) int {
	if xTerm <= 0 {
		return Xlen
	}
	index := lastIndexOfTerm(rf.log, xTerm)
	if index < 0 {
		return xIndex
	}
	return index
}

func (rf *Raft) tryUpdateCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != RoleLeader {
		return -1
	}
	for idx := len(rf.log) - 1; idx > rf.commitIndex; idx -= 1 {
		if rf.log[idx].Term < rf.currentTerm {
			break
		}
		if rf.log[idx].Term > rf.currentTerm {
			continue
		}
		matchCount := 1
		for i, m := range rf.matchIndex {
			if i == rf.me {
				continue
			}
			if m >= idx {
				matchCount += 1
			}
		}
		if 2*matchCount > len(rf.peers) {
			DPrintf("leader %d update commit index %d", rf.me, idx)
			rf.commitIndex = idx
			return 0
		}
	}
	return 1
}

func (rf *Raft) replicateWorker(server int) {
	for rf.killed() == false {
		rc := rf.syncLog(server)
		if rc < 0 {
			time.Sleep(53 * time.Millisecond)
		} else {
			time.Sleep(11 * time.Millisecond)
		}
	}
}

func (rf *Raft) commitIndexWorker() {
	for rf.killed() == false {
		rf.tryUpdateCommitIndex()
		time.Sleep(47 * time.Millisecond)
	}
}

func (rf *Raft) heartBeatWorker() {
	for rf.killed() == false {
		rc := rf.sendHeartBeat()
		if !rc {
			time.Sleep(7 * time.Millisecond)
		} else {
			time.Sleep(time.Duration(SendHeartBeatInterval) * time.Millisecond)
		}
	}
}

func (rf *Raft) spawnLeaderWorker() {
	go rf.heartBeatWorker()
	go rf.commitIndexWorker()

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.replicateWorker(idx)
	}
}
