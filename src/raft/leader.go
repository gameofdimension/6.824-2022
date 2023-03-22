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

func (rf *Raft) syncLog(ch chan bool, server int, leaderId int, currentTerm int,
	leaderCommit int, entries *[]LogEntry, preLogIndex int, preLogTerm int) {
	args := AppendEntriesArgs{Term: currentTerm, LeaderId: leaderId, PrevLogIndex: preLogIndex,
		PrevLogTerm: preLogTerm, Entries: *entries, LeaderCommit: leaderCommit}
	reply := AppendEntriesReply{}
	rc := rf.sendAppendEntries(server, &args, &reply)
	if !rc {
		DPrintf("sendAppendEntries rpc fail %t", rc)
		ch <- false
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		DPrintf("sendAppendEntries becomeFollower, %d vs %d", reply.Term, rf.currentTerm)
		rf.becomeFollower(reply.Term)
		ch <- false
		return
	}
	if !reply.Success {
		DPrintf("sendAppendEntries process fail %t", reply.Success)
		rf.nextIndex[server] -= 1
		ch <- false
		return
	}
	rf.matchIndex[server] = preLogIndex + len(*entries)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	ch <- true
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

		rpcCount := 0
		ch := make(chan bool)
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
				continue
			}
			rpcCount += 1
			go rf.syncLog(ch, idx, self, currentTerm, leaderCommit, &entries, preLogIndex, preLogTerm)
		}
		if rpcCount == 0 {
			time.Sleep(10 * time.Millisecond)
		} else {
			count := 0
			for {
				if count >= rpcCount {
					break
				}
				select {
				case val := <-ch:
					DPrintf("syncLog return %t", val)
					count += 1
				}
			}
		}
		rf.tryUpdateCommitIndex()
	}
}

func (rf *Raft) tryUpdateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
			rf.commitIndex = idx
			break
		}
	}
}
