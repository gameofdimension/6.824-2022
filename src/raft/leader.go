package raft

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
