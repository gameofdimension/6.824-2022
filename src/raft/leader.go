package raft

import (
	"fmt"
	"time"
)

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
	if role != RoleLeader {
		rf.mu.Unlock()
		return -1
	}
	self := rf.me
	currentTerm := rf.currentTerm
	leaderCommit := rf.commitIndex

	preLogIndex := rf.nextIndex[server] - 1
	if rf.nextIndex[server] > len(rf.log) {
		panic(fmt.Sprintf("inspect next index %d ,%d vs %d, %t, %d\n",
			len(rf.log), rf.me, server, role == RoleLeader, rf.nextIndex[server]))
	}
	preLogTerm := rf.log[preLogIndex].Term
	DPrintf("sync worker %d of leader %d, term %d, nextIndex: %d, log: %v",
		server, self, currentTerm, rf.nextIndex[server], rf.log)
	entries := rf.log[rf.nextIndex[server]:]
	rf.mu.Unlock()

	// 下面这个优化看起来可以节省不必要的 rpc 通信，事实上是有害的
	// 因为新 leader 上任之后，会将其的 nextIndex 初始化为 log
	// 尾巴的下一个位置，这就意味着这种情况下 entries 必然为空。
	// 如果这时 leader 的日志本身不增长（不那么繁忙），同时有一些
	// follower 日志落后 leader 。如果采用这个优化，这些 follower
	// 的日志将长时间不能跟 leader 同步
	// 因此为空的 entries 虽然没有传输数据，但是这个 rpc 调用本身
	// 还是起到了与 follower 同步进度的作用，因而是必需的
	//
	/*
		if len(entries) <= 0 {
			return -2
		}
	*/

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
		DPrintf("sync worker %d of leader %d, term:%d, sendAppendEntries rpc fail %t", server, self, currentTerm, rc)
		return 1
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		DPrintf("sync worker %d of leader %d, term:%d, sendAppendEntries becomeFollower, reply term:%d",
			server, self, rf.currentTerm, reply.Term)
		rf.becomeFollower(reply.Term)
		return 2
	}
	if !reply.Success {
		DPrintf("sync worker %d of leader %d, term:%d, sendAppendEntries not success %v", server, self, rf.currentTerm, reply)
		if reply.XTerm < 0 {
			return -3
		}
		next := rf.computeNextIndex(reply.XTerm, reply.XIndex, reply.XLen, preLogIndex)
		rf.nextIndex[server] = next
		if rf.nextIndex[server] > len(rf.log) {
			panic(fmt.Sprintf("nextIndex error %d, %d vs %d, %d\n", server, next, rf.nextIndex[server], len(rf.log)))
		}
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
		return 3
	}
	DPrintf("sync worker %d of leader %d, term:%d, sendAppendEntries success", server, self, rf.currentTerm)
	rf.matchIndex[server] = preLogIndex + len(entries)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	if rf.nextIndex[server] > len(rf.log) {
		panic(fmt.Sprintf("nextIndex fail %d, %d, %d\n", server, rf.nextIndex[server], len(rf.log)))
	}
	if len(entries) <= 0 {
		DPrintf("sync worker %d of leader %d, term:%d, sendAppendEntries empty entry", server, self, rf.currentTerm)
		return -2
	}
	return 0
}

func lastIndexOfTerm(log []LogEntry, term int, preLogIndex int) int {
	res := -1
	for i := preLogIndex; i >= 0; i -= 1 {
		if log[i].Term == term {
			return i
		}
		if log[i].Term < term {
			break
		}
	}
	return res
}

func (rf *Raft) computeNextIndex(xTerm int, xIndex int, Xlen int, preLogIndex int) int {
	if xTerm <= 0 {
		return Xlen
	}
	index := lastIndexOfTerm(rf.log, xTerm, preLogIndex)
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
			DPrintf("leader %d update commit index %d->%d, %v", rf.me, rf.commitIndex, idx, rf.log)
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
			time.Sleep(83 * time.Millisecond)
		} else {
			time.Sleep(17 * time.Millisecond)
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
