package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) makeHeartBeatArgs() (RoleType, AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	entries := make([]LogEntry, 0)
	logIdx, logTerm := rf.vlog.GetLastIndexTerm()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: logIdx,
		PrevLogTerm:  logTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return rf.role, args
}

func (rf *Raft) sendHeartBeat() bool {
	role, req := rf.makeHeartBeatArgs()
	if role != RoleLeader {
		return false
	}

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int, args *AppendEntriesArgs) {
			rf.mu.Lock()
			self := rf.me
			if rf.role != RoleLeader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			DPrintf("%d send heartbeat to %d begin", self, server)
			rc := rf.sendAppendEntries(server, args, &reply)
			DPrintf("%d send heartbeat to %d return: [rpc:%t,success:%t], %v, %v", self, server, rc, reply.Success, args, reply)
			rf.mu.Lock()
			if rc && reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
			}
			rf.mu.Unlock()
		}(idx, &req)
	}
	return true
}

func (rf *Raft) prepareArgs(server int) (bool, *AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	role := rf.role
	if role != RoleLeader {
		return false, nil
	}
	self := rf.me
	currentTerm := rf.currentTerm
	leaderCommit := rf.commitIndex
	DPrintf("sync worker %d of leader %d, term:%d, progress: %d vs %d",
		server, self, currentTerm, rf.matchIndex[server], rf.vlog.NextIndex()-1)
	if rf.matchIndex[server] > rf.vlog.NextIndex()-1 {
		panic(fmt.Sprintf("match index exceed last index %d vs %d", rf.matchIndex[server], rf.vlog.NextIndex()-1))
	}
	if rf.matchIndex[server] == rf.vlog.NextIndex()-1 {
		DPrintf("sync worker %d of leader %d, term:%d, no log to sync", server, self, currentTerm)
		return false, nil
	}

	preLogIndex := rf.nextIndex[server] - 1
	leaderNext := rf.vlog.NextIndex()
	if rf.nextIndex[server] > leaderNext {
		panic(fmt.Sprintf("inspect next index %d ,%d vs %d, %t, %d\n",
			leaderNext, rf.me, server, role == RoleLeader, rf.nextIndex[server]))
	}
	preLogTerm := rf.vlog.GetTermAtIndex(preLogIndex)
	DPrintf("sync worker %d of leader %d, term:%d, nextIndex: %d",
		server, self, currentTerm, rf.nextIndex[server])
	entries := rf.vlog.Slice(rf.nextIndex[server])

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
	return true, &args
}

func (rf *Raft) sendSnapshot(server int) int {
	rf.mu.Lock()
	role := rf.role
	if role != RoleLeader {
		rf.mu.Unlock()
		return -1
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.vlog.LastIncludedIndex,
		LastIncludedTerm:  rf.vlog.LastIncludedTerm,
		Data:              rf.snapshot,
	}
	prefix := fmt.Sprintf("%d sendInstallSnapshot %d, term:%d, LastIncludedIndex:%d, LastIncludedTerm:%d",
		rf.me, server, rf.currentTerm, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	rc := rf.sendInstallSnapshot(server, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rc {
		DPrintf("%s rpc fail", prefix)
		return 1
	}
	if reply.Term > rf.currentTerm {
		DPrintf("%s becomeFollower, reply term:%d", prefix, reply.Term)
		rf.becomeFollower(reply.Term)
		return 2
	}
	DPrintf("%s, assume successful", prefix)
	if args.LastIncludedIndex > rf.matchIndex[server] {
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}
	return 0
}

func (rf *Raft) syncLog(server int) int {
	ret, args := rf.prepareArgs(server)
	if !ret {
		return -1
	}

	reply := AppendEntriesReply{}
	DPrintf("sync worker %d of leader %d, term:%d, sendAppendEntries begin", server, args.LeaderId, args.Term)
	rc := rf.sendAppendEntries(server, args, &reply)
	DPrintf("sync worker %d of leader %d, term:%d, sendAppendEntries return %t", server, args.LeaderId, args.Term, rc)
	if !rc {
		DPrintf("sync worker %d of leader %d, term:%d, sendAppendEntries rpc fail %t",
			server, args.LeaderId, args.Term, rc)
		return 1
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		DPrintf("sync worker %d of leader %d, term:%d, sendAppendEntries becomeFollower, reply term:%d",
			server, args.LeaderId, rf.currentTerm, reply.Term)
		rf.becomeFollower(reply.Term)
		return 2
	}
	if !reply.Success {
		DPrintf("sync worker %d of leader %d, term:%d, sendAppendEntries not success %v",
			server, args.LeaderId, rf.currentTerm, reply)
		if reply.XTerm < 0 {
			return -3
		}
		next := rf.computeNextIndex(reply.XTerm, reply.XIndex, reply.XLen, args.PrevLogIndex)
		rf.nextIndex[server] = next
		if rf.nextIndex[server] > rf.vlog.NextIndex() {
			panic(fmt.Sprintf("nextIndex error %d, %d vs %d, %d\n", server, next, rf.nextIndex[server], rf.vlog.NextIndex()))
		}
		if rf.nextIndex[server] < rf.vlog.GetLastIncludedIndex()+1 {
			DPrintf("sync worker %d of leader %d, term:%d, sendAppendEntries will send snapshot %d vs %d",
				server, args.LeaderId, rf.currentTerm, rf.nextIndex[server], rf.vlog.GetLastIncludedIndex())
		}
		return 3
	}
	rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	if rf.nextIndex[server] > rf.vlog.NextIndex() {
		panic(fmt.Sprintf("nextIndex fail %d, %d, %d\n", server, rf.nextIndex[server], rf.vlog.NextIndex()))
	}
	if len(args.Entries) <= 0 {
		DPrintf("sync worker %d of leader %d, term:%d, sendAppendEntries empty entry", server, args.LeaderId, rf.currentTerm)
		return -2
	}
	DPrintf("sync worker %d of leader %d, term:%d, sendAppendEntries success", server, args.LeaderId, rf.currentTerm)
	return 0
}

func (rf *Raft) lastIndexOfTerm(term int, preLogIndex int) int {
	res := -1
	bottom := rf.vlog.GetLastIncludedIndex() + 1
	for i := preLogIndex; i >= bottom; i -= 1 {
		if rf.vlog.GetItem(i).Term == term {
			return i
		}
		if rf.vlog.GetItem(i).Term < term {
			break
		}
	}
	return res
}

func (rf *Raft) computeNextIndex(xTerm int, xIndex int, Xlen int, preLogIndex int) int {
	if xTerm <= 0 {
		return Xlen
	}
	index := rf.lastIndexOfTerm(xTerm, preLogIndex)
	if index < 0 {
		return xIndex
	}
	return index + 1
}

func (rf *Raft) tryUpdateCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != RoleLeader {
		return -1
	}
	DPrintf("tryUpdateCommitIndex %d, %d, %d, %v", rf.me, rf.commitIndex, rf.vlog.NextIndex()-1, rf.matchIndex)

	for idx := rf.vlog.NextIndex() - 1; idx > rf.commitIndex; idx -= 1 {
		term := rf.vlog.GetItem(idx).Term
		if term < rf.currentTerm {
			break
		}
		if term > rf.currentTerm {
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
			DPrintf("leader %d update commit index %d->%d", rf.me, rf.commitIndex, idx)
			rf.commitIndex = idx
			return 0
		}
	}
	return 1
}

func (rf *Raft) replicateWorker(server int) {
	for rf.killed() == false {
		rf.mu.Lock()
		sendSnapshot := false
		if rf.nextIndex[server] <= rf.vlog.GetLastIncludedIndex() {
			sendSnapshot = true
		}
		if rf.role == RoleLeader {
			DPrintf("%d replicateWorker %d, %d vs %d, send snapshot:%t",
				rf.me, server, rf.nextIndex[server], rf.vlog.GetLastIncludedIndex(), sendSnapshot)
		}
		rf.mu.Unlock()

		var rc int
		if sendSnapshot {
			rc = rf.sendSnapshot(server)
		} else {
			rc = rf.syncLog(server)
		}
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
