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

func (rf *Raft) sendHeartBeat(roundId string) bool {
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
			prefix := fmt.Sprintf("HBEAT%s from %d of [%d,%d] to", roundId, self, rf.currentTerm, rf.role)
			if rf.role != RoleLeader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			rf.sendEntries(args, prefix, server)
		}(idx, &req)
	}
	return true
}

func (rf *Raft) prepareArgs(server int, roundId string) (bool, *AppendEntriesArgs, string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	role := rf.role
	if role != RoleLeader {
		return false, nil, ""
	}
	self := rf.me
	currentTerm := rf.currentTerm
	leaderCommit := rf.commitIndex
	prefix := fmt.Sprintf("APPND%s %d of [%d,%d] to %d",
		roundId, self, currentTerm, role, server)
	DPrintf("%s prepareArgs progress now [%d vs %d]", prefix, rf.matchIndex[server], rf.vlog.NextIndex()-1)
	if rf.matchIndex[server] > rf.vlog.NextIndex()-1 {
		panic(fmt.Sprintf("match index exceed last index %d vs %d", rf.matchIndex[server], rf.vlog.NextIndex()-1))
	}
	if rf.matchIndex[server] == rf.vlog.NextIndex()-1 {
		DPrintf("%s prepareArgs no log to sync [%d vs %d]", prefix, rf.matchIndex[server], rf.vlog.NextIndex()-1)
		return false, nil, ""
	}

	preLogIndex := rf.nextIndex[server] - 1
	leaderNext := rf.vlog.NextIndex()
	if rf.nextIndex[server] > leaderNext {
		panic(fmt.Sprintf("inspect next index %d ,%d vs %d, %t, %d\n",
			leaderNext, rf.me, server, role == RoleLeader, rf.nextIndex[server]))
	}
	preLogTerm := rf.vlog.GetTermAtIndex(preLogIndex)
	entries := rf.vlog.Slice(rf.nextIndex[server])
	DPrintf("%s prepareArgs will send log from %d of len %d", prefix, rf.nextIndex[server], len(entries))

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
	return true, &args, prefix
}

func (rf *Raft) sendSnapshot(server int, roundId string) int {
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
	prefix := fmt.Sprintf("SSNAP%s %d of [%d,%d] to %d, args [%d,%d]",
		roundId, rf.me, rf.currentTerm, rf.role, server, args.LastIncludedTerm, args.LastIncludedIndex)
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
		DPrintf("%s becomeFollower by reply term:[%d vs %d]", prefix, reply.Term, rf.currentTerm)
		rf.becomeFollower(reply.Term)
		return 2
	}
	if reply.Term < rf.currentTerm {
		DPrintf("%s outdated reply of term:[%d vs %d]", prefix, reply.Term, rf.currentTerm)
		return 4
	}
	DPrintf("%s, assume successful", prefix)
	if args.LastIncludedIndex > rf.matchIndex[server] {
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}
	return 0
}

func (rf *Raft) sendEntries(args *AppendEntriesArgs, prefix string, server int) int {
	prefix = fmt.Sprintf("%s args [%d,%d]", prefix, args.PrevLogTerm, args.PrevLogIndex)
	reply := AppendEntriesReply{}
	DPrintf("%s begin", prefix)
	rc := rf.sendAppendEntries(server, args, &reply)
	DPrintf("%s rpc return %t", prefix, rc)
	if !rc {
		DPrintf("%s rpc fail", prefix)
		return 1
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		DPrintf("%s becomeFollower by reply term:[%d vs %d]", prefix, reply.Term, rf.currentTerm)
		rf.becomeFollower(reply.Term)
		return 2
	}
	if reply.Term < rf.currentTerm {
		DPrintf("%s ignore outdated reply of term:[%d vs %d]", prefix, reply.Term, rf.currentTerm)
		return 4
	}
	if !reply.Success {
		DPrintf("%s not success %v", prefix, reply)
		if reply.XTerm < 0 {
			return -3
		}
		next := rf.computeNextIndex(reply.XTerm, reply.XIndex, reply.XLen, args.PrevLogIndex)
		rf.nextIndex[server] = next
		if rf.nextIndex[server] > rf.vlog.NextIndex() {
			panic(fmt.Sprintf("nextIndex error %d, %d vs %d, %d\n", server, next, rf.nextIndex[server], rf.vlog.NextIndex()))
		}
		if rf.nextIndex[server] < rf.vlog.GetLastIncludedIndex()+1 {
			DPrintf("%s will send snapshot %d vs %d", prefix, rf.nextIndex[server], rf.vlog.GetLastIncludedIndex())
		}
		return 3
	}
	rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	if rf.nextIndex[server] > rf.vlog.NextIndex() {
		panic(fmt.Sprintf("nextIndex fail %d, %d, %d\n", server, rf.nextIndex[server], rf.vlog.NextIndex()))
	}
	if len(args.Entries) <= 0 {
		DPrintf("%s empty entry", prefix)
		return -2
	}
	DPrintf("%s success", prefix)
	return 0
}

func (rf *Raft) syncLog(server int, roundId string) int {
	ret, args, prefix := rf.prepareArgs(server, roundId)
	if !ret {
		return -1
	}
	return rf.sendEntries(args, prefix, server)
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

func (rf *Raft) tryUpdateCommitIndex(round int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != RoleLeader {
		return -1
	}
	prefix := fmt.Sprintf("UPCOM%016d %d of [%d,%d] diff: [%d vs %d], match index: %v",
		round, rf.me, rf.currentTerm, rf.role, rf.commitIndex, rf.vlog.NextIndex()-1, rf.matchIndex)
	DPrintf("%s start", prefix)

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
			DPrintf("%s update %d->%d", prefix, rf.commitIndex, idx)
			rf.commitIndex = idx
			return 0
		}
	}
	DPrintf("%s no action", prefix)
	return 1
}

func (rf *Raft) replicateWorker(server int) {
	round := 0
	for rf.killed() == false {
		round += 1
		roundId := fmt.Sprintf("%016d", round)
		rf.mu.Lock()
		sendSnapshot := false
		if rf.nextIndex[server] <= rf.vlog.GetLastIncludedIndex() {
			sendSnapshot = true
		}
		if rf.role == RoleLeader {
			DPrintf("REPLI%s %d of [%d,%d] to %d, [%d vs %d], will send snapshot:%t",
				roundId, rf.me, rf.currentTerm, rf.role, server, rf.nextIndex[server],
				rf.vlog.GetLastIncludedIndex(), sendSnapshot)
		}
		rf.mu.Unlock()

		var rc int
		if sendSnapshot {
			rc = rf.sendSnapshot(server, roundId)
		} else {
			rc = rf.syncLog(server, roundId)
		}
		if rc < 0 {
			time.Sleep(83 * time.Millisecond)
		} else {
			time.Sleep(17 * time.Millisecond)
		}
	}
}

func (rf *Raft) commitIndexWorker() {
	round := 0
	for rf.killed() == false {
		round += 1
		rf.tryUpdateCommitIndex(round)
		time.Sleep(47 * time.Millisecond)
	}
}

func (rf *Raft) heartBeatWorker() {
	round := 0
	for rf.killed() == false {
		round += 1
		roundId := fmt.Sprintf("%016d", round)
		rc := rf.sendHeartBeat(roundId)
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
