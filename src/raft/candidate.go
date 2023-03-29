package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) newSession(round int, args *RequestVoteArgs) int {
	currentTerm := args.Term
	result := make(chan int)
	for ix := range rf.peers {
		if ix == rf.me {
			continue
		}
		go func(server int, ch chan<- int) {
			rf.mu.Lock()
			prefix := fmt.Sprintf("ELECT%016d %d of [%d,%d]", round, rf.me, rf.currentTerm, rf.role)
			if rf.role != RoleCandidate {
				DPrintf("%s call vote %d return, not candidate", prefix, server)
				rf.mu.Unlock()
				ch <- -1
				return
			}
			if rf.currentTerm != currentTerm {
				DPrintf("%s call vote %d return, term changed [%d vs %d]", prefix, server, rf.currentTerm, currentTerm)
				rf.mu.Unlock()
				ch <- -1
				return
			}
			DPrintf("%s call vote %d start", prefix, server)
			rf.mu.Unlock()

			reply := RequestVoteReply{}
			rc := rf.sendRequestVote(server, args, &reply)
			if !rc {
				DPrintf("%s call vote %d rpc fail", prefix, server)
				ch <- 2
				return
			}
			if reply.Term > currentTerm {
				DPrintf("%s call vote %d becomeFollower [%d vs %d]", prefix, server, reply.Term, currentTerm)
				rf.mu.Lock()
				rf.becomeFollower(reply.Term)
				rf.leaderId = -1
				rf.mu.Unlock()
				ch <- -1
				return
			}
			if reply.VoteGranted {
				if reply.Term > currentTerm {
					panic("follower term is bigger than candidate, meanwhile vote granted")
				}
				DPrintf("%s call vote %d granted", prefix, server)
				ch <- 0
				return
			}
			DPrintf("%s call vote %d denied", prefix, server)
			ch <- 1
		}(ix, result)
	}

	rf.mu.Lock()
	prefix := fmt.Sprintf("ELECT%016d %d of [%d,%d]", round, rf.me, rf.currentTerm, rf.role)
	rf.mu.Unlock()

	votes := 1
	count := 1
	for rf.killed() == false {
		pn := len(rf.peers)
		if count >= pn {
			DPrintf("%s election fail", prefix)
			break
		}
		if votes*2 > pn {
			DPrintf("%s election successful", prefix)
			return 0
		}

		select {
		case ret := <-result:
			count += 1
			DPrintf("%s collect vote %t, progress: %d/%d/%d", prefix, ret == 0, votes, count, pn)
			if ret < 0 {
				DPrintf("%s election fail2", prefix)
				return -1
			}
			if ret == 0 {
				votes += 1
			}
		case <-time.After(ElectionTimeout * time.Millisecond):
			DPrintf("%s election timeout", prefix)
			return 1
		}
	}
	return 2
}

func (rf *Raft) becomeCandidate() {
	rf.role = RoleCandidate
	rf.votedFor = -1
	rf.electionStartAt = time.Now().UnixMilli()
	rf.currentTerm += 1
	rf.persist()
}

func (rf *Raft) startElection(round int) {
	rf.mu.Lock()
	role := rf.role
	prefix := fmt.Sprintf("ELECT%016d %d of [%d, %d]", round, rf.me, rf.currentTerm, role)
	if role != RoleFollower {
		// 这个检查可能并不需要，因为没有什么原因让它突然变成非 follower
		DPrintf("%s not follower now", prefix)
		rf.mu.Unlock()
		return
	}
	rf.becomeCandidate()
	rf.leaderId = -1

	currentTerm := rf.currentTerm
	candidateId := rf.me
	lastLogIndex, lastLogTerm := rf.vlog.GetLastIndexTerm()
	args := RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  candidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	startedAt := time.Now().UnixMilli()
	ret := rf.newSession(round, &args)
	prefix = fmt.Sprintf("ELECT%016d %d of [%d,%d]", round, rf.me, rf.currentTerm, role)
	DPrintf("%s return: %d, cost time: %d", prefix, ret, time.Now().UnixMilli()-startedAt)
	rf.mu.Lock()
	if ret == 0 && rf.role == RoleCandidate {
		rf.becomeLeader()
		rf.mu.Unlock()
		DPrintf("%s becomeLeader of term %d", prefix, rf.currentTerm)
	}
	rf.mu.Unlock()
}

func (rf *Raft) becomeLeader() {
	rf.votedFor = -1
	rf.role = RoleLeader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.vlog.NextIndex()
		rf.matchIndex[i] = 0
	}

	// insert a noop
	// tmp := LogEntry{Term: rf.currentTerm, Command: nil}
	// rf.vlog.AddItem(&tmp)
	rf.persist()
}
