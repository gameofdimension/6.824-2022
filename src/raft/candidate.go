package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) checkProgress(count int) (bool, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != RoleCandidate {
		return false, -1
	}
	pn := len(rf.peers)
	if count >= pn {
		return false, -2
	}
	return true, pn
}

func (rf *Raft) newSession() int {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	rf.leaderId = -1

	currentTerm := rf.currentTerm
	candidateId := rf.me
	lastLogIndex, lastLogTerm := rf.vlog.GetLastIndexTerm()
	rf.mu.Unlock()

	result := make(chan int)
	for ix := range rf.peers {
		if ix == rf.me {
			continue
		}
		go func(server int, ch chan<- int) {
			rf.mu.Lock()
			DPrintf("%d call sendRequestVote %d start", rf.me, server)
			if rf.role != RoleCandidate {
				DPrintf("%d call sendRequestVote %d not candidate", candidateId, server)
				ch <- -1
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			rc := rf.sendRequestVote(server, &args, &reply)
			if !rc {
				DPrintf("%d call sendRequestVote %d rpc fail", rf.me, server)
				ch <- 2
				return
			}
			if reply.Term > currentTerm {
				DPrintf("%d call sendRequestVote %d degraded, %d vs %d", candidateId, server, reply.Term, currentTerm)
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
				DPrintf("%d call sendRequestVote %d granted", candidateId, server)
				ch <- 0
				return
			}
			DPrintf("%d call sendRequestVote %d denied", candidateId, server)
			ch <- 1
		}(ix, result)
	}

	votes := 1
	count := 1
	for rf.killed() == false {
		ret, pn := rf.checkProgress(count)
		if !ret {
			break
		}

		select {
		case ret := <-result:
			count += 1
			DPrintf("candidate %d election ret [%t,%d], %d/%d/%d vote result", candidateId, ret == 0, ret, votes, count, pn)
			if ret < 0 {
				return -1
			}
			if ret == 0 {
				votes += 1
				if votes*2 > pn {
					return 0
				}
			}
		case <-time.After(ElectionTimeout * time.Millisecond):
			DPrintf("candidate %d election timeout", candidateId)
			return 1
		}
	}
	return 2
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d becomeCandidate at term %d", rf.me, rf.currentTerm)
	rf.role = RoleCandidate
}

func (rf *Raft) startElection() {
	rf.becomeCandidate()
	for rf.killed() == false {
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()

		if role != RoleCandidate {
			break
		}

		rf.electionStartAt = time.Now().UnixMilli()
		span := rand.Intn(Delta) + ElectionTimeout
		ret := rf.newSession()
		DPrintf("candidate %d election cost time: %d, ret: %d",
			rf.me, time.Now().UnixMilli()-rf.electionStartAt, ret)
		if ret == 0 {
			rf.becomeLeader()
			break
		}
		if ret == 1 {
			continue
		}
		time.Sleep(time.Duration(span) * time.Millisecond)
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != RoleCandidate {
		DPrintf("%d fail become leader of term %d", rf.me, rf.currentTerm)
		return
	}
	rf.role = RoleLeader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.vlog.NextIndex()
		rf.matchIndex[i] = 0
	}
	DPrintf("%d become leader of term %d", rf.me, rf.currentTerm)
}
