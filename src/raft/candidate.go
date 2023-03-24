package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) newSession() bool {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	rf.leaderId = -1

	term := rf.currentTerm
	candidateId := rf.me
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()

	result := make(chan int)
	for ix := range rf.peers {
		if ix == rf.me {
			continue
		}
		go func(server int, ch chan<- int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			rc := rf.sendRequestVote(server, &args, &reply)
			if !rc {
				DPrintf("sendRequestVote fail %d->%d", server, rf.me)
				ch <- 2
				return
			}
			if reply.Term > term {
				DPrintf("sendRequestVote degraded %d->%d, %d vs %d", server, rf.me, reply.Term, term)
				rf.mu.Lock()
				rf.becomeFollower(reply.Term)
				rf.leaderId = -1
				rf.mu.Unlock()
				ch <- -1
				return
			}
			if reply.VoteGranted {
				if reply.Term > term {
					panic("follower term is bigger than candidate, meanwhile vote granted")
				}
				DPrintf("sendRequestVote granted %d->%d", server, rf.me)
				ch <- 0
				return
			}
			DPrintf("sendRequestVote not granted %d->%d", server, rf.me)
			ch <- 1
		}(ix, result)
	}

	votes := 1
	count := 1
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role != RoleCandidate {
			rf.mu.Unlock()
			break
		}
		pn := len(rf.peers)
		if count >= pn {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		select {
		case ret := <-result:
			count += 1
			DPrintf("collect %d, %d/%d vote result", ret, votes, count)
			if ret < 0 {
				return false
			}
			if ret == 0 {
				votes += 1
				if votes*2 > pn {
					return true
				}
			}
		}
	}
	return false
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
		DPrintf("election start at: %d, end at: %d, candidate: %d, ret: %t",
			rf.electionStartAt, time.Now().UnixMilli(), rf.me, ret)
		if ret {
			rf.becomeLeader()
			break
		}
		time.Sleep(time.Duration(span) * time.Millisecond)
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = RoleLeader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	DPrintf("%d become leader of term %d", rf.me, rf.currentTerm)
}
