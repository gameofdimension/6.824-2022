package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) newSession(elected chan<- bool) {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.leaderId = -1

	term := rf.currentTerm
	candidateId := rf.me
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.persist()
	rf.mu.Unlock()

	votes := 1

	result := make(chan int)
	for ix := range rf.peers {
		if ix == rf.me {
			continue
		}
		go func(server int, ch chan<- int) {
			args := RequestVoteArgs{
				Term: term, CandidateId: candidateId,
				LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
			reply := RequestVoteReply{}
			rc := rf.sendRequestVote(server, &args, &reply)
			if rc {
				if reply.VoteGranted {
					if reply.Term > term {
						panic("follower term is bigger than candidate, meanwhile vote granted")
					}
					DPrintf("sendRequestVote granted %d->%d", rf.me, server)
					ch <- 0
					return
				} else {
					if reply.Term > term {
						DPrintf("sendRequestVote degraded %d->%d", rf.me, server)
						rf.mu.Lock()
						rf.becomeFollower(term)
						rf.leaderId = -1
						rf.mu.Unlock()

						ch <- -1
						return
					} else {
						DPrintf("sendRequestVote not granted %d->%d", rf.me, server)
						ch <- 1
						return
					}
				}
			} else {
				DPrintf("sendRequestVote fail %d->%d", rf.me, server)
				ch <- 2
				return
			}
		}(ix, result)
	}

	count := 0
	for {
		if count >= len(rf.peers)-1 {
			elected <- false
			return
		}
		select {
		case ret := <-result:
			count += 1
			DPrintf("collect %d, %d vote result", count, ret)
			if ret < 0 {
				elected <- false
				return
			}
			if ret == 0 {
				votes += 1
				if votes*2 > len(rf.peers) {
					elected <- true
					return
				}
			}
		}
	}
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = RoleCandidate
}

func (rf *Raft) startElection() {
	rf.becomeCandidate()
	for {
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()

		if role != RoleCandidate {
			break
		}
		elected := make(chan bool)
		rf.electionStartAt = time.Now().UnixMilli()
		go rf.newSession(elected)

		span := rand.Intn(Delta) + ElectionTimeout
		select {
		case ret := <-elected:
			DPrintf("election start at: %d, end at: %d, ret: %t", rf.electionStartAt, time.Now().UnixMilli(), ret)
			if ret {
				rf.becomeLeader()
			} else {
				time.Sleep(time.Duration(span) * time.Millisecond)
			}
		case <-time.After(time.Duration(span) * time.Millisecond):
		}
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
	DPrintf("%d become leader", rf.me)
}
