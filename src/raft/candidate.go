package raft

import (
	"fmt"
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

func (rf *Raft) newSession(round int, session int) int {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.persist()
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
	prefix := fmt.Sprintf("SESSN%08d:%07d %d of [%d,%d], args: [%d,%d]",
		round, session, rf.me, rf.currentTerm, rf.role, lastLogTerm, lastLogIndex)
	rf.mu.Unlock()

	result := make(chan int)
	for ix := range rf.peers {
		if ix == rf.me {
			continue
		}
		go func(server int, ch chan<- int) {
			rf.mu.Lock()
			DPrintf("%s call vote %d start", prefix, server)
			if rf.role != RoleCandidate {
				DPrintf("%s call vote %d return, not candidate", prefix, server)
				rf.mu.Unlock()
				ch <- -1
				return
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			rc := rf.sendRequestVote(server, &args, &reply)
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

	votes := 1
	count := 1
	for rf.killed() == false {
		ret, pn := rf.checkProgress(count)
		if !ret {
			DPrintf("%s election fail", prefix)
			break
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
				if votes*2 > pn {
					DPrintf("%s election successful", prefix)
					return 0
				}
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
	rf.persist()
}

func (rf *Raft) startElection(round int) {
	rf.mu.Lock()
	rf.becomeCandidate()
	rf.mu.Unlock()

	session := 0
	for rf.killed() == false {
		session += 1
		rf.mu.Lock()
		role := rf.role
		prefix := fmt.Sprintf("ELECT%016d %d of [%d, %d]", round, rf.me, rf.currentTerm, role)
		rf.mu.Unlock()

		if role != RoleCandidate {
			DPrintf("%s not candidate", prefix)
			break
		}

		rf.electionStartAt = time.Now().UnixMilli()
		DPrintf("%s session %d start", prefix, session)
		ret := rf.newSession(round, session)

		// term updated
		prefix = fmt.Sprintf("ELECT%016d %d of [%d,%d]", round, rf.me, rf.currentTerm, role)
		DPrintf("%s session %d return: %d, cost time: %d", prefix, session, ret, time.Now().UnixMilli()-rf.electionStartAt)
		rf.mu.Lock()
		if ret == 0 && rf.role == RoleCandidate {
			rf.becomeLeader()
			rf.mu.Unlock()
			DPrintf("%s session %d becomeLeader of term %d", prefix, session, rf.currentTerm)
			break
		}
		rf.mu.Unlock()
		span := rand.Intn(Delta) + ElectionTimeout
		time.Sleep(time.Duration(span) * time.Millisecond)
	}
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
