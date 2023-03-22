package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) startElection() {
	// if term >= rf.currentTerm {
	// 	rf.currentTerm = term
	// 	rf.votedFor = -1
	// 	rf.leaderId = args.LeaderId
	// 	rf.role = RoleFollower
	// }
	rf.mu.Lock()
	rf.role = RoleCandidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.leaderId = -1

	rf.electionStartAt = time.Now().UnixMilli()
	var votes int32 = 1
	term := rf.currentTerm
	candidateId := rf.me

	// var lastLogIndex int
	// var lastLogTerm int

	// // first boot, index start from 1
	// if len(rf.log) == 0 {
	// 	panic("log size is zero")
	// }
	// if len(rf.log) == 1 {
	// 	if term != 1 {
	// 		panic("term not 1 when boot")
	// 	}
	// 	lastLogIndex = 0
	// 	lastLogTerm = term - 1
	// } else {
	// 	lastLogIndex = len(rf.log) - 1
	// 	lastLogTerm = rf.log[lastLogIndex].Term
	// }
	rf.mu.Unlock()

	for {
		for ix := range rf.peers {
			if ix == rf.me {
				continue
			}
			go func() {
				args := RequestVoteArgs{
					Term: term, CandidateId: candidateId,
					LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
				reply := RequestVoteReply{}
				rc := rf.sendRequestVote(ix, &args, &reply)
				if rc {
					if reply.VoteGranted {
						if reply.Term > term {
							panic("follower term is bigger than candidate, meanwhile vote granted")
						}
						atomic.AddInt32(&votes, 1)
					}
				} else {
					DPrintf("sendRequestVote %d->%d", rf.me, ix)
				}
			}()
		}
	}
}
