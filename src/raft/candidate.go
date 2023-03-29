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
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				DPrintf("%s call vote %d becomeFollower by higher term:[%d vs %d]", prefix, server, reply.Term, rf.currentTerm)
				rf.becomeFollower(reply.Term)
				rf.leaderId = -1
				rf.mu.Unlock()
				ch <- -1
				return
			}
			if reply.Term < rf.currentTerm {
				DPrintf("%s call vote %d ignore outdated reply of term:[%d vs %d]", prefix, server, reply.Term, rf.currentTerm)
				rf.mu.Unlock()
				ch <- 3
				return
			}
			rf.mu.Unlock()
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
	rf.mu.Lock()
	prefix = fmt.Sprintf("ELECT%016d %d of [%d,%d]", round, rf.me, rf.currentTerm, role)
	DPrintf("%s return: %d, cost time: %d", prefix, ret, time.Now().UnixMilli()-startedAt)
	if ret == 0 && rf.role == RoleCandidate {
		rf.becomeLeader()
		DPrintf("%s becomeLeader of term %d", prefix, rf.currentTerm)
	}
	rf.mu.Unlock()
}

func (rf *Raft) becomeLeader() {
	rf.votedFor = -1
	rf.role = RoleLeader
	rf.leaderId = rf.me
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.vlog.NextIndex()
		rf.matchIndex[i] = 0
	}

	// insert a noop
	// tmp := LogEntry{Term: rf.currentTerm, Command: nil}
	// rf.vlog.AddItem(&tmp)
	// 上面的代码是为了尝试解决有些 test case 跑得很慢的问题。观察一下，发现原因在于一个 leader 在 term1 接受了
	// command1，但是奔溃了，然后再 term2 重新选为 leader，如果这时调用方没有发送新的 command 过来， 原来老的
	// 这个 command1 用于无法被认为 commit，从而也就不会 apply，从而就需要一直等直到当前循环超时。
	// raft 只能 commit 当前 term 的 command 的要求是其正确性的一个前提条件（参考 figure 8）因而是不能改的。
	// 这里的改动是参考第 8 节中 no-op 的做法，这样可以加速 commit ，但是会破坏另一些 case ，因此这里应该是预期
	// 不使用这个方法的
	rf.persist()
}
