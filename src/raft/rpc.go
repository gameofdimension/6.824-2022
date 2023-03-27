package raft

import (
	"fmt"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("RequestVote role: %d, term: %d, id: %d, caller term: %d, caller id:%d",
		rf.role, rf.currentTerm, rf.me, args.Term, args.CandidateId)

	term := args.Term
	if term > rf.currentTerm {
		rf.becomeFollower(term)
	}

	if rf.role == RoleFollower {
		if term < rf.currentTerm {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
		candidateId := args.CandidateId
		candidateLogTerm := args.LastLogTerm
		candidateLogIndex := args.LastLogIndex
		selfLogIndex, selfLogTerm := rf.vlog.GetLastIndexTerm()
		if canVote(rf.votedFor, candidateId, candidateLogTerm, candidateLogIndex, selfLogTerm, selfLogIndex) {
			DPrintf("follower %d grant vote to %d, args: %d, %d, %d, %d, %d",
				rf.me, candidateId, rf.votedFor, candidateLogTerm,
				candidateLogIndex, selfLogTerm, selfLogTerm)
			rf.votedFor = candidateId
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			return
		}
		DPrintf("follower %d deny vote to %d, args: %d, %d, %d, %d, %d",
			rf.me, candidateId, rf.votedFor, candidateLogTerm,
			candidateLogIndex, selfLogTerm, selfLogTerm)
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	start := time.Now().UnixMilli()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("Raft.RequestVote, time: %d", time.Now().UnixMilli()-start)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	XTerm   int
	XIndex  int
	XLen    int
	Success bool
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.role = RoleFollower
	rf.persist()
}

func (rf *Raft) firstIndexOfTerm(from int) int {
	term := rf.vlog.GetItem(from).Term
	bottom := rf.vlog.GetLastIncludedIndex() + 1
	for i := from; i >= bottom; i -= 1 {
		if rf.vlog.GetItem(i).Term != term {
			return i + 1
		}
	}
	return bottom
}

func (rf *Raft) firstTangibleIndex() (int, int) {
	lastIncludedIndex := rf.vlog.GetLastIncludedIndex()
	nextIndex := rf.vlog.NextIndex()
	if lastIncludedIndex+1 != nextIndex {
		return lastIncludedIndex + 1, rf.vlog.GetItem(lastIncludedIndex + 1).Term
	}
	return 0, 0
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("AppendEntries role: %d, term: %d, id: %d, caller term: %d, caller id:%d, len: %d",
		rf.role, rf.currentTerm, rf.me, args.Term, args.LeaderId, len(args.Entries))

	term := args.Term
	if term > rf.currentTerm {
		// 上面这个条件判断还是应该忠于论文用 ">"，而不是 ">="，否则会造成 bug。想象以下场景：
		// candidate 1 和 candidate 2 都进入了同一个 term T 进行拉票，1 的动作更快一点成了 leader，
		// 然后给发送心跳，在 ">=" 的逻辑下，其 currentTerm 不变还是 T ，但是 votedFor 会被重置。
		// 这时 2 就又有机会成为 term T 的 leader ，而这是不允许的，相反 ">" 可以防止这样的情况
		rf.becomeFollower(term)
		rf.leaderId = args.LeaderId
	}
	if rf.role != RoleFollower {
		DPrintf("AppendEntries role: %d, term: %d, id: %d, caller term: %d, caller id:%d, len: %d, fail not follower",
			rf.role, rf.currentTerm, rf.me, args.Term, args.LeaderId, len(args.Entries))
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = -1
		return
	}
	if term < rf.currentTerm {
		DPrintf("AppendEntries role: %d, term: %d, id: %d, caller term: %d, caller id:%d, len: %d, fail for term %d vs %d",
			rf.role, rf.currentTerm, rf.me, args.Term, args.LeaderId, len(args.Entries), term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = -1
		return
	}

	rf.lastFromLeaderAt = time.Now().UnixMilli()

	leaderPrevLogIndex := args.PrevLogIndex
	leaderPrevLogTerm := args.PrevLogTerm

	lastIncludedIndex := rf.vlog.GetLastIncludedIndex()
	nextIndex := rf.vlog.NextIndex()
	if leaderPrevLogIndex >= nextIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = 0
		reply.XIndex = 0
		reply.XLen = nextIndex
		return
	}
	if leaderPrevLogIndex < lastIncludedIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XIndex, reply.XTerm = rf.firstTangibleIndex()
		reply.XLen = nextIndex
		return
	}
	myTerm := rf.vlog.GetTermAtIndex(leaderPrevLogIndex)
	if leaderPrevLogTerm != myTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		if leaderPrevLogIndex > lastIncludedIndex {
			reply.XTerm = myTerm
			reply.XIndex = rf.firstIndexOfTerm(leaderPrevLogIndex)
		} else {
			reply.XIndex, reply.XTerm = rf.firstTangibleIndex()
		}
		reply.XLen = nextIndex
		return
	}
	rf.vlog.CopyEntries(leaderPrevLogIndex+1, args.Entries)
	rf.persist()
	leaderCommit := args.LeaderCommit
	if leaderCommit > rf.commitIndex {
		newVal := min(leaderCommit, args.PrevLogIndex+len(args.Entries))
		DPrintf("follower %d update commit index %d->%d", rf.me, rf.commitIndex, newVal)
		rf.commitIndex = newVal
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	DPrintf("AppendEntries role: %d, term: %d, id: %d, caller term: %d, caller id:%d, len: %d, successful",
		rf.role, rf.currentTerm, rf.me, args.Term, args.LeaderId, len(args.Entries))
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	start := time.Now().UnixMilli()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("Raft.AppendEntries, time: %d", time.Now().UnixMilli()-start)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) channelApplySnapshot(msg *ApplyMsg) {
	rf.applyCh <- *msg
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	prefix := fmt.Sprintf("InstallSnapshot role: %d, term: %d, id: %d, caller term: %d, caller id:%d, LastIncludedIndex: %d, LastIncludedTerm: %d",
		rf.role, rf.currentTerm, rf.me, args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	DPrintf("%s", prefix)

	term := args.Term
	if term > rf.currentTerm {
		rf.becomeFollower(term)
		rf.leaderId = args.LeaderId
	}
	reply.Term = rf.currentTerm
	if rf.role != RoleFollower {
		DPrintf("%s, fail not follower", prefix)
		return
	}
	if term < rf.currentTerm {
		DPrintf("%s, fail for term %d vs %d", prefix, term, rf.currentTerm)
		return
	}
	if args.LastIncludedIndex <= rf.vlog.GetLastIncludedIndex() {
		DPrintf("%s, no need to apply snapshot %d vs %d", prefix, args.LastIncludedIndex, rf.vlog.GetLastIncludedIndex())
		return
	}

	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.Data,
	}

	if args.LastIncludedIndex >= rf.commitIndex {
		DPrintf("%s, cover log and snapshot %d vs %d", prefix, args.LastIncludedIndex, rf.commitIndex)
		rf.vlog = VirtualLog{
			LastIncludedIndex: args.LastIncludedIndex,
			LastIncludedTerm:  args.LastIncludedTerm,
			Data:              make([]LogEntry, 0),
		}
		rf.snapshot = args.Data
		rf.commitIndex = args.LastIncludedIndex
	} else {
		DPrintf("%s, apply snapshot %d vs %d", prefix, args.LastIncludedIndex, rf.commitIndex)
		rf.vlog.ApplySnapshot(args.LastIncludedIndex)
		rf.snapshot = args.Data
	}
	go rf.channelApplySnapshot(&msg)
	rf.persist()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
