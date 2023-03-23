package raft

import "time"

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
		selfLogIndex := len(rf.log) - 1
		selfLogTerm := rf.log[selfLogIndex].Term
		if canVote(rf.votedFor, candidateId, candidateLogTerm, candidateLogIndex, selfLogTerm, selfLogIndex) {
			rf.votedFor = candidateId
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			return
		}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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

func firstIndexOfTerm(log []LogEntry, from int) int {
	term := log[from].Term
	for i := from; i >= 0; i -= 1 {
		if log[i].Term != term {
			return i + 1
		}
	}
	return 0
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("AppendEntries role: %d, term: %d, id: %d, caller term: %d, caller id:%d, len: %d",
		rf.role, rf.currentTerm, rf.me, args.Term, args.LeaderId, len(args.Entries))

	term := args.Term
	if term >= rf.currentTerm {
		rf.becomeFollower(term)
		rf.leaderId = args.LeaderId
	}
	if rf.role != RoleFollower {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.lastFromLeaderAt = time.Now().UnixMilli()

	leaderPrevLogIndex := args.PrevLogIndex
	leaderPrevLogTerm := args.PrevLogTerm
	if leaderPrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = 0
		reply.XIndex = 0
		reply.XLen = len(rf.log)
		return
	}
	if leaderPrevLogTerm != rf.log[leaderPrevLogIndex].Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = rf.log[leaderPrevLogIndex].Term
		reply.XIndex = firstIndexOfTerm(rf.log, leaderPrevLogIndex)
		reply.XLen = len(rf.log)
		return
	}
	index := leaderPrevLogIndex + 1
	for i := 0; i < len(args.Entries); i += 1 {
		if index >= len(rf.log) {
			left := args.Entries[i:]
			rf.log = append(rf.log, left...)
			break
		}
		entryTerm := rf.log[index].Term
		if entryTerm != args.Entries[i].Term {
			rf.log = rf.log[:index]
			left := args.Entries[i:]
			rf.log = append(rf.log, left...)
			break
		}
		index += 1
	}
	rf.persist()
	leaderCommit := args.LeaderCommit
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = min(leaderCommit, args.PrevLogIndex+len(args.Entries))
	}
	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
