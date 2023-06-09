package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// RoleType represents the role of a node in a cluster.
type RoleType uint64

const (
	RoleFollower  RoleType = 1
	RoleCandidate          = 2
	RoleLeader             = 3

	SendHeartBeatInterval = 150
	ElectionTimeout       = 400
	Delta                 = 200
)

// log entry struct
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	id      string
	applyCh chan ApplyMsg
	role    RoleType

	// persistent state
	currentTerm int
	votedFor    int
	vlog        VirtualLog
	snapshot    []byte

	leaderId    int
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastGrantVote    int64
	lastFromLeaderAt int64
	electionStartAt  int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == RoleLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.vlog)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var vlog VirtualLog
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&vlog) != nil {
		panic("readPersist fail, Decode")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.vlog = vlog
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	rf.vlog.ApplySnapshot(index)
	rf.snapshot = snapshot
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	for idx := range rf.nextIndex {
		if rf.nextIndex[idx] <= rf.vlog.GetLastIncludedIndex() {
			rf.nextIndex[idx] = rf.vlog.GetLastIncludedIndex() + 1
		}
	}
	rf.persist()
	rf.mu.Unlock()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.vlog.NextIndex()
	term := rf.currentTerm
	if rf.Killed() {
		return index, term, false
	}
	isLeader := rf.role == RoleLeader
	if !isLeader {
		return index, term, isLeader
	}

	DPrintf("%d add command %v at term %d onto index %d", rf.me, command, term, index)
	tmp := LogEntry{Term: term, Command: command}
	rf.vlog.AddItem(&tmp)
	rf.persist()

	// Your code here (2B).
	return index, term, isLeader
}

// guidance: http://nil.csail.mit.edu/6.824/2022/labs/guidance.html

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	DPrintf("%d was killed at term %d", rf.me, rf.currentTerm)
	defer rf.mu.Unlock()
}

func (rf *Raft) Killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) shouldStartElection() bool {
	rf.mu.Lock()
	lastFromLeaderAt := rf.lastFromLeaderAt
	lastGrantVote := rf.lastGrantVote
	electionStartAt := rf.electionStartAt
	span := ElectionTimeout
	nowMills := time.Now().UnixMilli()
	must := false
	if rf.role == RoleFollower {
		if nowMills-lastFromLeaderAt > int64(span) && nowMills-lastGrantVote > int64(span) {
			must = true
		}
	} else if rf.role == RoleCandidate {
		if nowMills-electionStartAt > int64(span) {
			must = true
		}
	}
	rf.mu.Unlock()
	return must
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	round := 0
	for rf.Killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.shouldStartElection() {
			round += 1
			rf.startElection(round)
		} else {
			time.Sleep(time.Duration(rand.Int63()%Delta) * time.Millisecond)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// rand.Seed(42)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	rf.applyCh = applyCh
	rf.role = RoleFollower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leaderId = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.snapshot = rf.persister.ReadSnapshot()
	rf.readPersist(persister.ReadRaftState())
	if rf.vlog.isPrime() {
		rf.currentTerm = 0
		rf.votedFor = -1
		DPrintf("log empty after readPersist")
		rf.vlog = VirtualLog{
			LastIncludedIndex: 0,
			LastIncludedTerm:  0,
			Data:              make([]LogEntry, 0),
		}
	}
	rf.commitIndex = rf.vlog.GetLastIncludedIndex()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = rf.vlog.NextIndex()
		rf.matchIndex[idx] = 0
	}
	rf.id = RandStr(16)
	DPrintf("start raft %d, %s with %d vs %d", rf.me, rf.id, rf.lastApplied, rf.commitIndex)

	rf.spawnWorker()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) spawnWorker() {
	go rf.applyLog()
	rf.spawnLeaderWorker()
}
