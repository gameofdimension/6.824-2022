package shardctrler

import (
	"fmt"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastApplied int
	// repo        map[string]string
	cache     map[int64]interface{} // client id -> applied result
	clientSeq map[int64]int64       // client id -> seq

	configs []Config // indexed by config num
}

type OpType int

const (
	OpQuery = 1
	OpJoin  = 2
	OpLeave = 3
	OpMove  = 4
)

type Op struct {
	// Your data here.
	Type     OpType
	Args     interface{}
	ClientId int64
	Seq      int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	rc := sc.getCachedUpdate(args.Id, args.Seq)
	if rc == -1 {
		reply.Err = ErrWrongLeader
		return
	}
	if rc == 0 {
		reply.Err = OK
		return
	}

	op := Op{
		Type:     OpJoin,
		Args:     args,
		ClientId: args.Id,
		Seq:      args.Seq,
	}
	ret := sc.update(args.Id, args.Seq, op)
	if ret != 0 {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	rc := sc.getCachedUpdate(args.Id, args.Seq)
	if rc == -1 {
		reply.Err = ErrWrongLeader
		return
	}
	if rc == 0 {
		reply.Err = OK
		return
	}

	op := Op{
		Type:     OpLeave,
		Args:     args,
		ClientId: args.Id,
		Seq:      args.Seq,
	}
	ret := sc.update(args.Id, args.Seq, op)
	if ret != 0 {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	rc := sc.getCachedUpdate(args.Id, args.Seq)
	if rc == -1 {
		reply.Err = ErrWrongLeader
		return
	}
	if rc == 0 {
		reply.Err = OK
		return
	}

	op := Op{
		Type:     OpMove,
		Args:     args,
		ClientId: args.Id,
		Seq:      args.Seq,
	}
	ret := sc.update(args.Id, args.Seq, op)
	if ret != 0 {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) getCachedUpdate(id int64, seq int64) int {
	if _, leader := sc.rf.GetState(); !leader {
		// reply.Err = ErrWrongLeader
		// return
		return -1
	}
	sc.mu.Lock()
	lastSeq, ok := sc.clientSeq[id]
	if ok {
		if seq < lastSeq {
			panic(fmt.Sprintf("PutAppend seq of %d out of order [%d vs %d]", id, seq, sc.clientSeq[id]))
		}
		if seq == lastSeq {
			if val, ok := sc.cache[id]; !ok || !val.(bool) {
				panic(fmt.Sprintf("impossible cache value %t %t", ok, val.(bool)))
			}
			// reply.Err = OK
			sc.mu.Unlock()
			// return
			return 0
		}
	}
	sc.mu.Unlock()
	return 1
}
func (sc *ShardCtrler) update(id int64, seq int64, op Op) int {
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		return -1
	}
	for {
		rc := sc.pollUpdate(term, index, id, seq)
		if rc == 1 {
			time.Sleep(1 * time.Millisecond)
		} else {
			return 0
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	if _, leader := sc.rf.GetState(); !leader {
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	lastSeq, ok := sc.clientSeq[args.Id]
	if ok {
		if args.Seq < lastSeq {
			panic(fmt.Sprintf("Get seq of %d out of order [%d vs %d]", args.Id, args.Seq, sc.clientSeq[args.Id]))
		}
		if args.Seq == lastSeq {
			if val, ok := sc.cache[args.Id]; !ok {
				panic(fmt.Sprintf("impossible cache value %t %t", ok, val.(bool)))
			} else {
				if val != nil {
					reply.Err = OK
					reply.Config = val.(Config)
				} else {
					reply.Err = ErrNoVersion
				}
			}
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	op := Op{
		Type:     OpQuery,
		Args:     args,
		ClientId: args.Id,
		Seq:      args.Seq,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		rc := sc.pollGet(term, index, args.Id, args.Seq, reply)
		if !rc {
			time.Sleep(1 * time.Millisecond)
		} else {
			return
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = 0
	sc.cache = make(map[int64]interface{})
	sc.clientSeq = map[int64]int64{}

	return sc
}
