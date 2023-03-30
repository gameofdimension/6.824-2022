package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OpNoop   = 0
	OpGet    = 1
	OpPut    = 2
	OpAppend = 3
)

type OpType uint64

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied int
	repo        map[string]string
	getResult   map[int]interface{}
	// indexTerm   map[int]int
}

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		kv.mu.Lock()
		if m.SnapshotValid {
		} else if m.CommandValid {
			if m.CommandIndex <= kv.lastApplied {
				panic(fmt.Sprintf("index error %d vs %d", m.CommandIndex, kv.lastApplied))
			}
			kv.lastApplied = m.CommandIndex
			op := m.Command.(Op)
			if op.Type == OpGet {
				if val, ok := kv.repo[op.Key]; ok {
					kv.getResult[m.CommandIndex] = val
				} else {
					kv.getResult[m.CommandIndex] = nil
				}
			} else if op.Type == OpPut {
				kv.repo[op.Key] = op.Value
			} else if op.Type == OpAppend {
				kv.repo[op.Key] = kv.repo[op.Key] + op.Value
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type: OpGet,
		Key:  args.Key,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for kv.killed() == false {
		kv.mu.Lock()
		if kv.lastApplied < index {
			kv.mu.Unlock()
			time.Sleep(3 * time.Millisecond)
			continue
		}
		val := kv.getResult[index]
		kv.mu.Unlock()
		if val == nil {
			reply.Err = ErrNoKey
			return
		} else {
			reply.Err = OK
			reply.Value = val.(string)
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opType := OpPut
	if args.Op == "Append" {
		opType = OpAppend
	}
	op := Op{
		Type:  OpType(opType),
		Key:   args.Key,
		Value: args.Value,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for kv.killed() == false {
		kv.mu.Lock()
		if kv.lastApplied < index {
			kv.mu.Unlock()
			time.Sleep(3 * time.Millisecond)
			continue
		}
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastApplied = -1
	kv.repo = make(map[string]string)
	kv.getResult = make(map[int]interface{})

	go kv.applier()
	// You may need initialization code here.

	return kv
}
