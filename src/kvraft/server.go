package kvraft

import (
	"bytes"
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
	if !Debug {
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
	Type     OpType
	Key      string
	Value    string
	ClientId int64
	Seq      int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister   *raft.Persister
	lastApplied int
	repo        map[string]string
	cache       map[int64]interface{} // client id -> applied result
	clientSeq   map[int64]int64       // client id -> seq
}

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if m.SnapshotValid {

		} else if m.CommandValid {
			kv.mu.Lock()
			if m.CommandIndex <= kv.lastApplied {
				panic(fmt.Sprintf("index error %d vs %d", m.CommandIndex, kv.lastApplied))
			}
			kv.lastApplied = m.CommandIndex
			op := m.Command.(Op)
			clientId, seq := op.ClientId, op.Seq
			lastSeq, ok := kv.clientSeq[clientId]
			// new op from clientId
			DPrintf("get op %v", op)
			if !ok || seq > lastSeq {
				DPrintf("run op %v", op)
				if op.Type == OpGet {
					if val, ok := kv.repo[op.Key]; ok {
						kv.cache[clientId] = val
					} else {
						kv.cache[clientId] = nil
					}
				} else if op.Type == OpPut {
					kv.repo[op.Key] = op.Value
					kv.cache[clientId] = true
				} else if op.Type == OpAppend {
					kv.repo[op.Key] = kv.repo[op.Key] + op.Value
					kv.cache[clientId] = true
				}
				kv.clientSeq[clientId] = seq
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
				data := kv.makeSnapshot()
				kv.rf.Snapshot(m.CommandIndex, data)
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) pollGet(term int, index int, clientId int64, seq int64, reply *GetReply) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ct, cl := kv.rf.GetState()
	if !cl || ct != term {
		reply.Err = ErrWrongLeader
		return true
	}
	if kv.lastApplied < index {
		return false
	}
	if logSeq, ok := kv.clientSeq[clientId]; !ok || logSeq != seq {
		reply.Err = ErrWrongLeader
		return true
	}
	val := kv.cache[clientId]

	if val == nil {
		reply.Err = ErrNoKey
		return true
	}
	reply.Err = OK
	reply.Value = val.(string)
	return true
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, leader := kv.rf.GetState(); !leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	lastSeq, ok := kv.clientSeq[args.Id]
	if ok {
		if args.Seq < lastSeq {
			panic(fmt.Sprintf("Get seq of %d out of order [%d vs %d]", args.Id, args.Seq, kv.clientSeq[args.Id]))
		}
		if args.Seq == lastSeq {
			if val, ok := kv.cache[args.Id]; !ok {
				panic(fmt.Sprintf("impossible cache value %t %t", ok, val.(bool)))
			} else {
				if val != nil {
					reply.Err = OK
					reply.Value = val.(string)
				} else {
					reply.Err = ErrNoKey
				}
			}
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	op := Op{
		Type:     OpGet,
		Key:      args.Key,
		ClientId: args.Id,
		Seq:      args.Seq,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for kv.killed() == false {
		rc := kv.pollGet(term, index, args.Id, args.Seq, reply)
		if !rc {
			time.Sleep(1 * time.Millisecond)
		} else {
			return
		}
	}
}

func (kv *KVServer) pollPutAppend(term int, index int, clientId int64, seq int64, reply *PutAppendReply) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ct, cl := kv.rf.GetState()
	if !cl || ct != term {
		reply.Err = ErrWrongLeader
		return true
	}
	if kv.lastApplied < index {
		return false
	}
	if logSeq, ok := kv.clientSeq[clientId]; !ok || logSeq != seq {
		reply.Err = ErrWrongLeader
		return true
	}
	reply.Err = OK
	return true
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, leader := kv.rf.GetState(); !leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	lastSeq, ok := kv.clientSeq[args.Id]
	if ok {
		if args.Seq < lastSeq {
			panic(fmt.Sprintf("PutAppend seq of %d out of order [%d vs %d]", args.Id, args.Seq, kv.clientSeq[args.Id]))
		}
		if args.Seq == lastSeq {
			if val, ok := kv.cache[args.Id]; !ok || !val.(bool) {
				panic(fmt.Sprintf("impossible cache value %t %t", ok, val.(bool)))
			}
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	opType := OpPut
	if args.Op == "Append" {
		opType = OpAppend
	}
	op := Op{
		Type:     OpType(opType),
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.Id,
		Seq:      args.Seq,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for kv.killed() == false {
		rc := kv.pollPutAppend(term, index, args.Id, args.Seq, reply)
		if !rc {
			time.Sleep(1 * time.Millisecond)
		} else {
			return
		}
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

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.repo)
	e.Encode(kv.clientSeq)
	e.Encode(kv.cache)
	data := w.Bytes()
	return data
}

func (kv *KVServer) loadSnapshot(data []byte) {
	// DPrintf("loadSnapshot %d", len(data))
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var repo map[string]string
	var clientSeq map[int64]int64
	var cache map[int64]interface{}
	if d.Decode(&repo) != nil ||
		d.Decode(&clientSeq) != nil ||
		d.Decode(&cache) != nil {
		panic("readPersist fail, Decode")
	} else {
		kv.repo = repo
		kv.clientSeq = clientSeq
		kv.cache = cache
	}
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
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	// DPrintf("make raft %d", me)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastApplied = 0
	kv.repo = make(map[string]string)
	kv.cache = make(map[int64]interface{})
	kv.clientSeq = map[int64]int64{}

	// DPrintf("StartKVServer, %d", me)
	kv.loadSnapshot(kv.persister.ReadSnapshot())
	go kv.applier()
	// You may need initialization code here.

	return kv
}
