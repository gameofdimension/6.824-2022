package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
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
	Type   OpType
	Key    string
	Value  string
	Unique string
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
	cache       map[string]interface{}
	clientSeq   map[int64]int64 // client id -> seq
	indexReq    map[int]string
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
			unique := op.Unique
			kv.indexReq[m.CommandIndex] = unique
			id, seq := parseReqId(unique)
			if seq <= kv.clientSeq[id] {
				kv.mu.Unlock()
				continue
			}
			if op.Type == OpGet {
				if val, ok := kv.repo[op.Key]; ok {
					kv.cache[unique] = val
				} else {
					kv.cache[unique] = nil
				}
			} else if op.Type == OpPut {
				kv.repo[op.Key] = op.Value
				kv.cache[unique] = true
			} else if op.Type == OpAppend {
				kv.repo[op.Key] = kv.repo[op.Key] + op.Value
				kv.cache[unique] = true
			}
			kv.clientSeq[id] = seq
		}
		kv.mu.Unlock()

		if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
			data := kv.makeSnapshot()
			kv.rf.Snapshot(m.CommandIndex, data)
		}
	}
}

func makeReqId(id int64, seq int64) string {
	return fmt.Sprintf("%d-%d", id, seq)
}

func parseReqId(unique string) (int64, int64) {
	arr := strings.Split(unique, "-")
	id, _ := strconv.Atoi(arr[0])
	seq, _ := strconv.Atoi(arr[1])
	return int64(id), int64(seq)
}

func (kv *KVServer) pollGet(term int, index int, unique string, reply *GetReply) bool {
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
	reqId, ok := kv.indexReq[index]
	if !ok || reqId != unique {
		reply.Err = ErrWrongLeader
		return true
	}
	val := kv.cache[reqId]

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
	kv.mu.Lock()
	unique := makeReqId(args.Id, args.Seq)
	if val, ok := kv.cache[unique]; ok {
		if val != nil {
			reply.Err = OK
			reply.Value = val.(string)
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:   OpGet,
		Key:    args.Key,
		Unique: unique,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for kv.killed() == false {
		rc := kv.pollGet(term, index, unique, reply)
		if !rc {
			time.Sleep(1 * time.Millisecond)
		} else {
			return
		}
	}
}

func (kv *KVServer) pollPutAppend(term int, index int, unique string, reply *PutAppendReply) bool {
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
	reqId, ok := kv.indexReq[index]
	if !ok || reqId != unique {
		reply.Err = ErrWrongLeader
		return true
	}
	reply.Err = OK
	return true
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	unique := makeReqId(args.Id, args.Seq)
	if val, ok := kv.cache[unique]; ok {
		if val.(bool) {
			reply.Err = OK
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	opType := OpPut
	if args.Op == "Append" {
		opType = OpAppend
	}
	op := Op{
		Type:   OpType(opType),
		Key:    args.Key,
		Value:  args.Value,
		Unique: unique,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for kv.killed() == false {
		rc := kv.pollPutAppend(term, index, unique, reply)
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
	e.Encode(kv.indexReq)
	data := w.Bytes()
	return data
}

func (kv *KVServer) loadSnapshot(data []byte) {
	DPrintf("loadSnapshot %d", len(data))
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var repo map[string]string
	var clientSeq map[int64]int64
	var cache map[string]interface{}
	var indexReq map[int]string
	if d.Decode(&repo) != nil ||
		d.Decode(&clientSeq) != nil ||
		d.Decode(&cache) != nil ||
		d.Decode(&indexReq) != nil {
		panic("readPersist fail, Decode")
	} else {
		kv.repo = repo
		kv.clientSeq = clientSeq
		kv.cache = cache
		kv.indexReq = indexReq
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
	DPrintf("make raft %d", me)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastApplied = 0
	kv.repo = make(map[string]string)
	kv.cache = make(map[string]interface{})
	kv.indexReq = make(map[int]string)
	kv.clientSeq = map[int64]int64{}

	DPrintf("StartKVServer, %d", me)
	kv.loadSnapshot(kv.persister.ReadSnapshot())
	go kv.applier()
	// You may need initialization code here.

	return kv
}
