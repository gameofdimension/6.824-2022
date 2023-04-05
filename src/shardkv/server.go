package shardkv

import (
	"fmt"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister   *raft.Persister
	lastApplied int
	repo        map[string]string
	cache       map[int64]interface{} // client id -> applied result
	clientSeq   map[int64]int64       // client id -> seq
}

//	func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
//		// Your code here.
//	}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
	for {
		rc := kv.pollGet(term, index, args.Id, args.Seq, reply)
		if !rc {
			time.Sleep(1 * time.Millisecond)
		} else {
			return
		}
	}
}

//	func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
//		// Your code here.
//	}
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
	for {
		rc := kv.pollPutAppend(term, index, args.Id, args.Seq, reply)
		if !rc {
			time.Sleep(1 * time.Millisecond)
		} else {
			return
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	kv.persister = persister
	kv.lastApplied = 0
	kv.repo = make(map[string]string)
	kv.cache = make(map[int64]interface{})
	kv.clientSeq = map[int64]int64{}

	DPrintf("server %d StartKVServer", me)
	kv.loadSnapshot(kv.persister.ReadSnapshot())
	go kv.applier()

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	return kv
}
