package shardkv

import (
	"fmt"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	OpNoop    = 0
	OpGet     = 1
	OpPut     = 2
	OpAppend  = 3
	OpConfig  = 4
	OpMigrate = 5
)

type OpType uint64

type ConfigChange struct {
	CurrentVersion int
	CurrentConfig  shardctrler.Config
	NextVersion    int
	NextConfig     shardctrler.Config
	Status         [shardctrler.NShards]ShardStatus
}

type ShardData struct {
	Shard int
	Data  map[string]string

	// 以下两个字段用来在数据迁移前后检测重复请求
	LastClientSeq    map[int64]int64
	LastClientResult map[int64]interface{}
}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     OpType
	ClientId int64
	Seq      int64

	Key       string
	Value     string
	Change    ConfigChange
	ShardData ShardData
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
	mck *shardctrler.Clerk
	// cm          *ConfigManager
	id          int64
	persister   *raft.Persister
	lastApplied int
	repo        map[string]string
	cache       map[int64]interface{} // client id -> applied result
	clientSeq   map[int64]int64       // client id -> seq

	currentVersion int
	currentConfig  shardctrler.Config
	nextVersion    int
	nextConfig     shardctrler.Config
	status         [shardctrler.NShards]ShardStatus
	migrateSeq     int64
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, leader := kv.rf.GetState(); !leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	prefix := fmt.Sprintf("get handler %d of group %d with args %v", kv.me, kv.gid, *args)
	shard := key2shard(args.Key)
	DPrintf("%s start with status %v", prefix, kv.status)
	status := kv.GetShardStatus(shard)
	if status != Ready {
		reply.Err = ErrWrongGroup
		DPrintf("%s shard %d status not ready %v", prefix, shard, kv.status)
		kv.mu.Unlock()
		return
	}
	DPrintf("%s try get from cache", prefix)
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
	DPrintf("%s send to raft", prefix)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("%s not leader", prefix)
		reply.Err = ErrWrongLeader
		return
	}
	for !kv.rf.Killed() {
		stop, success := kv.pollAgreement(term, index, args.Id, args.Seq)
		if !stop {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		if !success {
			reply.Err = ErrWrongLeader
		} else {
			kv.mu.Lock()
			val := kv.cache[args.Id]
			kv.mu.Unlock()
			if val == nil {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
				reply.Value = val.(string)
			}
		}
		break
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, leader := kv.rf.GetState(); !leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	prefix := fmt.Sprintf("put handler %d of group %d with args %v", kv.me, kv.gid, *args)
	shard := key2shard(args.Key)
	status := kv.GetShardStatus(shard)
	DPrintf("%s start with status %v", prefix, kv.status)
	if status != Ready {
		reply.Err = ErrWrongGroup
		DPrintf("%s shard %d status not ready %v", prefix, shard, kv.status)
		kv.mu.Unlock()
		return
	}
	DPrintf("%s try get from cache", prefix)
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
	DPrintf("%s send to raft", prefix)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("%s not leader", prefix)
		reply.Err = ErrWrongLeader
		return
	}
	for !kv.rf.Killed() {
		stop, success := kv.pollAgreement(term, index, args.Id, args.Seq)
		if !stop {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		if !success {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
		break
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("server %d,%d of group %d killed %d, %v", kv.me, kv.id, kv.gid, kv.currentVersion, kv.status)
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
	kv.clientSeq = make(map[int64]int64)

	kv.id = nrand()
	kv.loadSnapshot(kv.persister.ReadSnapshot())
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	DPrintf("server %d of group %d StartKVServer", me, gid)
	go kv.applier()
	go kv.configFetcher()

	return kv
}
