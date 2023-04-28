package shardkv

import (
	"sync"

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
