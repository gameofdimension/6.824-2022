package shardctrler

import (
	"sync"

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

	configs []Config // indexed by config num
}

type OpType int

const (
	OpGet = 1
	OpPut = 2
)

type Op struct {
	// Your data here.
	Type    OpType
	Version int
	Config  Config
}

func (sc *ShardCtrler) putConfig(version int, config Config) bool {
	return true
}

func (sc *ShardCtrler) getConfig(version int) (bool, *Config) {
	return nil
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	version := len(sc.configs)
	groups := make(map[int][]string)
	for k, v := range sc.configs[version-1].Groups {
		groups[k] = v
	}
	gids := []int{}
	for k, v := range args.Servers {
		groups[k] = v
		gids = append(gids, k)
	}

	shards := add(sc.configs[version-1].Shards[:], gids)
	config := Config{
		Num:    version,
		Groups: groups,
		Shards: SliceToArr(shards),
	}
	sc.mu.Unlock()
	sc.putConfig(version, config)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	version := len(sc.configs)
	groups := make(map[int][]string)
	for k, v := range sc.configs[version-1].Groups {
		if !SliceContains(args.GIDs, k) {
			groups[k] = v
		}
	}
	shards := remove(sc.configs[version-1].Shards[:], args.GIDs)
	config := Config{
		Num:    version,
		Groups: groups,
		Shards: SliceToArr(shards),
	}
	sc.mu.Unlock()
	sc.putConfig(version, config)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	version := len(sc.configs)
	groups := make(map[int][]string)
	for k, v := range sc.configs[version-1].Groups {
		groups[k] = v
	}
	shards := sc.configs[version-1].Shards[:]
	oldGid := shards[args.Shard]
	cc := 0
	for _, v := range shards {
		if v == oldGid {
			cc += 1
		}
	}
	if cc == 1 {
		delete(groups, oldGid)
	}

	shards[args.Shard] = args.GID
	config := Config{
		Num:    version,
		Groups: groups,
		Shards: SliceToArr(shards),
	}
	sc.mu.Unlock()
	sc.putConfig(version, config)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	version := args.Num
	if version == -1 {
		version = len(sc.configs) - 1
	}
	sc.mu.Unlock()
	reply.Config = *sc.getConfig(version)
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

	return sc
}
