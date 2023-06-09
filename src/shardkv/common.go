package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrWrongVersion = "ErrWrongVersion"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id  int64
	Seq int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id  int64
	Seq int64
}

type GetReply struct {
	Err   Err
	Value string
}

type DumpArgs struct {
	Id               int64
	Seq              int64
	OldVersion       int
	NewVersion       int
	Shard            int
	ShardData        map[string]string
	LastClientSeq    map[int64]int64
	LastClientResult map[int64]interface{}
}

type DumpReply struct {
	Err Err
}
