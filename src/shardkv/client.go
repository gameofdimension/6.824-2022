package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	me  int64
	seq int64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.

	ck.me = nrand()
	DPrintf("MakeClerk %d", ck.me)
	ck.seq = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	ck.seq += 1
	args.Id = ck.me
	args.Seq = ck.seq

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		prefix := fmt.Sprintf("client %d seq %d get key %s shard %d group %d", ck.me, ck.seq, key, shard, gid)
		DPrintf("%s servers %v", prefix, ck.config.Groups[gid])
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				DPrintf("%s start call server %s", prefix, servers[si])
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					DPrintf("%s done", prefix)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					DPrintf("%s wrong group, will switch", prefix)
					break
				}
				DPrintf("%s fail %t with reply %v", prefix, ok, reply)
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	ck.seq += 1
	args.Id = ck.me
	args.Seq = ck.seq

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		prefix := fmt.Sprintf("client %d seq %d put key %s shard %d group %d", ck.me, ck.seq, key, shard, gid)
		DPrintf("%s servers %v", prefix, ck.config.Groups[gid])
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				DPrintf("%s start call server %s", prefix, servers[si])
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					DPrintf("%s done", prefix)
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					DPrintf("%s wrong group, will switch", prefix)
					break
				}
				DPrintf("%s fail %t with reply %v", prefix, ok, reply)
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
