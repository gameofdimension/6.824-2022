package shardkv

import (
	"fmt"
	"time"
)

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

func (kv *ShardKV) Migrate(args *DumpArgs, reply *DumpReply) {
	if _, leader := kv.rf.GetState(); !leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	prefix := fmt.Sprintf("migrate handler %d of group %d with %d vs %d shard %d from %d seq %d, data %v",
		kv.me, kv.gid, args.OldVersion, args.NewVersion, args.Shard, args.Id, args.Seq, args.ShardData)
	DPrintf("%s start", prefix)
	if !kv.isSwitching() {
		DPrintf("%s not switching %d", prefix, kv.nextVersion)
		reply.Err = ErrWrongVersion
		kv.mu.Unlock()
		return
	}
	// 为什么允许调用者进度落后，因为有可能调用者失败
	// 而此时被调用方这边已经执行成功并顺利转到下个版本
	// 调用者因为失败了会重试，如果要严格匹配的话，就永远卡在这里了
	if args.NewVersion < kv.nextVersion {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	if args.NewVersion != kv.nextVersion {
		DPrintf("%s version not match %d vs %d", prefix, args.NewVersion, kv.nextVersion)
		reply.Err = ErrWrongVersion
		kv.mu.Unlock()
		return
	}
	if kv.status[args.Shard] == Ready {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	DPrintf("%s will merge data %d", prefix, len(args.ShardData))
	if kv.nextConfig.Shards[args.Shard] != kv.gid {
		panic(fmt.Sprintf("%s shard %d not data for me %d, %v", prefix, args.Shard, kv.gid, kv.nextConfig))
	}
	copy := map[string]string{}
	for k := range args.ShardData {
		copy[k] = args.ShardData[k]
	}
	copySeq := make(map[int64]int64)
	for k, v := range args.LastClientSeq {
		copySeq[k] = v
	}
	copyResult := make(map[int64]interface{})
	for k, v := range args.LastClientResult {
		copyResult[k] = v
	}
	clientId := args.Id
	seq := args.Seq
	kv.mu.Unlock()

	shardData := ShardData{
		Shard:            args.Shard,
		Data:             copy,
		LastClientSeq:    copySeq,
		LastClientResult: copyResult,
	}
	op := Op{
		Type:      OpMigrate,
		ClientId:  clientId,
		Seq:       seq,
		ShardData: shardData,
	}
	index, term, isLeader := kv.rf.Start(op)
	DPrintf("%s send migration to raft %d, %d, %t", prefix, index, term, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ok := kv.raftSyncOp(&op)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}
