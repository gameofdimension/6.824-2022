package shardkv

import (
	"bytes"
	"fmt"
	"time"

	"6.824/labgob"
	"6.824/shardctrler"
)

func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		if m.SnapshotValid {
			kv.mu.Lock()
			DPrintf("server %d of group %d resume snapshot of term %d at index %d", kv.me, kv.gid, m.SnapshotTerm, m.SnapshotIndex)
			kv.loadSnapshot(m.Snapshot)
			kv.mu.Unlock()
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
			if !ok || seq != lastSeq {
				DPrintf("server %d of group %d will run op %v at index %d", kv.me, kv.gid, op, m.CommandIndex)
				if seq < lastSeq {
					panic(fmt.Sprintf("smaller seq %d vs %d for %d, %v", seq, lastSeq, clientId, op))
				}
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
				} else if op.Type == OpConfig {
					config := op.Config
					kv.currentVersion = config.Num
					kv.currentConfig = config
					kv.nextVersion = 0

					data := kv.makeSnapshot()
					kv.rf.Snapshot(m.CommandIndex, data)
				} else if op.Type == OpMigrate {
					kv.merge(op.Data)

					data := kv.makeSnapshot()
					kv.rf.Snapshot(m.CommandIndex, data)
				}
				kv.clientSeq[clientId] = seq
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
				data := kv.makeSnapshot()
				DPrintf("server %d of group %d take snapshot at index %d", kv.me, kv.gid, m.CommandIndex)
				kv.rf.Snapshot(m.CommandIndex, data)
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) dump(shard int) map[string]string {
	kv.mu.Lock()
	tmp := make(map[string]string, 0)
	for k, v := range kv.repo {
		if key2shard(k) == shard {
			tmp[k] = v
		}
	}
	kv.mu.Unlock()
	return tmp
}

func (kv *ShardKV) merge(data map[string]string) {
	for k, v := range data {
		kv.repo[k] = v
	}
}

func (kv *ShardKV) Migrate(args *DumpArgs, reply *DumpReply) {
	if _, leader := kv.rf.GetState(); !leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	prefix := fmt.Sprintf("migrate handler %d of group %d with %d vs %d shard %d from group %d",
		kv.me, kv.gid, args.OldVersion, args.NewVersion, args.Shard, args.CallerGid)
	kv.mu.Unlock()
	DPrintf("%s start", prefix)
	if !kv.isSwitching() {
		DPrintf("%s not switching %d", prefix, kv.nextVersion)
		reply.Err = ErrWrongVersion
		return
	}
	kv.mu.Lock()
	if args.NewVersion != kv.nextVersion {
		DPrintf("%s version not match %d vs %d", prefix, args.NewVersion, kv.nextVersion)
		reply.Err = ErrWrongVersion
		kv.mu.Unlock()
		return
	}
	DPrintf("%s will merge data %d", prefix, len(args.ShardData))
	if kv.nextConfig.Shards[args.Shard] != kv.gid {
		panic(fmt.Sprintf("%s shard %d not data for me %d", prefix, args.Shard, kv.gid))
	}
	copy := map[string]string{}
	for k := range args.ShardData {
		copy[k] = args.ShardData[k]
	}
	kv.migrateSeq += 1
	seq := kv.migrateSeq
	clientId := int64(args.CallerGid)
	op := Op{
		Type:     OpMigrate,
		ClientId: clientId,
		Seq:      seq,
		Data:     copy,
	}
	index, term, isLeader := kv.rf.Start(op)
	DPrintf("%s send migration to raft %d, %d, %t", prefix, index, term, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	for {
		stop, success := kv.pollAgreement(term, index, clientId, seq)
		if stop {
			if success {
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (kv *ShardKV) pollAgreement(term int, index int, clientId int64, seq int64) (bool, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ct, cl := kv.rf.GetState()
	if !cl || ct != term {
		return true, false
	}
	if kv.lastApplied < index {
		return false, false
	}
	if logSeq, ok := kv.clientSeq[clientId]; !ok || logSeq != seq {
		return true, false
	}
	return true, true
}

func (kv *ShardKV) pollGet(term int, index int, clientId int64, seq int64, reply *GetReply) bool {
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

func (kv *ShardKV) pollPutAppend(term int, index int, clientId int64, seq int64, reply *PutAppendReply) bool {
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

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.repo)
	e.Encode(kv.clientSeq)
	e.Encode(kv.cache)
	e.Encode(kv.currentVersion)
	e.Encode(kv.currentConfig)
	e.Encode(kv.nextVersion)
	e.Encode(kv.nextConfig)
	e.Encode(kv.status)
	e.Encode(kv.migrateSeq)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) loadSnapshot(data []byte) {
	// DPrintf("loadSnapshot %d", len(data))
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var repo map[string]string
	var clientSeq map[int64]int64
	var cache map[int64]interface{}
	var currentVersion int
	var currentConfig shardctrler.Config
	var nextVersion int
	var nextConfig shardctrler.Config
	var status [shardctrler.NShards]ShardStatus
	var migrateSeq int64
	if d.Decode(&repo) != nil ||
		d.Decode(&clientSeq) != nil ||
		d.Decode(&cache) != nil ||
		d.Decode(&currentVersion) != nil ||
		d.Decode(&currentConfig) != nil ||
		d.Decode(&nextVersion) != nil ||
		d.Decode(&nextConfig) != nil ||
		d.Decode(&status) != nil ||
		d.Decode(&migrateSeq) != nil {
		panic("readPersist fail, Decode")
	} else {
		kv.repo = repo
		kv.clientSeq = clientSeq
		kv.cache = cache
		kv.currentVersion = currentVersion
		kv.currentConfig = currentConfig
		kv.nextVersion = nextVersion
		kv.nextConfig = nextConfig
		kv.status = status
		kv.migrateSeq = migrateSeq
	}
}
