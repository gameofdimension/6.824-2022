package shardkv

import (
	"bytes"
	"fmt"

	"6.824/labgob"
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
					panic(fmt.Sprintf("smaller seq %d vs %d for %d", seq, lastSeq, clientId))
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
	kv.mu.Lock()
	for k, v := range data {
		kv.repo[k] = v
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) Migrate(args *DumpArgs, reply *DumpReply) {
	targetVersion := args.Version
	if targetVersion != kv.cm.currentVersion {
		reply.Err = ErrWrongVersion
		return
	}
	kv.merge(args.ShardData)
	reply.Err = OK
	kv.cm.status[args.Shard] = Ready
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
